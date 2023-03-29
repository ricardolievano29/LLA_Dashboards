--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - STRAIGHT TO SOFT DX #####

WITH

--- --- --- Month you wish the code run for
parameters as (
SELECT date_trunc('month', date('2023-02-01')) AS input_month
)

--- --- --- FMC table
, fmc_table as (
SELECT
    *
FROM "db_stage_dev"."lcpr_fmc_table_dec_mar23" --- Make sure to set the month accordindly to the input month of parameters
UNION ALL (SELECT * FROM "db_stage_dev"."lcpr_fmc_table_jan_mar23")
UNION ALL (SELECT * FROM "db_stage_dev"."lcpr_fmc_table_feb_mar23")
-- WHERE 
    -- fmc_s_dim_month = (SELECT input_month FROM parameters)
)

, repeated_accounts as (
SELECT 
    fmc_s_dim_month, 
    fix_s_att_account, --- Operational Drivers are focused just in Fixed dynamics, not Mobile. I don't take the FMC account because sometimes in concatenates Fixed and Mobile accounts ids, which alters the results when joining with other bases using the account id.
    count(*) as records_per_user
FROM fmc_table
WHERE 
    fix_s_att_account is not null --- Making sure that we are focusing just in Fixed.
GROUP BY 1, 2
ORDER BY 3 desc
)

, fmc_table_adj as (
SELECT 
    F.*,
    records_per_user
FROM fmc_table F
LEFT JOIN repeated_accounts R
    ON F.fix_s_att_account = R.fix_s_att_account and F.fmc_s_dim_month = R.fmc_s_dim_month
)

--- ### ### ### STRAIGHT TO SOFT DX

, soft_dx as (
SELECT
    date_trunc('month', date(connect_dte_sbb)) as install_month, 
    date_trunc('month', date(dt)) as overdue_month,
    sub_acct_no_sbb as fix_s_att_account,
    sub_acct_no_sbb as new_sales2m_flag,
    case when delinquency_days = 50 then sub_acct_no_sbb else null end as soft_dx_flag
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE 
    play_type != '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
    and date_trunc('month', date(connect_dte_sbb)) = (SELECT input_month FROM parameters) - interval '2' month
)

-- , soft_dx_new_cust as (
-- SELECT
--     A.fix_s_att_account, 
--     A.install_month, 
--     new_sales2m_flag,
--     overdue_month, 
--     soft_dx_flag
-- FROM new_customers2m A
-- LEFT JOIN soft_dx B
--     ON cast(A.fix_s_att_account as varchar) = cast(B.fix_s_att_account as varchar)
-- )

, flag_soft_dx as (
SELECT 
    F.*, 
    install_month, 
    new_sales2m_flag,
    overdue_month, 
    soft_dx_flag
FROM fmc_table_adj F
LEFT JOIN soft_dx I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar) and date(fmc_s_dim_month) = date(I.overdue_month)
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    and fix_e_att_active = 1 
)

SELECT 
    fmc_s_dim_month as opd_s_dim_month,
    fmc_e_fla_tech as odr_e_fla_final_tech, -- E_Final_Tech_Flag, 
    fmc_e_fla_fmcsegment as odr_e_fla_fmc_segment, -- E_FMC_Segment, 
    fmc_e_fla_fmc as odr_e_fla_fmc_type, -- E_FMCType, 
    case 
        when fmc_e_fla_tenure = 'Early Tenure' then 'Early-Tenure'
        when fmc_e_fla_tenure = 'Mid Tenure' then 'Mid-Tenure'
        when fmc_e_fla_tenure = 'Late Tenure' then 'Late-Tenure'
    end as odr_e_fla_final_tenure, ---E_FinalTenureSegment,
    count(distinct fix_s_att_account) as odr_s_mes_active_base, -- as activebase, 
    -- sales_channel, 
    count(distinct new_sales2m_flag) as opd_s_mes_sales, -- sum(monthsale_flag) as Sales, 
    -- sum(SoftDx_Flag) as Soft_Dx, 
    -- sum (NeverPaid_Flag) as NeverPaid,
    -- count(distinct outlier_install_flag) as opd_s_mes_long_installs, 
    -- sum (increase_flag) as MRC_Increases, 
    -- sum (no_plan_change_flag) as NoPlan_Changes, 
    -- sum(mountingbill_flag) as MountingBills, 
    -- sum(earlyticket_flag) as EarlyTickets,
    -- Sales_Month, 
    -- Install_Month, 
    -- Ticket_Month, 
    -- count(distinct F_SalesFlag) Unique_Sales, 
    count(distinct soft_dx_flag) as opd_s_mes_uni_softdx
    -- count(distinct day_85) as opd_s_mes_uni_never_paid,
    -- count(distinct F_LongInstallFlag) Unique_LongInstall,
    -- count(distinct mrc_increase_flag) as opd_s_mes_uni_mrcincrease,
    -- count(distinct no_plan_change) as opd_s_mes_uni_noplan_changes,
    -- count(distinct mounting_bill_flag) as opd_s_mes_uni_moun_gbills, 
    -- count(distinct early_ticket_flag) as opd_s_mes_uni_early_tickets
    -- count(distinct billing_claim_flag) as opd_s_mes_uni_bill_claim
FROM flag_soft_dx
WHERE 
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', 'Fixed Base Exception', 'Churn Exception') 
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
-- GROUP BY 1, 2, 3, 4, 5, 7, 16, 17, 18
-- ORDER BY 1, 2, 3, 4, 5, 7, 16, 17, 18

-- SELECT count(distinct new_sales2m_flag) FROM new_customers2m
-- SELECT count(distinct soft_dx_flag), count(distinct new_sales2m_flag) FROM flag_soft_dx
