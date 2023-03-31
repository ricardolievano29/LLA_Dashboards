--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - OUTLIER INSTALLS #####

WITH

parameters as (
SELECT date_trunc('month', date('2023-01-01')) AS input_month
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


--- --- --- New customers
, new_customers_pre as (
SELECT 
    cast(cast(first_value(connect_dte_sbb) over (partition by sub_acct_no_sbb order by date(dt) desc) as timestamp) as date) as fix_b_att_maxstart,
    sub_acct_no_sbb as fix_s_att_account 
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE 
    play_type != '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month',date(connect_dte_sbb)) = (SELECT input_month FROM parameters) 
ORDER BY 1
)

, new_customers as (
SELECT
    date_trunc('month', fix_b_att_maxstart) as install_month, 
    fix_b_att_maxstart, 
    fix_s_att_account, 
    fix_s_att_account as new_sales_flag
FROM new_customers_pre
)

--- ### ### ### Outlier Installs

, installations as (
SELECT
    *
FROM "lcpr.stage.prod"."so_ln_lcpr"
WHERE
    org_id = 'LCPR' and org_cntry = 'PR'
    and order_status = 'COMPLETE'
    and command_id = 'CONNECT'
)

, new_installations as (
SELECT
    fix_s_att_account, 
    fix_b_att_maxstart, 
    new_sales_flag,
    install_month,
    cast(cast(order_start_date as timestamp) as date) as order_start_date, 
    cast(cast(completed_date as timestamp) as date) as completed_date, 
    case when date_diff('day', date(order_start_date), date(completed_date)) > 6 then fix_s_att_account else null end as outlier_install_flag
FROM new_customers a
LEFT JOIN installations b
    ON cast(a.fix_s_att_account as varchar) = cast(b.account_id as varchar)
)

, flag_outlier_installs as (
SELECT
    F.*, 
    new_sales_flag,
    install_month,
    order_start_date, 
    completed_date, 
    outlier_install_flag
FROM fmc_table_adj F
LEFT JOIN new_installations I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar)
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
    count(distinct new_sales_flag) as opd_s_mes_sales, -- sum(monthsale_flag) as Sales, 
    -- sum(SoftDx_Flag) as Soft_Dx, 
    -- sum (NeverPaid_Flag) as NeverPaid,
    count(distinct outlier_install_flag) as opd_s_mes_long_installs
    -- sum (increase_flag) as MRC_Increases, 
    -- sum (no_plan_change_flag) as NoPlan_Changes, 
    -- sum(mountingbill_flag) as MountingBills, 
    -- count(distinct early_ticket_flag) as EarlyTickets
    -- Sales_Month, 
    -- Install_Month, 
    -- Ticket_Month, 
    -- count(distinct F_SalesFlag) Unique_Sales, 
    -- count(distinct soft_dx_flag) as opd_s_mes_uni_softdx
    -- count(distinct day_85) as opd_s_mes_uni_never_paid,
    -- count(distinct F_LongInstallFlag) Unique_LongInstall,
    -- count(distinct mrc_increase_flag) as opd_s_mes_uni_mrcincrease,
    -- count(distinct no_plan_change) as opd_s_mes_uni_noplan_changes,
    -- count(distinct mounting_bill_flag) as opd_s_mes_uni_moun_gbills, 
    -- count(distinct early_ticket_flag) as opd_s_mes_uni_early_tickets
    -- count(distinct billing_claim_flag) as opd_s_mes_uni_bill_claim
FROM flag_outlier_installs
WHERE 
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', 'Fixed Base Exception', 'Churn Exception') 
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
