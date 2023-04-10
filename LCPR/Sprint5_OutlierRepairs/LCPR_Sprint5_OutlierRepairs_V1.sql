--- ##### LCPR SPRINT 5  OPERATIONAL DRIVERS - OUTLIER REPAIR TIMES #####
--- ### Initial steps
WITH
  
 parameters as (
 SELECT date_trunc('month', date('2023-02-01')) as input_month --- Input month you wish the code run for
 )
 
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

, truckrolls as (
SELECT 
    sub_acct_no_sbb,
    job_no_ojb,
    job_typ_ojb,
    job_stat_ojb,
    create_dte_ojb, 
    comp_dte
FROM "lcpr.stage.dev"."truckrolls" 
)

--- ### Outlier Repair Times

, relevant_truckrolls as (
SELECT
    date_trunc('month', date(create_dte_ojb)) as job_month,
    sub_acct_no_sbb,
    job_no_ojb,
    create_dte_ojb, 
    comp_dte, 
    case when date_diff('day', date(create_dte_ojb), date(comp_dte)) > 4 then sub_acct_no_sbb else null end as outlier_repair
FROM truckrolls
WHERE 
    job_typ_ojb != 'MN'
    and date(create_dte_ojb) = (SELECT input_month FROM parameters)
    and comp_dte not in ('NO CONTACT CUST', 'NULL', 'D47', '80 CALLE DIAMANTE')
    and comp_dte is not null
    and job_stat_ojb = 'C'
)

, outlier_repairs as (
SELECT
    distinct sub_acct_no_sbb as account_id, 
    job_month,
    max(outlier_repair) as outlier_repair_flag
FROM relevant_truckrolls
GROUP BY 1, 2
)


--- ### Outlier Repairs flag

, outlier_repairs_flag as (
SELECT 
    F.*, 
    outlier_repair_flag
FROM fmc_table_adj F
LEFT JOIN outlier_repairs I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fmc_s_dim_month = I.job_month
WHERE date(fmc_s_dim_month) = (SELECT input_month FROM parameters)
)

, final_table as (
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
    -- count(distinct new_sales_flag) as opd_s_mes_sales, -- sum(monthsale_flag) as Sales, 
    -- sum(SoftDx_Flag) as Soft_Dx, 
    -- sum (NeverPaid_Flag) as NeverPaid,
    -- count(distinct outlier_install_flag) as opd_s_mes_long_installs
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
    count(distinct outlier_repair_flag) as odr_s_mes_outlier_repairs
FROM outlier_repairs_flag
WHERE 
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', 'Fixed Base Exception', 'Churn Exception') 
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
)

SELECT * FROM final_table

--- ### Specific numbers

-- SELECT
--     sum(odr_s_mes_outlier_repairs),
--     sum(odr_s_mes_active_base)
-- FROM final_table
