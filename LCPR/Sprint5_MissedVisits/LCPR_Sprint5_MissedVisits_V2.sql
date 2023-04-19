--- ##### LCPR SPRINT 5  OPERATIONAL DRIVERS - OUTLIER REPAIR TIMES #####

--- ### Initial steps
WITH
  
 parameters as (
 SELECT date_trunc('month', date('2023-03-01')) as input_month --- Input month you wish the code run for
 )
 
, fmc_table as (
SELECT
    *
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
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
    comp_dte, 
    resched_dte_ojb,
    dispatch_memo_ojb, 
    job_compl_code_desc
FROM "lcpr.stage.dev"."truckrolls" 
)

--- ### Missed Visits

, relevant_truckrolls as (
SELECT
    date_trunc('month', date(create_dte_ojb)) as job_month,
    sub_acct_no_sbb,
    job_no_ojb,
    create_dte_ojb, 
    comp_dte, 
    resched_dte_ojb,
    job_stat_ojb
    -- case when (job_stat_ojb = 'R' or resched_dte_ojb not like '00%-%-%') then sub_acct_no_sbb else null end as missed_visit
FROM truckrolls
WHERE 
    job_typ_ojb != 'MN'
    and date_trunc('month', date(create_dte_ojb)) between ((SELECT input_month FROM parameters) - interval '2' month) and (SELECT input_month FROM parameters)
    and comp_dte like '%-%-%'
    and job_stat_ojb in ('C', 'R')
    -- and resched_dte_ojb not like '00%-%-%'
)

, last_truckroll as (
SELECT
    sub_acct_no_sbb as last_account, 
    first_value(create_dte_ojb) over (partition by sub_acct_no_sbb, date_trunc('month', date(create_dte_ojb)) order by create_dte_ojb desc) as last_job_date
FROM relevant_truckrolls
)

, join_last_truckroll as (
SELECT
    date_trunc('month', date(create_dte_ojb)) as job_month,
    sub_acct_no_sbb,
    job_no_ojb,
    create_dte_ojb, 
    last_job_date, 
    case when (job_stat_ojb = 'R' or resched_dte_ojb not like '00%-%-%') then sub_acct_no_sbb else null end as missed_visit,
    date_add('DAY', -60, date(last_job_date)) as window_day
FROM relevant_truckrolls W
INNER JOIN last_truckroll L
    ON W.sub_acct_no_sbb = L.last_account
)

, missed_visits as (
SELECT
    distinct sub_acct_no_sbb as account_id, 
    job_month,
    create_dte_ojb,
    last_job_date,
    max(missed_visit) as missed_visit_flag
FROM join_last_truckroll
WHERE
    date(create_dte_ojb) between window_day and date(last_job_date)
GROUP BY 1, 2, 3, 4
)

-- --- ### Outlier Repairs flag

, missed_visits_flag as (
SELECT 
    F.*, 
    missed_visit_flag
FROM fmc_table_adj F
LEFT JOIN missed_visits I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) 
        -- and (F.fmc_s_dim_month = I.job_month or F.fmc_s_dim_month = (I.job_month - interval '2' month))
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
    -- count(distinct outlier_repair_flag) as odr_s_mes_outlier_repairs, 
    count(distinct missed_visit_flag) as odr_s_mes_missed_visits
FROM missed_visits_flag
WHERE 
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', 'Fixed Base Exception', 'Churn Exception') 
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
)

SELECT * FROM final_table

-- --- ### Specific numbers

-- SELECT
    -- sum(odr_s_mes_missed_visits),
    -- sum(odr_s_mes_active_base)
-- FROM final_table
