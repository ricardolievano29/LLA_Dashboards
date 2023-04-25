--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 5 - OPERATIONAL DRIVERS - MISSED VISITS ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
WITH
  
 parameters as (
 SELECT date_trunc('month', date('2023-03-01')) as input_month --- Input month you wish the code run for
 )
 
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- FMC Table --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, fmc_table as (
SELECT
    *
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters)
)

, repeated_accounts as (
SELECT 
    fmc_s_dim_month, 
    fix_s_att_account,
    count(*) as records_per_user
FROM fmc_table
WHERE 
    fix_s_att_account is not null
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

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Truckrolls --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

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

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Missed Visits --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, relevant_truckrolls as (
SELECT
    date_trunc('month', date(create_dte_ojb)) as job_month,
    sub_acct_no_sbb,
    job_no_ojb,
    create_dte_ojb, 
    comp_dte, 
    resched_dte_ojb,
    job_stat_ojb
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
    -- date_trunc('month', date(create_dte_ojb)) as job_month,
    date_trunc('month', date(last_job_date)) as last_job_month,
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
    -- job_month,
    create_dte_ojb,
    last_job_date,
    last_job_month,
    max(missed_visit) as missed_visit_flag
FROM join_last_truckroll
WHERE
    date(create_dte_ojb) between window_day and date(last_job_date)
GROUP BY 1, 2, 3, 4
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Final Flags --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, missed_visits_flag as (
SELECT 
    F.*, 
    missed_visit_flag
FROM fmc_table_adj F
LEFT JOIN missed_visits I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) 
        and (F.fmc_s_dim_month = I.last_job_month)
WHERE date(fmc_s_dim_month) = (SELECT input_month FROM parameters)
)

, final_table as (
SELECT 
    fmc_s_dim_month as opd_s_dim_month,
    fmc_e_fla_tech as odr_e_fla_final_tech,
    fmc_e_fla_fmcsegment as odr_e_fla_fmc_segment,
    fmc_e_fla_fmc as odr_e_fla_fmc_type,
    case 
        when fmc_e_fla_tenure = 'Early Tenure' then 'Early-Tenure'
        when fmc_e_fla_tenure = 'Mid Tenure' then 'Mid-Tenure'
        when fmc_e_fla_tenure = 'Late Tenure' then 'Late-Tenure'
    end as odr_e_fla_final_tenure,
    count(distinct fix_s_att_account) as odr_s_mes_active_base,
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

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Tests --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

-- SELECT
--     sum(odr_s_mes_missed_visits) as missed_visits,
--     sum(odr_s_mes_active_base) as active_base, 
--     cast(sum(odr_s_mes_missed_visits) as double)/cast(sum(odr_s_mes_active_base) as double) as KPI
-- FROM final_table
