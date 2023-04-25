--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 5 - OPERATIONAL DRIVERS - OUTLIER REPAIRS ##### --- --- --- ---
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
    comp_dte
FROM "lcpr.stage.dev"."truckrolls" 
)


--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Outlier Repairs --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

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
    and date_trunc('month', date(create_dte_ojb)) = (SELECT input_month FROM parameters)
    and comp_dte like '%-%-%'
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

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Final flags --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

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
    fmc_e_fla_tech as odr_e_fla_final_tech,
    fmc_e_fla_fmcsegment as odr_e_fla_fmc_segment,
    fmc_e_fla_fmc as odr_e_fla_fmc_type,
    case 
        when fmc_e_fla_tenure = 'Early Tenure' then 'Early-Tenure'
        when fmc_e_fla_tenure = 'Mid Tenure' then 'Mid-Tenure'
        when fmc_e_fla_tenure = 'Late Tenure' then 'Late-Tenure'
    end as odr_e_fla_final_tenure,
    count(distinct fix_s_att_account) as odr_s_mes_active_base,
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

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Tests --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

-- SELECT
--     sum(odr_s_mes_outlier_repairs) as Outlier_Repairs,
--     sum(odr_s_mes_active_base) as Active_Base, 
--     cast(sum(odr_s_mes_outlier_repairs) as double)/cast(sum(odr_s_mes_active_base) as double) as KPI
-- FROM final_table
