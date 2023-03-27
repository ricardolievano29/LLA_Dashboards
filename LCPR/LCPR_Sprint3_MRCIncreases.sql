--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - MRC Changes #####

WITH 

parameters AS (
SELECT date_trunc('month', date('2023-02-01')) as input_month
)

, bom_previous_month as (
SELECT 
    fmc_s_dim_month as fmc_s_dim_month_prev, 
    fix_s_att_account, 
    fmc_s_att_account, 
    fix_b_mes_mrc as fix_b_mes_mrc_prev
FROM "db_stage_dev"."lcpr_fmc_table_jan_mar23" 
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters) - interval '1' month  
    and fix_b_mes_overdue < 85
 )

, eom_current_month as (
SELECT
    fmc_s_dim_month, 
    fix_s_att_account, 
    fix_e_mes_overdue,
    fix_e_mes_mrc
FROM "db_stage_dev"."lcpr_fmc_table_feb_mar23"
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters) 
    and fix_e_mes_overdue < 85
)

 , mrcflag as (
SELECT  
    c.fix_s_att_account,
    fmc_s_dim_month, 
    case when fix_e_mes_mrc/fix_b_mes_mrc_prev > 1.05 then c.fix_s_att_account else null end as MRC_increase_flag,
    case when fix_e_mes_mrc/fix_b_mes_mrc_prev <= 1.05 then c.fix_s_att_account else null end as no_plan_change  
FROM bom_previous_month p 
LEFT JOIN eom_current_month c 
    ON p.fix_s_att_account = c.fix_s_att_account
)
 
SELECT 
    fmc_s_dim_month, 
    count(distinct MRC_Increase_flag) as opd_s_mes_uni_mrcincrease, 
    count(distinct no_plan_change) as opd_s_mes_uni_mrcnoincrease, 
    count(distinct fix_s_att_account) as activebase
FROM mrcflag 
GROUP BY 1 
ORDER BY 1
 
 
