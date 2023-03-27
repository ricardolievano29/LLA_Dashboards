--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - MOUNTING BILLS #####

WITH 

parameters as (
SELECT date_trunc('month', date('2023-02-01')) as input_month
)

, usefulfields as (
SELECT  
    date_trunc('month', date(dt)) as fmc_s_dim_month,
    delinquency_days, 
    sub_acct_no_sbb as fix_s_att_account, 
    count(distinct case when delinquency_days = 60 then sub_acct_no_sbb else null end) as mounting_bill_flag
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    and date(dt) between (SELECT input_month FROM parameters) and (SELECT input_month FROM parameters) + interval '1' month - interval '1' day
GROUP BY 1, 2, 3
)

-- SELECT distinct mounting_bill_flag, count(distinct fix_s_att_account) FROM usefulfields GROUP BY 1

, mounting_bills as (
 SELECT 
    a.fmc_s_dim_month,
    a.fix_s_att_account,
    sum(mounting_bill_flag) as mounting_bill_flag
FROM "db_stage_dev"."lcpr_fmc_table_feb_mar23" a 
LEFT JOIN usefulfields b 
    ON a.fmc_s_dim_month = b.fmc_s_dim_month and a.fix_s_att_account = b.fix_s_att_account 
WHERE 
    fix_e_att_active = 1
GROUP BY 1, 2
 )
 
SELECT 
    sum(mounting_bill_flag) as mounting_bills, 
    count(distinct fix_s_att_account) as active_base, 
    cast(sum(mounting_bill_flag) as double)/cast(count(distinct fix_s_att_account) as double) as mounting_bills_kpi
FROM mounting_bills


