--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - NEVER PAID #####

WITH 
parameters as (
SELECT date_trunc('month', date('2022-12-01')) as input_month
)
---New customer directly from the DNA
, new_customers_pre as (
SELECT 
    (cast(cast(first_value(connect_dte_sbb) over (partition by sub_acct_no_sbb order by date(dt) desc) as timestamp) as date)) as fix_b_att_maxstart,   
    SUB_ACCT_NO_SBB as fix_s_att_account 
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    and delinquency_days = 0 
HAVING 
    date_trunc('month',date(CONNECT_DTE_SBB)) = (select input_month from parameters) 
ORDER BY 1
)
    
, new_customer as (
SELECT 
    date_trunc('month', fix_b_att_maxstart) as install_month, 
    fix_b_att_maxstart,  
    fix_s_att_account 
FROM new_customers_pre 
)
   
, new_customers_3_m as (
SELECT 
    delinquency_days,  
    SUB_ACCT_NO_SBB as day_85 
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    and delinquency_days >= 85 
    and date_trunc('month',date(dt)) = (select input_month + interval '3' month from parameters) 
ORDER BY 1
)
    
, new_customers_2_m as (
SELECT 
    delinquency_days,  
    SUB_ACCT_NO_SBB as day_60 
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    and delinquency_days >= 60 
    and date_trunc('month',date(dt)) = (select input_month + interval '2' month from parameters) 
ORDER BY 1
)
    
, new_customers_1_m as (
SELECT 
    delinquency_days,  
    SUB_ACCT_NO_SBB as day_30 
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    and delinquency_days >= 30 
    and date_trunc('month',date(dt)) = (select input_month + interval '1' month from parameters) 
ORDER BY 1
) 
   
SELECT
    install_month, 
    count(distinct day_30) as day_30s,  
    count(distinct day_60) as day_60s, 
    count(distinct day_85) as day_85s, 
    count(distinct fix_s_att_account) as new_customer 
FROM new_customer 
LEFT JOIN new_customers_1_m 
    ON day_30 = fix_s_att_account  
LEFT JOIN new_customers_2_m 
    ON day_60 = fix_s_att_account 
LEFT JOIN new_customers_3_m 
    ON day_85 = fix_s_att_account  
GROUP BY 1 
ORDER BY 1
