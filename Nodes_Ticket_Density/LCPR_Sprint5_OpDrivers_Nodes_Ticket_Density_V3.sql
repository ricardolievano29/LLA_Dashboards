--- ##### LCPR SPRINT 5  OPERATIONAL DRIVERS - NODES TICKET DENSITY #####

WITH

parameters as (SELECT date_trunc('month', date('2023-01-01')) as input_month)

, customers_dna_pre as (
SELECT 
    sub_acct_no_sbb, 
    date(dt) as dt, 
    bridger_addr_hse
    -- first_value(nr_bb_mac) over(partition by act_acct_cd order by dt) as last_nr_bb_mac, 
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE 
    date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
    and cust_typ_sbb = 'RES'
)

, customers_dna as (
SELECT 
    sub_acct_no_sbb, 
    bridger_addr_hse,
    min(dt) as first_dt, 
    max(dt) as last_dt
FROM customers_dna_pre
GROUP BY sub_acct_no_sbb, bridger_addr_hse
)

-- , interactions_fields as (

-- )

SELECT * FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" LIMIT 100
