--- ##### LCPR SPRINT 5  OPERATIONAL DRIVERS - NODES TICKET DENSITY #####

--- ### Initial steps

WITH

parameters as (
SELECT date_trunc('month', date('2023-01-01')) as input_month --- Input month you wish the code run for
)

, fmc_table as ( --- Remember that we are using fixed table while fmc is ready
SELECT 
    fix_s_att_account, 
    fix_s_dim_month
FROM "db_stage_dev"."lcpr_fixed_table_jan_mar06"
WHERE
    fix_s_dim_month = (SELECT input_month FROM parameters)
)

, last_node as (
SELECT
    A.sub_acct_no_sbb, 
    B.bridger_addr_hse as node_code, 
    date_trunc('month', date(dt)) as month
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" A 
LEFT JOIN (
    SELECT
        sub_acct_no_sbb, 
        bridger_addr_hse,
        min(dt) as first_dt
    FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
    GROUP BY 1, 2
    ) B
    ON A.sub_acct_no_sbb = B.sub_acct_no_sbb
WHERE A.dt = B.first_dt and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
)

, join_node as (
SELECT 
    month,
    fix_s_att_account,
    node_code
FROM fmc_table A
INNER JOIN last_node B
    ON cast(A.fix_s_att_account as varchar) = cast(B.sub_acct_no_sbb as varchar) and A.fix_s_dim_month = B.month
)

SELECT 
    count(distinct sub_acct_no_sbb), 
    count(bridger_addr_hse)
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE 
    date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
    and date(dt) = (date_trunc('month', date(dt)) + interval '1' month - interval '1' day)
--- I have identified the gap between the Fixed table and the records in the DNA. There must a way to get the same number of records in DNA than in Fixed table as the latter was made up from the former. 
