--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - FULL FLAGS TABLE #####

WITH

parameters as (SELECT date('2022-12-01') as input_month)

, new_customers_pre as (
SELECT 
    *,
    cast(cast(first_value(connect_dte_sbb) over (partition by sub_acct_no_sbb order by date(dt) desc) as timestamp) as date) as fix_b_att_maxstart,   
    sub_acct_no_sbb as fix_s_att_account 
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" --- Making my own calculation for new sales
WHERE 
    play_type != '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month',date(connect_dte_sbb)) = (SELECT input_month FROM parameters) 
ORDER BY 1
)

-- , new_customers as (
-- SELECT
--     fix_s_dim_month as install_month,
--     fix_b_att_maxstart, 
--     fix_s_att_account
-- FROM "db_stage_dev"."lcpr_fixed_table_jan_mar17"
-- WHERE
--     fix_s_fla_mainmovement = '4.New Customer' --- Getting the new sales directly from the FMC (or Fixed) table.
-- )

, new_customers as (
SELECT
    date_trunc('month', fix_b_att_maxstart) as install_month, 
    fix_b_att_maxstart, 
    fix_s_att_account,
    fix_s_att_account as new_sales_flag
FROM new_customers_pre
)

, new_customers_3_m as (
SELECT 
    SUB_ACCT_NO_SBB as fix_s_att_account, 
    case when delinquency_days >= 85 then SUB_ACCT_NO_SBB else null end as day_85s
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    -- and delinquency_days >= 85 
    and date_trunc('month',date(dt)) = (select input_month from parameters) + interval '3' month 
    -- and date_trunc('month', date(connect_dte_sbb)) = (SELECT input_month FROM parameters)
ORDER BY 1
)

, never_paid as (
SELECT
    a.fix_s_att_account,
    day_85s
FROM new_customers a
LEFT JOIN new_customers_3_m b
    ON cast(a.fix_s_att_account as varchar) = cast(b.fix_s_att_account as varchar)
-- GROUP BY 1, 2
-- ORDER BY 1, 2
)

SELECT count(distinct day_85s), count(distinct fix_s_att_account) FROM never_paid
