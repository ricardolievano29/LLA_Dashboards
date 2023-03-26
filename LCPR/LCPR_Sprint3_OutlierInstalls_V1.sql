--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - OUTLIER INSTALLS #####

WITH

parameters as (
SELECT date_trunc('month', date('2022-12-01')) AS input_month
)

, new_customers_pre as (
SELECT 
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
    fix_s_att_account
FROM new_customers_pre
)

, installations as (
SELECT
    *
FROM "lcpr.stage.prod"."so_ln_lcpr"
WHERE
    org_id = 'LCPR' and org_cntry = 'PR'
    and order_status = 'COMPLETE'
    and command_id = 'CONNECT'
)

, new_installations as (
SELECT
    account_id, 
    fix_b_att_maxstart, 
    date_trunc('month', date(dt)) as month, -- Why dt instead of fix_b_att_maxstart or other dates?
    cast(cast(order_start_date as timestamp) as date) as order_start_date, 
    cast(cast(completed_date as timestamp) as date) as completed_date, 
    case when date_diff('day', cast(cast(order_start_date as timestamp) as date), cast(cast(completed_date as timestamp) as date)) > 6 then account_id else null end as outlier_install
FROM installations a
INNER JOIN new_customers b
    ON cast(a.account_id as varchar) = cast(b.fix_s_att_account as varchar)
)

--- This considers that only sales made in the month and orders completed in the month are part of this month's KPI.
 
SELECT
    month as opd_s_dim_month, 
    count(distinct outlier_install) as opd_s_mes_uni_long_install, 
    count(distinct account_id) as opd_s_mes_mea_sales
FROM new_installations
GROUP BY 1
ORDER BY 1
    


--- ### KPI Calculation

--- This considers that all orders completed in the month and all sales made in the month are part of this month's KPI. This may not match with visualisation numbers dpeending on what logic is being taken.

-- SELECT 
--     count(distinct outlier_install) as outlier_installs, 
--     count(distinct account_id) as new_sales, 
--     round(cast(count(distinct outlier_install) as double)/cast(count(distinct account_id) as double), 2) as KPI
-- FROM new_installations
