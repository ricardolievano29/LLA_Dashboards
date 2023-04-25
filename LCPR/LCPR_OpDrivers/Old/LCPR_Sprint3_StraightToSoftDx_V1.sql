--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - STRAIGHT TO SOFT DX #####

WITH

--- --- --- Month you wish the code run for
parameters as (
SELECT date_trunc('month', date('2022-12-01')) AS input_month
)

--- --- --- FMC table
, fmc_table as (
SELECT
    *
FROM "db_stage_dev"."lcpr_fmc_table_dec_mar23" --- Make sure to set the month accordindly to the input month of parameters
UNION ALL (SELECT * FROM "db_stage_dev"."lcpr_fmc_table_jan_mar23")
UNION ALL (SELECT * FROM "db_stage_dev"."lcpr_fmc_table_feb_mar23")
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
    and fix_e_att_active = 1 --- The denominator of most of the Sprint 5 is the active base.
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

--- --- --- New customers base
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

--- ### ### ### STRAIGHT TO SOFT DX

, soft_dx as (
SELECT
    date_trunc('month', date(connect_dte_sbb)) as install_month, 
    sub_acct_no_sbb as fix_s_att_account, 
    case when delinquency_days = 50 then sub_acct_no_sbb else null end as soft_dx_flag
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" --- Making my own calculation for new sales
WHERE 
    play_type != '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(connect_dte_sbb)) = (SELECT input_month FROM parameters) 
)

, flag_soft_dx as (
SELECT 
    F.*, 
    soft_dx_flag
FROM fmc_table_adj F
LEFT JOIN soft_dx I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar) and date(fmc_s_dim_month) = date(I.install_month)
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters)
)

SELECT count(distinct soft_dx_flag), count(distinct fix_s_att_account) FROM flag_soft_dx
