--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - STRAIGHT TO SOFT DX #####

WITH

--- --- --- Month you wish the code run for
parameters as (
SELECT date_trunc('month', date('2023-02-01')) AS input_month
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

--- ### ### ### STRAIGHT TO SOFT DX

, soft_dx as (
SELECT
    date_trunc('month', date(connect_dte_sbb)) as install_month, 
    date_trunc('month', date(dt)) as overdue_month,
    sub_acct_no_sbb as fix_s_att_account,
    sub_acct_no_sbb as new_sales2m_flag,
    case when delinquency_days = 50 then sub_acct_no_sbb else null end as soft_dx_flag
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE 
    play_type != '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
    and date_trunc('month', date(connect_dte_sbb)) = (SELECT input_month FROM parameters) - interval '2' month
)

-- , soft_dx_new_cust as (
-- SELECT
--     A.fix_s_att_account, 
--     A.install_month, 
--     new_sales2m_flag,
--     overdue_month, 
--     soft_dx_flag
-- FROM new_customers2m A
-- LEFT JOIN soft_dx B
--     ON cast(A.fix_s_att_account as varchar) = cast(B.fix_s_att_account as varchar)
-- )

, flag_soft_dx as (
SELECT 
    F.*, 
    install_month, 
    new_sales2m_flag,
    overdue_month, 
    soft_dx_flag
FROM fmc_table_adj F
LEFT JOIN soft_dx I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar) and date(fmc_s_dim_month) = date(I.overdue_month)
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    and fix_e_att_active = 1 
)

-- SELECT count(distinct new_sales2m_flag) FROM new_customers2m
SELECT count(distinct soft_dx_flag), count(distinct new_sales2m_flag) FROM flag_soft_dx
