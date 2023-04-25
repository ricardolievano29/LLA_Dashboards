--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 3 - OPERATIONAL DRIVERS - MOUNTING BILLS ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
WITH 

parameters as (
SELECT date_trunc('month', date('2023-03-01')) as input_month
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
--- --- --- --- --- --- --- --- --- --- --- Mounting Bills --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, mounting_bills as (
SELECT  
    date_trunc('month', date(dt)) as fmc_s_dim_month,
    delinquency_days, 
    sub_acct_no_sbb as fix_s_att_account, 
    case when delinquency_days = 60 then sub_acct_no_sbb else null end as mounting_bill_flag
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
GROUP BY 1, 2, 3
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Final flags --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, flag_mounting_bills as (
 SELECT 
    F.*,
    mounting_bill_flag
FROM fmc_table_adj F 
LEFT JOIN mounting_bills I 
    ON F.fmc_s_dim_month = I.fmc_s_dim_month and F.fix_s_att_account = I.fix_s_att_account 
WHERE 
    fix_e_att_active = 1
 )
 
, final_table as (
SELECT 
    fmc_s_dim_month as opd_s_dim_month,
    fmc_e_fla_tech as odr_e_fla_final_tech,
    fmc_e_fla_fmcsegment as odr_e_fla_fmc_segment,
    fmc_e_fla_fmc as odr_e_fla_fmc_type,
    case --- Making sure the tenures fit with the dashboard ones
        when fmc_e_fla_tenure = 'Early Tenure' then 'Early-Tenure'
        when fmc_e_fla_tenure = 'Mid Tenure' then 'Mid-Tenure'
        when fmc_e_fla_tenure = 'Late Tenure' then 'Late-Tenure'
    end as odr_e_fla_final_tenure,
    count(distinct fix_s_att_account) as odr_s_mes_active_base, 
    count(distinct mounting_bill_flag) as opd_s_mes_uni_moun_gbills
FROM flag_mounting_bills
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
--     sum(opd_s_mes_uni_moun_gbills) as mounting_bills, 
--     sum(odr_s_mes_active_base) as active_base, 
--     cast(sum(opd_s_mes_uni_moun_gbills) as double)/cast(sum(odr_s_mes_active_base) as double) as KPI
-- FROM final_table
