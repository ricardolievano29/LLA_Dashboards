--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - BILLING CLAIMS #####

with 

parameters as (select date('2022-12-01') as input_month)

, fmc_table as (
SELECT 
    date_trunc('month', date(dt)) as fix_s_dim_month,  
    SUB_ACCT_NO_SBB as fix_s_att_account 
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
)

, pre_bill_claim as (
SELECT 
    customer_id, 
    interaction_start_time, 
    date_trunc('month', date(interaction_start_time)) as interaction_start_month, 
    interaction_purpose_descrip
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE 
    interaction_purpose_descrip in ('Adjustment Request', 'Approved Adjustment', 'Billing', 'Cancelled Np', 'Chuito Retained', 'Vd: Billing', 'Vd: Cant Afford', 'Vd: Closed Buss', 'Vd: Deceased', 'E-Bill', 'G:customer Billable', 'Not Retained', 'Np: Cancelled Np', 'Np: Payment Plan', 'Np: Promise To Pay', 'Promise To Pay', 'Ret- Adjustment', 'Ret- Promise-To-Pay', 'Ret-Bill Expln', 'Ret-Direct Debit', 'Ret-Pay Meth Expln', 'Ret-Payment', 'Ret-Right Pricing', 'Rt: Price Increase', 'Rt: Rate Pricing')
    or (interaction_purpose_descrip like '%Ci:%')
    or (interaction_purpose_descrip like '%Payment%')
    or interaction_purpose_descrip like '%Vd: Can%'
    and (account_type = 'RES') 
    and (interaction_status = 'Closed')
)

, bill_claim as (
SELECT * 
FROM pre_bill_claim
WHERE 
    interaction_purpose_descrip not in  ('Ci: Cable Card Req', 'Ci: Inst/Tc Status', 'Ci: Install Stat', 'Ci: Installer / Tech')
)

SELECT 
    fix_s_dim_month as opd_s_dim_month, 
    count(distinct customer_id) as opd_s_mes_uni_bill_claim, 
    count(distinct fix_s_att_account) as opd_s_mes_active_base
FROM fmc_table 
LEFT JOIN bill_claim 
    ON cast(fix_s_att_account as varchar) = customer_id and fix_s_dim_month = interaction_start_month 
GROUP BY 1
ORDER BY 1
