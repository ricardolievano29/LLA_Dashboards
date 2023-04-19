--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 3 - OPERATIONAL DRIVERS - BILLING CLAIMS ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
WITH

parameters as (select date('2023-03-01') as input_month)

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
--- --- --- --- --- --- --- --- --- --- --- BILLING CLAIMS --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

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
SELECT 
    *, 
    customer_id as bill_claim_flag
FROM pre_bill_claim
WHERE 
    interaction_purpose_descrip not in ('Ci: Cable Card Req', 'Ci: Inst/Tc Status', 'Ci: Install Stat', 'Ci: Installer / Tech')
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Final flag --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, flag_outlier_installs as (
SELECT
    F.*, 
    bill_claim_flag
FROM fmc_table_adj F
LEFT JOIN bill_claim I
    ON cast(F.fix_s_att_account as varchar) = cast(I.customer_id as varchar) and F.fmc_s_dim_month = I.interaction_start_month
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    and fix_e_att_active = 1 
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
    end as odr_e_fla_final_tenure, ---E_FinalTenureSegment,
    count(distinct fix_s_att_account) as odr_s_mes_active_base, 
    count(distinct bill_claim_flag) as opd_s_mes_uni_bill_claim
FROM flag_outlier_installs
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
--     sum(opd_s_mes_uni_bill_claim) as bill_claims, 
--     sum(odr_s_mes_active_base) as active_base, 
--     cast(sum(opd_s_mes_uni_bill_claim) as double)/cast(sum(odr_s_mes_active_base) as double) as KPI 
-- FROM final_table
