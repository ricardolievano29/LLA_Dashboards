--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - FULL FLAGS TABLE #####

--- ### ### ### Initial steps (Common in most of the calculations)

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

--- --- --- DNA base
, relevant_dna as (
SELECT
    *
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE 
    play_type != '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(connect_dte_sbb)) between ((SELECT input_month FROM parameters) - interval '3' month) and ((SELECT input_month FROM parameters))
)

, new_customers3m_now_pre as (
SELECT
    cast(cast(first_value(connect_dte_sbb) over (partition by sub_acct_no_sbb order by DATE(dt) DESC) as timestamp) as date) as fix_b_att_maxstart,   
    SUB_ACCT_NO_SBB as fix_s_att_account, 
    delinquency_days
FROM relevant_dna
ORDER BY 1
)
    
, new_customers3m_now as (   
SELECT 
    date_trunc('month', fix_b_att_maxstart) as install_month, 
    fix_b_att_maxstart,
    fix_s_att_account, 
    fix_s_att_account as new_sales_flag,
    delinquency_days
FROM new_customers3m_now_pre
)

-- , new_customers as (
-- SELECT 
--     *,
--     fix_s_att_account as new_sales_flag
-- FROM new_customers3m_now
-- WHERE date_trunc('month', date(fix_b_att_maxstart)) = (SELECT input_month FROM parameters)
-- )

--- --- --- Interactions
, clean_interaction_time as (
SELECT *
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    (cast(interaction_start_time as varchar) != ' ') 
    and (interaction_start_time is not null)
    and date_trunc('month', date(interaction_start_time)) between ((SELECT input_month FROM parameters) - interval '3' month) and ((SELECT input_month FROM parameters))
    and account_type = 'RES'
)

, interactions_fields as (
SELECT
    *,
    cast(substr(cast(interaction_start_time as varchar), 1, 10) as date) as interaction_date, 
    date_trunc('month', cast(substr(cast(interaction_start_time as varchar), 1, 10) as date)) as month
FROM clean_interaction_time
)

, interactions_not_repeated as (
SELECT
    first_value(interaction_id) OVER(partition by account_id, interaction_date, interaction_channel, interaction_agent_id, interaction_purpose_descrip order by interaction_date desc) AS interaction_id2
FROM interactions_fields
)

, interactions_fields2 as (
SELECT *
FROM interactions_not_repeated a
LEFT JOIN interactions_fields b
    ON a.interaction_id2 = b.interaction_id
)

--- --- --- External file: Truckrolls
, truckrolls as (
SELECT 
    create_dte_ojb, 
    job_no_ojb, 
    sub_acct_no_sbb
FROM "lcpr.stage.dev"."truckrolls"
)

--- ### ### ### Straight to Soft Dx

--- Skipped for now. It is in no use for Panama.

--- ### ### ### Never Paid

--- The 85-day moving window is not completed yet.

--- ### ### ### Early Tickets

--- Num: Customers with tickets on a time span of 7 weeks between sales and ticket (associated to certain sales month)
--- Denom: Month new sales

--- As it goes backwards 7 weeks, the cohort is m-2.

--- Therefore, we need to get the new sales of 2 months ago and check if those clients had an interaction in their
---first 7 weeks, which requires us to use the interactions of 2 months and 1 month ago.

--- Getting those interactions between 2 months a 1 month ago:
, relevant_interactions as (
SELECT
    distinct account_id, 
    min(interaction_date) as first_interaction_date, 
    date_trunc('month', date(min(interaction_date))) as first_interaction_start_month 
FROM interactions_fields2 a
LEFT JOIN truckrolls b
    ON a.interaction_date = cast(create_dte_ojb as date) and cast(a.account_id as varchar) = cast(b.sub_acct_no_sbb as varchar)
WHERE 
    interaction_purpose_descrip not in ('Work Order Status', 'Default Call Wrapup', 'G:outbound Calls', 'Eq: Cust. First', 'Eq: Audit', 'Eq: Code Error', 'Downgrade Service', 'Disconnect Service', 'Rt: Dowgrde Service', 'Cust Service Calls')
    and (lower(interaction_purpose_descrip) like '%ppv%problem%'
    or lower(interaction_purpose_descrip) like '%hsd%problem%'
    or lower(interaction_purpose_descrip) like '%cable%problem%'
    or lower(interaction_purpose_descrip) like '%tv%problem%'
    or lower(interaction_purpose_descrip) like '%video%problem%'
    or lower(interaction_purpose_descrip) like '%tel%problem%'
    or lower(interaction_purpose_descrip) like '%phone%problem%'
    or lower(interaction_purpose_descrip) like '%int%problem%'
    or lower(interaction_purpose_descrip) like '%line%problem%'
    or lower(interaction_purpose_descrip) like '%hsd%issue%'
    or lower(interaction_purpose_descrip) like '%ppv%issue%'
    or lower(interaction_purpose_descrip) like '%video%issue%'
    or lower(interaction_purpose_descrip) like '%tel%issue%'
    or lower(interaction_purpose_descrip) like '%phone%issue%'
    or lower(interaction_purpose_descrip) like '%int%issue%'
    or lower(interaction_purpose_descrip) like '%line%issue%'
    or lower(interaction_purpose_descrip) like '%cable%issue%'
    or lower(interaction_purpose_descrip) like '%tv%issue%'
    or lower(interaction_purpose_descrip) like '%bloq%'
    or lower(interaction_purpose_descrip) like '%slow%'
    or lower(interaction_purpose_descrip) like '%slow%service%'
    or lower(interaction_purpose_descrip) like '%service%tech%'
    or lower(interaction_purpose_descrip) like '%tech%service%'
    or lower(interaction_purpose_descrip) like '%no%service%'
    or lower(interaction_purpose_descrip) like '%hsd%no%'
    or lower(interaction_purpose_descrip) like '%hsd%slow%'
    or lower(interaction_purpose_descrip) like '%hsd%intermit%'
    or lower(interaction_purpose_descrip) like '%no%brows%'
    or lower(interaction_purpose_descrip) like '%phone%cant%'
    or lower(interaction_purpose_descrip) like '%phone%no%'
    or lower(interaction_purpose_descrip) like '%no%connect%'
    or lower(interaction_purpose_descrip) like '%no%conect%'
    or lower(interaction_purpose_descrip) like '%no%start%'
    or lower(interaction_purpose_descrip) like '%equip%'
    or lower(interaction_purpose_descrip) like '%intermit%'
    or lower(interaction_purpose_descrip) like '%no%dat%'
    or lower(interaction_purpose_descrip) like '%dat%serv%'
    or lower(interaction_purpose_descrip) like '%int%data%'
    or lower(interaction_purpose_descrip) like '%tech%'
    or lower(interaction_purpose_descrip) like '%supp%'
    or lower(interaction_purpose_descrip) like '%outage%'
    or lower(interaction_purpose_descrip) like '%mass%'
    or lower(interaction_purpose_descrip) like '%discon%warn%'
    ) and (
    lower(interaction_purpose_descrip) not like '%work%order%status%'
    and lower(interaction_purpose_descrip) not like '%default%call%wrapup%'
    and lower(interaction_purpose_descrip) not like '%bound%call%'
    and lower(interaction_purpose_descrip) not like '%cust%first%'
    and lower(interaction_purpose_descrip) not like '%audit%'
    and lower(interaction_purpose_descrip) not like '%eq%code%'
    and lower(interaction_purpose_descrip) not like '%downg%'
    and lower(interaction_purpose_descrip) not like '%upg%'
    and lower(interaction_purpose_descrip) not like '%vol%discon%'
    and lower(interaction_purpose_descrip) not like '%discon%serv%'
    and lower(interaction_purpose_descrip) not like '%serv%call%'
        )
    and date_trunc('month', date(interaction_start_time)) between (select input_month from parameters) - interval '2' month and (select input_month from parameters) - interval '1' month
GROUP BY 1
)

--- Associating those interactions to the new customers of two months ago.
, early_tickets as (
SELECT 
    fix_s_att_account, 
    new_sales_flag as new_sales2m_flag,
    install_month, 
    first_interaction_start_month, 
    first_interaction_date,
    fix_b_att_maxstart,
    case when date_diff('week',date(fix_b_att_maxstart), date(first_interaction_date)) <= 7 then account_id else null end as early_ticket_flag
FROM new_customers3m_now A 
LEFT JOIN relevant_interactions B 
    ON cast(A.fix_s_att_account as varchar) = cast(B.account_id as varchar) 
WHERE 
    date_trunc('month', date(fix_b_att_maxstart)) = (SELECT input_month FROM parameters) - interval '2' month
)


--- ### ### ### Outlier Install Times

--- Num: New installations with a duration over 6 days
--- Denom: Month new sales

--- This KPI doesn't use an specific cohort; it is calculated for the current month.

, installations as (
SELECT
    distinct account_id, 
    max(order_start_date) as order_start_date, 
    max(completed_date) as completed_date
FROM "lcpr.stage.prod"."so_ln_lcpr"
WHERE
    org_id = 'LCPR' and org_cntry = 'PR'
    and order_status = 'COMPLETE'
    and command_id = 'CONNECT'
GROUP BY 1
)

, outlier_installs as (
SELECT
    distinct fix_s_att_account,
    fix_b_att_maxstart,
    new_sales_flag,
    install_month,
    date(order_start_date) as order_start_date, 
    date(completed_date) as completed_date, 
    case when date_diff('day', date(order_start_date), date(completed_date)) > 6 then account_id else null end as outlier_install_flag
FROM new_customers3m_now a
LEFT JOIN installations b
    ON cast(a.fix_s_att_account as varchar) = cast(b.account_id as varchar)
WHERE
    date_trunc('month', date(fix_b_att_maxstart)) = (SELECT input_month FROM parameters)
)

--- ### ### ### MRC Changes

--- Num: Customers with >5% MRC changes between 2 months
--- Denom:  Active customers without plan changes

--- As this KPI requires a comparison between two months, it uses data from the previous month.

, bom_previous_month as (
SELECT 
    fmc_s_dim_month as fmc_s_dim_month_prev, 
    fix_s_att_account, 
    fmc_s_att_account, 
    fix_b_mes_mrc as fix_b_mes_mrc_prev
FROM fmc_table_adj
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters) - interval '1' month  
    and fix_b_mes_overdue < 85
)
 
 , eom_current_month as (
SELECT
    fmc_s_dim_month, 
    fix_s_att_account, 
    fix_e_mes_overdue,
    fix_e_mes_mrc
FROM fmc_table_adj
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters) 
    and fix_e_mes_overdue < 85
)

 , mrc_increases as (
SELECT  
    c.fix_s_att_account,
    fmc_s_dim_month, 
    case when fix_e_mes_mrc/fix_b_mes_mrc_prev > 1.05 then c.fix_s_att_account else null end as mrc_increase_flag,
    case when fix_e_mes_mrc/fix_b_mes_mrc_prev <= 1.05 then c.fix_s_att_account else null end as no_plan_change  
FROM bom_previous_month p 
LEFT JOIN eom_current_month c 
    ON p.fix_s_att_account = c.fix_s_att_account
)

---  ### ### ### Billing Claims

--- Num: Number of users with bill claims
--- Denom: Active customers

--- This KPI requires an association between users and interactions identified as billing claims. Then, we'll find those interactions and generate a flag to the customer that made it. As a time range is not specified it is assumed that we are looking for biling claims interactions just in the current month.

, billing_claims as (
SELECT 
    fix_s_att_account,
    fmc_s_dim_month,
    account_id as billing_claim_flag, 
    interaction_date
FROM fmc_table_adj A
LEFT JOIN interactions_fields2 B
    ON cast(A.fix_s_att_account as varchar) = cast(B.account_id as varchar) and date(A.fmc_s_dim_month) = date(B.month)
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    and month = (SELECT input_month FROM parameters)
    and account_type = 'RES'
    and (interaction_status = 'Closed')
    and (interaction_purpose_descrip in ('Adjustment Request', 'Approved Adjustment', 'Billing', 'Cancelled Np', 'Chuito Retained', 'Vd: Billing', 'Vd: Cant Afford', 'Vd: Closed Buss', 'Vd: Deceased', 'E-Bill', 'G:customer Billable', 'Not Retained', 'Np: Cancelled Np', 'Np: Payment Plan', 'Np: Promise To Pay', 'Promise To Pay', 'Ret- Adjustment', 'Ret- Promise-To-Pay', 'Ret-Bill Expln', 'Ret-Direct Debit', 'Ret-Pay Meth Expln', 'Ret-Payment', 'Ret-Right Pricing', 'Rt: Price Increase', 'Rt: Rate Pricing')
    or (lower(interaction_purpose_descrip) like '%ci:%' and interaction_purpose_descrip not in  ('Ci: Cable Card Req', 'Ci: Inst/Tc Status', 'Ci: Install Stat', 'Ci: Installer / Tech'))
    or (lower(interaction_purpose_descrip) like '%payment%')
    or (lower(interaction_purpose_descrip) like '%vd%Ccn%'))
)

--- ### ### ### Joining all the flags

, flag3_early_tickets as (
SELECT
    F.*, 
    early_ticket_flag, 
    new_sales2m_flag
FROM fmc_table_adj F
LEFT JOIN early_tickets I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar) and date(F.fmc_s_dim_month) = date(I.install_month + interval '2' month)
WHERE
    F.fmc_s_dim_month = (SELECT input_month FROM parameters)
    and F.fix_e_att_active = 1 
)

, flag4_outlier_installs as (
SELECT
    F.*, 
    new_sales_flag, 
    outlier_install_flag
FROM flag3_early_tickets F
LEFT JOIN outlier_installs I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar) and date(F.fmc_s_dim_month) = date(I.install_month)
)

, flag5_mrc_changes as (
SELECT
    F.*, 
    no_plan_change, 
    mrc_increase_flag
FROM flag4_outlier_installs F
LEFT JOIN mrc_increases I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar) and date(F.fmc_s_dim_month) = date(I.fmc_s_dim_month) 
)

, flag6_billing_claims as (
SELECT 
    F.*, 
    billing_claim_flag
FROM flag5_mrc_changes F
LEFT JOIN billing_claims I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar) and date(F.fmc_s_dim_month) = date(I.fmc_s_dim_month)
)

--- ### ### ### Final table
, final_table as (
SELECT 
    fmc_s_dim_month as opd_s_dim_month,
    fmc_e_fla_tech as odr_e_fla_final_tech, -- E_Final_Tech_Flag, 
    fmc_e_fla_fmcsegment as odr_e_fla_fmc_segment, -- E_FMC_Segment, 
    fmc_e_fla_fmc as odr_e_fla_fmc_type, -- E_FMCType, 
    case 
        when fmc_e_fla_tenure = 'Early Tenure' then 'Early-Tenure'
        when fmc_e_fla_tenure = 'Mid Tenure' then 'Mid-Tenure'
        when fmc_e_fla_tenure = 'Late Tenure' then 'Late-Tenure'
    end as odr_e_fla_final_tenure, ---E_FinalTenureSegment,
    count(distinct fix_s_att_account) as odr_s_mes_active_base, -- as activebase, 
    -- sales_channel, 
    count(distinct new_sales_flag) as opd_s_mes_sales, -- sum(monthsale_flag) as Sales, 
    count(distinct new_sales2m_flag) as opd_s_mes_sales2m,
    -- sum(SoftDx_Flag) as Soft_Dx, 
    -- sum (NeverPaid_Flag) as NeverPaid,
    count(distinct outlier_install_flag) as opd_s_mes_long_installs, 
    -- sum (increase_flag) as MRC_Increases, 
    -- sum (no_plan_change_flag) as NoPlan_Changes, 
    -- sum(mountingbill_flag) as MountingBills, 
    -- sum(earlyticket_flag) as EarlyTickets,
    -- Sales_Month, 
    -- Install_Month, 
    -- Ticket_Month, 
    -- count(distinct F_SalesFlag) Unique_Sales, 
    -- count(distinct soft_dx_flag) as opd_s_mes_uni_softdx,
    -- count(distinct day_85) as opd_s_mes_uni_never_paid,
    -- count(distinct F_LongInstallFlag) Unique_LongInstall,
    count(distinct mrc_increase_flag) as opd_s_mes_uni_mrcincrease,
    count(distinct no_plan_change) as opd_s_mes_uni_noplan_changes,
    -- count(distinct mounting_bill_flag) as opd_s_mes_uni_moun_gbills, 
    count(distinct early_ticket_flag) as opd_s_mes_uni_early_tickets,
    count(distinct billing_claim_flag) as opd_s_mes_uni_bill_claim
FROM flag6_billing_claims
WHERE 
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', 'Fixed Base Exception', 'Churn Exception') 
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
)



-- SELECT
--     count(distinct outlier_install_flag),
--     count(distinct new_sales_flag)
-- FROM outlier_installs
-- WHERE cast(fix_s_att_account as varchar) = '8211790230284246'

SELECT sum(opd_s_mes_uni_bill_claim), sum(odr_s_mes_active_base) FROM final_table
