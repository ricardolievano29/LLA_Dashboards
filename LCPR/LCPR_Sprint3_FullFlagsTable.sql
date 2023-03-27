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
FROM "db_stage_dev"."lcpr_fmc_table_jan_mar23" --- Make sure to set the month accordindly to the input month of parameters
UNION ALL (SELECT * FROM "db_stage_dev"."lcpr_fmc_table_feb_mar23") --- Take all the FMC tables available for the calculations that require more than 1 month.
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

--- --- --- Interactions
, clean_interaction_time as (
SELECT *
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    (cast(interaction_start_time as varchar) != ' ') 
    and (interaction_start_time is not null)
    and date_trunc('month', cast(substr(cast(interaction_start_time as varchar),1,10) as date)) between ((SELECT input_month FROM parameters)) and ((SELECT input_month FROM parameters) + interval '1' month - interval '1' day)
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



--- ### ### ### Never Paid



--- ### ### ### Early Tickets

, relevant_interactions as (
SELECT
    customer_id, 
    interaction_date, 
    date_trunc('month', date(interaction_date)) as interaction_start_month 
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
    and date_trunc('month',date(interaction_start_time)) between (select input_month from parameters) and (select input_month + interval '2' month from parameters)
)

, early_tickets as (
SELECT 
    fix_s_att_account, 
    new_sales_flag,
    install_month, 
    interaction_start_month, 
    fix_b_att_maxstart,
    case when date_diff('week',cast(fix_b_att_maxstart AS DATE), cast(interaction_date AS DATE)) <= 7 then fix_s_att_account else null end as early_ticket_flag
FROM new_customers A 
LEFT JOIN relevant_interactions B 
    ON A.fix_s_att_account = cast(B.customer_id as bigint) 
)
    

--- ### ### ### Outlier Install Times

, installations as (
SELECT
    *
FROM "lcpr.stage.prod"."so_ln_lcpr"
WHERE
    org_id = 'LCPR' and org_cntry = 'PR'
    and order_status = 'COMPLETE'
    and command_id = 'CONNECT'
)

, outlier_installs as (
SELECT
    account_id, 
    fix_b_att_maxstart, 
    date_trunc('month', date(dt)) as month, -- Why dt instead of fix_b_att_maxstart or other dates?
    cast(cast(order_start_date as timestamp) as date) as order_start_date, 
    cast(cast(completed_date as timestamp) as date) as completed_date, 
    case when date_diff('day', cast(cast(order_start_date as timestamp) as date), cast(cast(completed_date as timestamp) as date)) > 6 then account_id else null end as outlier_install_flag
FROM installations a
INNER JOIN new_customers b
    ON cast(a.account_id as varchar) = cast(b.fix_s_att_account as varchar)
)

--- ### ### ### MRC Changes

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

--- ### ### ### Billing Claims

, billing_claims as (
SELECT 
    fix_s_att_account,
    fmc_s_dim_month,
    account_id as billing_claim_flag, 
    interaction_date
FROM fmc_table_adj
LEFT JOIN interactions_fields2
    ON cast(fix_s_att_account as varchar) = cast(account_id as varchar) and fmc_s_dim_month = month
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    and account_type = 'RES'
    and (interaction_status = 'Closed')
    and (interaction_purpose_descrip in ('Adjustment Request', 'Approved Adjustment', 'Billing', 'Cancelled Np', 'Chuito Retained', 'Vd: Billing', 'Vd: Cant Afford', 'Vd: Closed Buss', 'Vd: Deceased', 'E-Bill', 'G:customer Billable', 'Not Retained', 'Np: Cancelled Np', 'Np: Payment Plan', 'Np: Promise To Pay', 'Promise To Pay', 'Ret- Adjustment', 'Ret- Promise-To-Pay', 'Ret-Bill Expln', 'Ret-Direct Debit', 'Ret-Pay Meth Expln', 'Ret-Payment', 'Ret-Right Pricing', 'Rt: Price Increase', 'Rt: Rate Pricing')
    or (lower(interaction_purpose_descrip) like '%ci:%' and interaction_purpose_descrip not in  ('Ci: Cable Card Req', 'Ci: Inst/Tc Status', 'Ci: Install Stat', 'Ci: Installer / Tech'))
    or (lower(interaction_purpose_descrip) like '%payment%')
    or (lower(interaction_purpose_descrip) like '%vd%Ccn%'))
)


--- ### ### ### Mounting Bills

, usefulfields_dna as (
SELECT  
    date_trunc('month', date(dt)) as fmc_s_dim_month,
    delinquency_days, 
    sub_acct_no_sbb as fix_s_att_account, 
    sum(case when delinquency_days = 60 then 1 else 0 end) as mounting_bill_flag
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    and date(dt) between (SELECT input_month FROM parameters) and (SELECT input_month FROM parameters) + interval '1' month - interval '1' day
GROUP BY 1, 2, 3
)

, mounting_bills as (
 SELECT 
    a.fmc_s_dim_month,
    a.fix_s_att_account,
    sum(mounting_bill_flag) as mounting_bill_flag
FROM fmc_table_adj a 
LEFT JOIN usefulfields_dna b 
    ON a.fmc_s_dim_month = b.fmc_s_dim_month and a.fix_s_att_account = b.fix_s_att_account 
WHERE 
    a.fmc_s_dim_month = (SELECT input_month FROM parameters)
GROUP BY 1, 2
 )

--- ### ### ### Joining all flags

, flag3_early_tickets as (
SELECT 
    F.*, 
    early_ticket_flag, 
    new_sales_flag
FROM fmc_table_adj F
LEFT JOIN early_tickets I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar) and F.fmc_s_dim_month = I.install_month
WHERE
    F.fmc_s_dim_month = (SELECT input_month FROM parameters)
    and fix_e_att_active = 1 
)

, flag4_outlier_installs as (
SELECT 
    F.*, 
    outlier_install_flag
FROM flag3_early_tickets F
LEFT JOIN outlier_installs I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fmc_s_dim_month = I.month
)

, flag5_mrc_increases as (
SELECT 
    F.*, 
    mrc_increase_flag,
    no_plan_change
FROM flag4_outlier_installs F
LEFT JOIN mrc_increases I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar) and F.fmc_s_dim_month = I.fmc_s_dim_month
)

, flag6_billing_claim as (
SELECT 
    F.*, 
    billing_claim_flag
FROM flag5_mrc_increases F
LEFT JOIN billing_claims I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar) and F.fmc_s_dim_month = I.fmc_s_dim_month
)

, flag7_mounting_bills as (
SELECT 
    F.*, 
    mounting_bill_flag
FROM flag6_billing_claim F
LEFT JOIN mounting_bills I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar) and F.fmc_s_dim_month = I.fmc_s_dim_month
)

--- ### ### ### Final table

--- --- --- Jamaica's structure
, sprint3_full_table_LikeJam as (
SELECT 
    fmc_s_dim_month as opd_s_dim_month,
    fmc_e_fla_tech as odr_e_fla_final_tech, -- E_Final_Tech_Flag, 
    fmc_e_fla_fmcsegment as odr_e_fla_fmc_segment, -- E_FMC_Segment, 
    fmc_e_fla_fmc as odr_e_fla_fmc_type, -- E_FMCType, 
    fmc_e_fla_tenure as odr_e_fla_final_tenure, ---E_FinalTenureSegment,
    count(distinct fix_s_att_account) as odr_s_mes_active_base, -- as activebase, 
    count(distinct new_sales_flag) as opd_s_mes_sales,-- sales_channel, 
    -- sum(monthsale_flag) as Sales, 
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
    -- count(distinct F_SoftDxFlag) Unique_SoftDx,
    -- count(distinct F_NeverPaidFlag) Unique_NeverPaid,
    -- count(distinct F_LongInstallFlag) Unique_LongInstall,
    count(distinct mrc_increase_flag) as opd_s_mes_uni_mrcincrease,
    count(distinct no_plan_change) as opd_s_mes_uni_noplan_changes,
    sum(mounting_bill_flag) as opd_s_mes_uni_moun_gbills, 
    count(distinct early_ticket_flag) as opd_s_mes_uni_early_tickets, 
    count(distinct billing_claim_flag) as opd_s_mes_uni_bill_claim
FROM flag7_mounting_bills
WHERE 
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', 'Fixed Base Exception', 'Churn Exception') 
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
-- GROUP BY 1, 2, 3, 4, 5, 7, 16, 17, 18
-- ORDER BY 1, 2, 3, 4, 5, 7, 16, 17, 18
)

--- --- --- Panamás structure


--- --- ---
SELECT * FROM sprint3_full_table_LikeJam

--- ### ### ### Specific numbers

--- Straight to Soft Dx


--- Never paid


--- Early Tickets
-- SELECT 
--     odr_s_dim_month, 
--     sum(opd_s_mes_uni_early_tickets) as early_tickets, 
--     sum(opd_s_mes_sales) as new_customers_base, 
--     (cast(sum(opd_s_mes_uni_early_tickets) as double)/cast(sum(opd_s_mes_sales) as double)) as early_tickets_kpi
-- FROM sprint3_full_table_LikeJam 
-- GROUP BY 1 
-- ORDER BY 1

--- Outlier Install Times
-- SELECT
--     opd_s_dim_month, 
--     sum(opd_s_mes_long_installs) as outlier_installs, 
--     sum(opd_s_mes_sales) as new_customers_base, 
--     (cast(sum(opd_s_mes_long_installs) as double)/cast(sum(opd_s_mes_sales) as double)) as outlier_installs_kpi
-- FROM sprint3_full_table_LikeJam 
-- GROUP BY 1
-- ORDER BY 1

--- MRC Changes
-- SELECT 
--     opd_s_dim_month, 
--     sum(opd_s_mes_uni_mrcincrease) as mrc_increase, 
--     sum(opd_s_mes_uni_noplan_changes) as no_plan_changes, 
--     (cast(sum(opd_s_mes_uni_mrcincrease) as double)/cast(sum(opd_s_mes_uni_noplan_changes) as double)) as mrc_increases_kpi
-- FROM sprint3_full_table_LikeJam  
-- GROUP BY 1 
-- ORDER BY 1

--- Billing Claims
-- SELECT
--     opd_s_dim_month, 
--     sum(opd_s_mes_uni_bill_claim) as billing_claims, 
--     sum(odr_s_mes_active_base) as active_base, 
--     cast(sum(opd_s_mes_uni_bill_claim) as double)/cast(sum(odr_s_mes_active_base) as double) as billing_claims_kpi
-- FROM sprint3_full_table_LikeJam  
-- GROUP BY 1 
-- ORDER BY 1
    
--- Mounting Bills
-- SELECT 
--     opd_s_dim_month,
--     sum(opd_s_mes_uni_moun_gbills) as mounting_bills, 
--     sum(odr_s_mes_active_base) as active_base, 
--     cast(sum(opd_s_mes_uni_moun_gbills) as double)/cast(sum(odr_s_mes_active_base) as double) as mounting_bills_kpi
-- FROM sprint3_full_table_LikeJam  
-- GROUP BY 1 
-- ORDER BY 1
