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
    new_sales_flag,
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

SELECT
    count(distinct early_ticket_flag), 
    count(distinct new_sales_flag)
FROM early_tickets
-- ORDER BY 1 asc
-- LIMIT 100
