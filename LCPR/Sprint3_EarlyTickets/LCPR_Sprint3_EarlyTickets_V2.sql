--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - EARLY TICKETS #####

WITH

parameters as (
SELECT date_trunc('month', date('2023-02-01')) as input_month
)
--- ### New customer directly from the DNA

, new_customers_pre as (
SELECT
    cast(cast(first_value(connect_dte_sbb) over (partition by sub_acct_no_sbb order by DATE(dt) DESC) as timestamp) as date) as fix_b_att_maxstart,   
    SUB_ACCT_NO_SBB as fix_s_att_account 
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type != '0P'
    and cust_typ_sbb = 'RES' 
    and delinquency_days = 0 
HAVING 
    date_trunc('month', date(CONNECT_DTE_SBB)) = (SELECT input_month FROM parameters) 
ORDER BY 1
)
    
, new_customer as (   
SELECT 
    date_trunc('month', fix_b_att_maxstart) as install_month, 
    fix_b_att_maxstart,  
    fix_s_att_account 
FROM new_customers_pre
)

--- ### Interactions

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

, truckrolls as (
SELECT 
    create_dte_ojb, 
    job_no_ojb, 
    sub_acct_no_sbb
FROM "lcpr.stage.dev"."truckrolls"
)

--- ### Early tickets

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

, new_customer_interactions_info AS (
SELECT 
    fix_s_att_account, 
    install_month, 
    interaction_start_month, 
    fix_b_att_maxstart,
    case when date_diff('week',cast(fix_b_att_maxstart AS DATE), cast(interaction_date AS DATE)) <= 7 then fix_s_att_account else null end as early_interaction_flag
FROM new_customer A 
LEFT JOIN relevant_interactions B 
    ON A.fix_s_att_account = cast(B.customer_id as bigint) 
    --AND interaction_type = 'Technical'
-- GROUP BY 1,2,3
)

SELECT 
    date_add('month', 0,install_month) as odr_s_dim_month, 
    count(distinct early_interaction_flag) as opd_s_mes_uni_early_tickets, 
    count(distinct fix_s_att_account) as fmc_s_att_account
FROM new_customer_interactions_info 
GROUP BY 1 
ORDER BY 1
