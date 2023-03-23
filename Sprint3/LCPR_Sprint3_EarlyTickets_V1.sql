with parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT DATE_TRUNC('month',DATE('2022-12-01')) AS input_month
)
---New customer directly from the DNA
,new_customers_pre as (
SELECT (CAST(CAST(first_value(connect_dte_sbb) over (PARTITION BY sub_acct_no_sbb order by DATE(dt) DESC) AS TIMESTAMP) AS DATE)) as fix_b_att_maxstart,   
    SUB_ACCT_NO_SBB as fix_s_att_account FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" WHERE play_type <> '0P'
    AND cust_typ_sbb = 'RES' and delinquency_days = 0 having date_trunc('month',date(CONNECT_DTE_SBB)) 
    -- > date('2022-10-01') 
    = (select input_month from parameters) 
    order by 1)
    
 ,new_customer as (   select date_trunc('month', fix_b_att_maxstart) as install_month,fix_b_att_maxstart,  fix_s_att_account from new_customers_pre )

,interactions as (
select 
    customer_id, 
    interaction_start_time, 
    date_trunc('month',date(interaction_start_time)) as interaction_start_month 
from "lcpr.stage.prod"."lcpr_interactions_csg" 
where 
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

,new_customer_interactions_info AS (
SELECT fix_s_att_account,install_month,interaction_start_month,fix_b_att_maxstart,
        case when DATE_DIFF('week',CAST(fix_b_att_maxstart AS DATE),CAST(interaction_start_time AS DATE)) <= 7 then fix_s_att_account else null end as early_interaction_flag
FROM new_customer A LEFT JOIN interactions B ON A.fix_s_att_account = cast(B.customer_id as bigint) 
    --AND interaction_type = 'Technical'
-- GROUP BY 1,2,3
)

select date_add('month', 0,install_month), count(distinct early_interaction_flag) as opd_s_mes_uni_early_tickets
,count(distinct fix_s_att_account) as fixed_Account from new_customer_interactions_info group by 1 order by 1
