--- ##### LCPR SPRINT 5  OPERATIONAL DRIVERS - NODES TICKET DENSITY #####

WITH

parameters as (SELECT date_trunc('month', date('2023-01-01')) as input_month)

--- ### ### ### DNA

, customers_dna_pre as (
SELECT 
    sub_acct_no_sbb, 
    date(dt) as dt, 
    bridger_addr_hse
    -- first_value(nr_bb_mac) over(partition by act_acct_cd order by dt) as last_nr_bb_mac, 
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE 
    date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
    and cust_typ_sbb = 'RES'
)

, customers_dna as (
SELECT 
    sub_acct_no_sbb, 
    bridger_addr_hse,
    min(dt) as first_dt, 
    max(dt) as last_dt
FROM customers_dna_pre
GROUP BY sub_acct_no_sbb, bridger_addr_hse
)

--- ### ### ### Interactions

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
    first_value(interaction_id) over(partition by account_id, interaction_date, interaction_channel, interaction_agent_id, interaction_purpose_descrip order by interaction_date desc) as interaction_id2
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

, tickets_and_truckrolls as (
SELECT
    distinct account_id, 
    interaction_id, 
    interaction_date, 
    interaction_purpose_descrip,
    other_interaction_info10,
    case when (
        lower(interaction_purpose_descrip) like '%ppv%problem%'
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
        then interaction_id else null
    end as techticket_flag,
    cast(job_no_ojb as varchar) as truckroll_flag
FROM interactions_fields2 a
LEFT JOIN truckrolls b
    ON a.interaction_date = cast(create_dte_ojb as date) and cast(a.account_id as varchar) = cast(b.sub_acct_no_sbb as varchar)
WHERE
    interaction_purpose_descrip not in ('Work Order Status', 'Default Call Wrapup', 'G:outbound Calls', 'Eq: Cust. First', 'Eq: Audit', 'Eq: Code Error', 'Downgrade Service', 'Disconnect Service', 'Rt: Dowgrde Service', 'Cust Service Calls')
    and date_trunc('month', interaction_date) = (SELECT input_month FROM parameters)
ORDER BY interaction_date
)

--- ### ### ### Voluntary Dx Orders by User

, vol_user_panel_pre as (
SELECT 
    cast(a.account_id as varchar) as account_id_vol_dx, 
    a.order_id as order_id, 
    date(cast(a.order_start_date as timestamp)) as order_start_date, 
    a.command_id as command_id, 
    a.cease_reason_desc as cease_reason_desc, 
    a.channel_desc as cease_reason_desc, 
    a.order_status as order_status
FROM "lcpr.stage.prod"."so_ln_lcpr" a
LEFT JOIN "lcpr.stage.prod"."so_hdr_lcpr" b
    ON a.order_id = b.order_id and cast(a.account_id as varchar) = cast(b.account_id as varchar)
WHERE
    date_trunc('month', date(a.order_start_date)) = (SELECT input_month FROM parameters)
    and a.command_id = 'V_DISCO'
    and a.command_id != 'NON PAY'
    and a.order_status = 'COMPLETE'
    and cast(b.lob_vo_count as double) + cast(b.lob_tv_count as double) + cast(b.lob_bb_count as double) > 0
)

, vol_user_panel as (
SELECT
    account_id_vol_dx, 
    order_start_date, 
    case when count(*) > 0 then 1 else 0 end as vol_churn_flg
FROM vol_user_panel_pre
GROUP BY account_id_vol_dx, order_start_date
)

, vol_churners as (
SELECT
    account_id_vol_dx, 
    case when sum(vol_churn_flg) > 0 then 1 else 0 end as vol_churner
FROM vol_user_panel
GROUP BY 1
)

, invol_churners as (
SELECT
    *
FROM "db_stage_dev"."lcpr_fmc_table_jan_mar23"
WHERE fmc_s_fla_churntype = 'Involuntary Churner'
)

--- ### Joining tables together

, join_interactions as (
SELECT
    a.*, 
    b.*
FROM tickets_and_truckrolls a
LEFT JOIN customers_dna b
    ON cast(a.account_id as varchar) = cast(b.sub_acct_no_sbb as varchar)
)

, interactions_panel as (
SELECT
    interaction_id, 
    interaction_date, 
    case when (techticket_flag is not null and truckroll_flag is null) then 1 else 0 end as tech_ticket_flg, 
    case when (techticket_flag is null and truckroll_flag is not null) or (techticket_flag is not null and truckroll_flag is not null) then 1 else 0 end as truckroll_flg, 
    case 
        when (techticket_flag is not null and truckroll_flag is null) then 'tech_ticket'
        when (techticket_flag is null and truckroll_flag is not null) or (techticket_flag is not null and truckroll_flag is not null) then 'tech_truckroll'
        else null
    end as interact_category, 
    account_id, 
    bridger_addr_hse
FROM join_interactions
)

, node_panel as (
SELECT 
    bridger_addr_hse, 
    sum(tech_ticket_flg) as tech_tickets,
    sum(truckroll_flg) as truckroll
    -- filter(array_sort(array_agg(distinct (case when tech_ticket_flg = 1 or truckroll_flg = 1 then last_interaction_disposition_info end))), x -> x IS NOT NULL) as list_disposition_tech_tickets_and_truckrolls
FROM interactions_panel
GROUP BY bridger_addr_hse
ORDER BY tech_tickets desc
)

, summary_tickets as (
SELECT
    *
    -- case when interact_category ='technical_claim' then 'technical_claim'
    --         when last_interaction_disposition_info like '%masivo%' then 'ticket_massive'
    --         else 'ticket_non-massive' end as interaction_group,
    -- case when last_interaction_disposition_info like '%masivo%pi%' then 'masive_internal_plant' 
    -- when last_interaction_disposition_info like '%masivo%pe%' or last_interaction_disposition_info like '%masivo%fib%'  then 'masive_external_plant'
    -- when last_interaction_disposition_info like '%masivo%elect%'then 'masive_electrical'
    -- when last_interaction_disposition_info like '%cambio%cpe%'
    --     or last_interaction_disposition_info like '%reemplaz%cpe%'
    --     then 'cpe_replacement'
    -- when last_interaction_disposition_info like '%reset%cpe%' or last_interaction_disposition_info like '%reinic%cpe%'  then 'reset_cpe'
    -- when last_interaction_disposition_info like '%config%cpe%' 
    --     or last_interaction_disposition_info like '%config%wifi%'
    --     or last_interaction_disposition_info like '%firmware%cpe%'
    --     or last_interaction_disposition_info like '%profile%cpe%'
    --     or last_interaction_disposition_info like '%enruta%modem%'  
    --     then 'config_cpe'
    -- when last_interaction_disposition_info like '%restablece%servicio%'then 'service_reestablished'
    -- when last_interaction_disposition_info like '%cliente%ausente%'
    --     or last_interaction_disposition_info like '%cliente%no%entrar%'
    --     or last_interaction_disposition_info like '%cliente%no%encuentra%'  
    --     or last_interaction_disposition_info like '%cliente%reagenda%'
    --     then 'customer_missess_appointment'
    -- when last_interaction_disposition_info like '%home%coax%'
    --     or last_interaction_disposition_info like '%coax%home%'
    --     then 'home_coax_issue'
    -- when last_interaction_disposition_info like '%distri%coax%'
    --     or last_interaction_disposition_info like '%coax%distri%'
    --     then 'distribution_coax_issue'
    -- when last_interaction_disposition_info like '%cliente%cierra%llamada%'
    --     then 'customer_hangs_up'
    -- else 'others' end as ticket_category
FROM interactions_panel
WHERE
    tech_ticket_flg = 1 or truckroll_flg = 1
ORDER BY tech_ticket_flg desc --- , last_interaction_disposition_info desc
)

, final_pre1 as (
SELECT
    bridger_addr_hse, 
    count(distinct n.sub_acct_no_sbb) as total_accounts, 
    count(distinct account_id_vol_dx) as vol_churners,
    count(distinct i.fmc_s_att_account) as invol_churners
FROM customers_dna n
LEFT JOIN vol_churners v
    ON cast(n.sub_acct_no_sbb as varchar) = cast(v.account_id_vol_dx as varchar)
LEFT JOIN invol_churners i
    ON cast(n.sub_acct_no_sbb as varchar) = cast(i.fmc_s_att_account as varchar)
GROUP BY 1
)

, final_pre2 as (
SELECT
    bridger_addr_hse, 
    count(distinct account_id) as unique_act_with_tickets, 
    -- count(distinct case when interaction_group = 'ticket_non-massive' THEN act_acct_cd END) AS unique_act_with_non_massive_tickets,
    -- count(distinct case when interaction_group = 'ticket_massive' THEN act_acct_cd END) AS unique_act_with_massive_tickets,
    count(distinct interaction_id) as num_tickets, 
    sum(truckroll_flg) as num_truck_rolls
FROM summary_tickets
GROUP BY bridger_addr_hse
)

, final as (
SELECT
    *, 
    unique_act_with_tickets, 
    -- unique_act_with_non_massive_tickets, 
    -- unique_act_with_massive_tickets, 
    num_tickets, 
    num_truck_rolls,
    cast(unique_act_with_tickets as double)/cast(total_accounts as double) as act_with_ticket_rate
    -- cast(unique_act_with_non_massive_tickets as double)/cast(total_accounts as double) as act_with_non_massive_ticket_rate,
    -- cast(unique_act_with_massive_tickets as double)/cast(total_accounts as double) as act_with_massive_ticket_rate
FROM final_pre1 a
LEFT JOIN final_pre2 b
    ON a.bridger_addr_hse = b.bridger_addr_hse
WHERE a.total_accounts > 30
)

-- SELECT
--     *, 
--     case when act_with_ticket_rate >= 0.06 then 1 else 0 end as "total_6%",
--     -- case when act_with_non_massive_ticket_rate >= 0.06 then 1 else 0 end as "non_massive_6%",
--     -- case when act_with_massive_ticket_rate >= 0.06 then 1 else 0 end as "massive_6%",
--     case when act_with_ticket_rate >= 0.1 then 1 else 0 end as "total_10%"
--     -- case when act_with_non_massive_ticket_rate >= then 1 else 0 end as "non_massive_10%",
--     -- case when act_with_massive_ticket_rate >= 0.1 then 1 else 0 end as "massive_10%"
-- FROM final
-- ORDER BY act_with_ticket_rate desc

--- ### KPI Calculation

SELECT 
    -- sum(case when act_with_ticket_rate >= 0.06 then 1 else 0 end)
    sum(case when act_with_ticket_rate >= 0.1 then 1 else 0 end)
FROM final

-- SELECT
    -- count(distinct bridger_addr_hse)
-- FROM final_pre2
