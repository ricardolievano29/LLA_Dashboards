WITH FMC_Table AS
( SELECT * FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" 
)

------------ reiterative tickets ---------------------------------------

,initial_table as 
(
SELECT date_trunc('Month', date(interaction_start_time)) as Ticket_Month, account_id_2 as account, 
last_value(interaction_start_time) over (partition by account_id_2, date_trunc('Month', date(interaction_start_time)) order by interaction_start_time) as last_int_dt, *
FROM (select *, REGEXP_REPLACE(account_id,'[^0-9 ]','') as account_id_2
from "db-stage-dev"."interactions_cwc"
where lower(org_cntry) like '%jam%'
))

, tickets_count as (

SELECT Ticket_Month, account, 
case when (length(account) = 8) THEN 'Cerillion' else 'Liberate' end as CRM
,count(distinct interaction_id) as tickets
FROM initial_table
WHERE interaction_start_time between (last_int_dt - interval '60' day) and last_int_dt
GROUP BY 1,2
)

,reiterations_summary AS(

SELECT t.*, 
CASE WHEN tickets = 1 THEN account else null end as one_tckt,
CASE WHEN tickets > 1 THEN account else null end as over1_tckt,
CASE WHEN tickets = 2 THEN account else null end as two_tckt,
CASE WHEN tickets >= 3 THEN account else null end as three_tckt
FROM tickets_count t

)

,reiterationtickets_flag AS(

SELECT f.*, Ticket_Month as RTicket_Month, one_tckt, over1_tckt, two_tckt, three_tckt
FROM FMC_Table f left join reiterations_summary r on f.fixed_account = r.account
and f.Month = r.Ticket_Month

)

----------- Outlier repair times - interactions approach -----------------------

,repair_times AS(

SELECT date_trunc('Month', date(interaction_start_time)) as Repair_Month, account_id_2 as account, interaction_start_time, interaction_end_time,
date_diff('day', interaction_start_time, interaction_end_time) as solving_time
FROM (select *, REGEXP_REPLACE(account_id,'[^0-9 ]','') as account_id_2
from "db-stage-dev"."interactions_cwc"
where lower(org_cntry) like '%jam%' and interaction_status = 'CLOSED'
AND Length (account_id) in (8,12))
)

, outlier_times AS(

SELECT Repair_month, account, interaction_start_time, interaction_end_time, solving_time,
CASE WHEN max(solving_time) > 4 THEN account else null end as outlier_repair
FROM repair_times r
GROUP BY 1,2,3,4,5
ORDER BY 1 desc, 5 desc, account
)

, outlier_repair_flag AS
(
SELECT f.*, case when length (f.fixed_account) = 8 then 'Cerillion'
else 'Liberate' END AS CRM, repair_month,
CASE when account is not null then account else null end as techticket,
outlier_repair
FROM reiterationtickets_flag f 
left join outlier_times o on f.fixed_account = o.account and f.month = o.repair_month

)

----------------- Tech tickets density ---------------------------------------

, tickets_per_account AS(
SELECT date_trunc('Month', date(interaction_start_time)) as Ticket_Month, account, count(distinct interaction_id) as numtickets
FROM (select *, REGEXP_REPLACE(account_id,'[^0-9 ]','') as account
from "db-stage-dev"."interactions_cwc"
where lower(org_cntry) like '%jam%' 
)
GROUP BY 1,2
)


,records_fixed_accounts as (
select distinct Month, fixed_account, count(*) as numrecords
FROM outlier_repair_flag
WHERE month = date(dt)
Group by 1,2
)

,ticket_density_flag AS(

SELECT f.*, numtickets, numrecords, (numtickets/numrecords) as adj_tickets
FROM outlier_repair_flag f  INNER JOIN records_fixed_accounts r 
ON f.fixed_account = r.fixed_account and f.Month = r.Month 
LEFT JOIN tickets_per_account t
ON f.fixed_account = t.account AND f.month = t.Ticket_Month
)

,results_table_S5 as (SELECT Month,E_Final_Tech_Flag, E_FMC_Segment, E_FMCType, E_FinalTenureSegment, count(distinct fixed_account) as activebase, count(distinct one_tckt) as one_ticket,  count(distinct over1_tckt) over1_ticket, count(distinct two_tckt) as two_tickets, count(distinct three_tckt) as three_more_tickets,
count (distinct techticket) as ticket_customers,
sum(adj_tickets) as totaltickets ,count(distinct outlier_repair) as outlier_repairs 
FROM ticket_density_flag
where finalchurnflag <> 'Fixed Churner' and waterfall_flag <> 'Downsell-Fixed Customer Gap' and waterfall_flag <> 'Fixed Base Exceptions' and mainmovement <> '6.Null last day' and waterfall_flag <> 'Churn Exception'
and month = date(dt)
group by 1,2,3,4,5
order by 1,2,3,4,5)

select * from results_table_S5
