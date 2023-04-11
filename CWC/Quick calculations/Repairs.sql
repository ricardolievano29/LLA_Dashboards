WITH

parameters as (SELECT date('2022-12-01') as input_month)

 ,FMC_Table AS
( SELECT  Month,E_Final_Tech_Flag, E_FMC_Segment, E_FMCType, E_FinalTenureSegment,fixed_account,finalchurnflag,waterfall_flag,mainmovement FROM "lla_cco_int_ana_dev"."cwc_fmc_churn_dev" WHERE month = DATE(dt)
    -- CB -- Al usar esta condición nos estamos asegurando que solo revisemo EN ESTE subquery el mes del input_month y nada más (el subquery de clean_interaction_time tiene otra lógica que no haría sentido por la que se usa en este subquery)
    AND month = (SELECT input_month FROM parameters) 
)



,clean_interactions_base as(
select interaction_start_time, interaction_end_time,account_id,interaction_id,interaction_status, row_number() OVER (PARTITION BY REGEXP_REPLACE(account_id,'[^0-9 ]',''), cast(interaction_start_time as date) ORDER BY interaction_start_time desc) as row_num
 from "db-stage-prod-lf"."interactions_cwc" 
where lower(org_cntry) like '%jam%'
and date_trunc('Month', date(interaction_start_time)) between (SELECT date_add('month', -1, input_month) FROM parameters) and
(SELECT input_month FROM parameters) 
)

,repair_times AS(

SELECT date_trunc('Month', date(interaction_start_time)) as Repair_Month, account_id_2 as account, interaction_start_time, interaction_end_time,
date_diff('day', interaction_start_time, interaction_end_time) as solving_time
FROM (select *, REGEXP_REPLACE(account_id,'[^0-9 ]','') as account_id_2
from (select * from clean_interactions_base having row_num = 1)
where interaction_status = 'CLOSED'
AND Length (account_id) in (8,12))
)

SELECT count(distinct interaction_id)
FROM clean_interactions_base a 
WHERE 
    cast(account_id as varchar) in (SELECT cast(fixed_account as varchar) FROM FMC_Table)
    and interaction_status = 'CLOSED'
    and Length (account_id) in (8,12)
-- ORDER BY account_id desc
