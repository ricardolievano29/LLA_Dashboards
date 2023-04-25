with

parameters as (SELECT date_trunc('month', date('2023-02-01')) as input_month)

--------------------------------Input Tables-------------------------------------------------------
,fmc_table as(
select distinct fmc_s_dim_month as month,'LCPR' as Opco,'Puerto_Rico' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as  facet,null as journey_waypoint,null as kpi_name,null as kpi_meas,null as kpi_num,null as kpi_den, 'M-0' as Kpi_delay_display,null as Network
from 
    (SELECT * FROM "db_stage_dev"."lcpr_fmc_table_dec_mar23" UNION ALL (SELECT * FROM "db_stage_dev"."lcpr_fmc_table_jan_mar23") UNION ALL (SELECT * FROM"db_stage_dev"."lcpr_fmc_table_feb_mar23"))
where fmc_s_dim_month = (SELECT input_month FROM parameters)
)

,FIXED_DATA AS(
SELECT  DATE_TRUNC('MONTH', cast(DATE_PARSE(CAST(month AS VARCHAR(10)), '%m/%d/%Y') as date)) as month,
        market,
        network,
        cast("total subscribers" as double) as total_subscribers,
        cast("assisted installations" as double) as assisted_instalations,
        cast(mtti as double) as mtti,
        cast("truck rolls" as double) as truck_rolls,
        cast(mttr as double) as mttr,
        cast(scr as double) as scr,
        cast("i-elf(28days)" as double) as i_elf_28days,
        cast("r-elf(28days)" as double) as r_elf_28days,
        cast("i-sl" as double) as i_sl,
        cast("r-sl" as double) as r_sl
from "db_stage_dev"."service_delivery" 
)

,service_delivery as(
SELECT  distinct month as Month,
        Network,
        'LCPR' as Opco,
        'Puerto_Rico' as Market,
        'Large' as MarketSize,
        'Fixed' as Product,
        'B2C' as Biz_Unit,
        --total_subscribers as Total_Users,
        round(assisted_instalations,0) as Install,
        round(mtti,2) as MTTI,
        --assisted_instalations*mtti as Inst_MTTI,
        round(truck_rolls,0) as Repairs,
        round(mttr,2) as MTTR,
        --truck_rolls*mttr as Rep_MTTR,
        round(scr,2) as Repairs_1k_rgu,
        round((100-i_elf_28days)/100,4) as FTR_Install,
        round((100-r_elf_28days)/100,4) as FTR_Repair,
        round((i_sl/assisted_instalations),4) as Installs_SL,
        round((r_sl/truck_rolls),4) as Repairs_SL,
        --(100-i_elf_28days)*assisted_instalations as FTR_Install_M,
        --(100-r_elf_28days)*truck_rolls as FTR_Repair_M,
        round(i_sl,0) as Inst_SL,
        round(r_sl,0) as Rep_SL
FROM    FIXED_DATA
WHERE   market = 'Puerto Rico' 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
ORDER BY 1,2,3
)
,nps_kpis as(
select distinct date(date_parse(cast(month as varchar),'%Y%m%d')) as month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, kpi_delay_display,Network from "db_stage_dev"."nps" where opco='LCPR')
,wanda_kpis as(
select date(date_parse(cast(month as varchar),'%Y%m%d')) as month,Opco,'Puerto_Rico' as market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,kpi_delay_display,network from "db_stage_dev"."wanda"  where opco='LCPR')

,digital_sales as(
select date(date_parse(cast(month as varchar),'%Y%m%d')) as month,opco,market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,kpi_delay_display,kpi_sla,network
from "db_stage_dev"."digital_sales" where opco='LCPR')


, customers_dna_pre as (SELECT sub_acct_no_sbb, date(dt) as dt, bridger_addr_hse FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" WHERE date_trunc('month', date(dt)) = (SELECT input_month FROM parameters) and cust_typ_sbb = 'RES')

, customers_dna as (SELECT sub_acct_no_sbb, bridger_addr_hse, min(dt) as first_dt, max(dt) as last_dt FROM customers_dna_pre GROUP BY sub_acct_no_sbb, bridger_addr_hse)

, clean_interaction_time as (SELECT * FROM "lcpr.stage.prod"."lcpr_interactions_csg" WHERE (cast(interaction_start_time as varchar) != ' ') and (interaction_start_time is not null) and date_trunc('month', cast(substr(cast(interaction_start_time as varchar),1,10) as date)) between ((SELECT input_month FROM parameters)) and ((SELECT input_month FROM parameters) + interval '1' month - interval '1' day) and account_type = 'RES')

, interactions_fields as (SELECT *, cast(substr(cast(interaction_start_time as varchar), 1, 10) as date) as interaction_date, date_trunc('month', cast(substr(cast(interaction_start_time as varchar), 1, 10) as date)) as month FROM clean_interaction_time)

, interactions_not_repeated as (SELECT first_value(interaction_id) over(partition by account_id, interaction_date, interaction_channel, interaction_agent_id, interaction_purpose_descrip order by interaction_date desc) as interaction_id2 FROM interactions_fields)

, interactions_fields2 as (SELECT * FROM interactions_not_repeated a LEFT JOIN interactions_fields b ON a.interaction_id2 = b.interaction_id)

, truckrolls as (SELECT create_dte_ojb, job_no_ojb, sub_acct_no_sbb FROM "lcpr.stage.dev"."truckrolls" )

, tickets_and_truckrolls as (SELECT distinct account_id, interaction_id, interaction_date, interaction_purpose_descrip, other_interaction_info10, case when (lower(interaction_purpose_descrip) like '%ppv%problem%'or lower(interaction_purpose_descrip) like '%hsd%problem%' or lower(interaction_purpose_descrip) like '%cable%problem%' or lower(interaction_purpose_descrip) like '%tv%problem%' or lower(interaction_purpose_descrip) like '%video%problem%' or lower(interaction_purpose_descrip) like '%tel%problem%' or lower(interaction_purpose_descrip) like '%phone%problem%' or lower(interaction_purpose_descrip) like '%int%problem%' or lower(interaction_purpose_descrip) like '%line%problem%' or lower(interaction_purpose_descrip) like '%hsd%issue%' or lower(interaction_purpose_descrip) like '%ppv%issue%' or lower(interaction_purpose_descrip) like '%video%issue%' or lower(interaction_purpose_descrip) like '%tel%issue%' or lower(interaction_purpose_descrip) like '%phone%issue%' or lower(interaction_purpose_descrip) like '%int%issue%' or lower(interaction_purpose_descrip) like '%line%issue%' or lower(interaction_purpose_descrip) like '%cable%issue%' or lower(interaction_purpose_descrip) like '%tv%issue%' or lower(interaction_purpose_descrip) like '%bloq%' or lower(interaction_purpose_descrip) like '%slow%' or lower(interaction_purpose_descrip) like '%slow%service%' or lower(interaction_purpose_descrip) like '%service%tech%' or lower(interaction_purpose_descrip) like '%tech%service%' or lower(interaction_purpose_descrip) like '%no%service%' or lower(interaction_purpose_descrip) like '%hsd%no%' or lower(interaction_purpose_descrip) like '%hsd%slow%' or lower(interaction_purpose_descrip) like '%hsd%intermit%' or lower(interaction_purpose_descrip) like '%no%brows%' or lower(interaction_purpose_descrip) like '%phone%cant%' or lower(interaction_purpose_descrip) like '%phone%no%' or lower(interaction_purpose_descrip) like '%no%connect%' or lower(interaction_purpose_descrip) like '%no%conect%' or lower(interaction_purpose_descrip) like '%no%start%' or lower(interaction_purpose_descrip) like '%equip%' or lower(interaction_purpose_descrip) like '%intermit%' or lower(interaction_purpose_descrip) like '%no%dat%' or lower(interaction_purpose_descrip) like '%dat%serv%' or lower(interaction_purpose_descrip) like '%int%data%' or lower(interaction_purpose_descrip) like '%tech%' or lower(interaction_purpose_descrip) like '%supp%' or lower(interaction_purpose_descrip) like '%outage%' or lower(interaction_purpose_descrip) like '%mass%' or lower(interaction_purpose_descrip) like '%discon%warn%') and (lower(interaction_purpose_descrip) not like '%work%order%status%' and lower(interaction_purpose_descrip) not like '%default%call%wrapup%' and lower(interaction_purpose_descrip) not like '%bound%call%' and lower(interaction_purpose_descrip) not like '%cust%first%' and lower(interaction_purpose_descrip) not like '%audit%' and lower(interaction_purpose_descrip) not like '%eq%code%' and lower(interaction_purpose_descrip) not like '%downg%' and lower(interaction_purpose_descrip) not like '%upg%' and lower(interaction_purpose_descrip) not like '%vol%discon%' and lower(interaction_purpose_descrip) not like '%discon%serv%' and lower(interaction_purpose_descrip) not like '%serv%call%') then interaction_id else null end as techticket_flag, cast(job_no_ojb as varchar) as truckroll_flag FROM interactions_fields2 a LEFT JOIN truckrolls b ON a.interaction_date = cast(create_dte_ojb as date) and cast(a.account_id as varchar) = cast(b.sub_acct_no_sbb as varchar) WHERE interaction_purpose_descrip not in ('Work Order Status', 'Default Call Wrapup', 'G:outbound Calls', 'Eq: Cust. First', 'Eq: Audit', 'Eq: Code Error', 'Downgrade Service', 'Disconnect Service', 'Rt: Dowgrde Service', 'Cust Service Calls') and date_trunc('month', interaction_date) = (SELECT input_month FROM parameters) ORDER BY interaction_date)

, vol_user_panel_pre as (SELECT cast(a.account_id as varchar) as account_id_vol_dx, a.order_id as order_id, date(cast(a.order_start_date as timestamp)) as order_start_date, a.command_id as command_id, a.cease_reason_desc as cease_reason_desc, a.channel_desc as cease_reason_desc, a.order_status as order_status FROM "lcpr.stage.prod"."so_ln_lcpr" a LEFT JOIN "lcpr.stage.prod"."so_hdr_lcpr" b ON a.order_id = b.order_id and cast(a.account_id as varchar) = cast(b.account_id as varchar) WHERE date_trunc('month', date(a.order_start_date)) = (SELECT input_month FROM parameters) and a.command_id = 'V_DISCO'     and a.command_id != 'NON PAY' and a.order_status = 'COMPLETE' and cast(b.lob_vo_count as double) + cast(b.lob_tv_count as double) + cast(b.lob_bb_count as double) > 0)

, vol_user_panel as (SELECT account_id_vol_dx, order_start_date, case when count(*) > 0 then 1 else 0 end as vol_churn_flg FROM vol_user_panel_pre GROUP BY account_id_vol_dx, order_start_date)

, vol_churners as (SELECT account_id_vol_dx, case when sum(vol_churn_flg) > 0 then 1 else 0 end as vol_churner FROM vol_user_panel GROUP BY 1)

, invol_churners as (SELECT * FROM "db_stage_dev"."lcpr_fmc_table_jan_mar23" WHERE fmc_s_fla_churntype = 'Involuntary Churner')

, join_interactions as (SELECT a.*, b.* FROM tickets_and_truckrolls a LEFT JOIN customers_dna b ON cast(a.account_id as varchar) = cast(b.sub_acct_no_sbb as varchar))

, interactions_panel as (SELECT interaction_id, interaction_date, case when (techticket_flag is not null and truckroll_flag is null) then 1 else 0 end as tech_ticket_flg, case when (techticket_flag is null and truckroll_flag is not null) or (techticket_flag is not null and truckroll_flag is not null) then 1 else 0 end as truckroll_flg, case when (techticket_flag is not null and truckroll_flag is null) then 'tech_ticket' when (techticket_flag is null and truckroll_flag is not null) or (techticket_flag is not null and truckroll_flag is not null) then 'tech_truckroll' else null end as interact_category, account_id, bridger_addr_hse FROM join_interactions)

, node_panel as (SELECT bridger_addr_hse, sum(tech_ticket_flg) as tech_tickets, sum(truckroll_flg) as truckroll FROM interactions_panel GROUP BY bridger_addr_hse ORDER BY tech_tickets desc)

, summary_tickets as (SELECT * FROM interactions_panel WHERE tech_ticket_flg = 1 or truckroll_flg = 1 ORDER BY tech_ticket_flg desc)

, final_pre1 as (SELECT bridger_addr_hse, bridger_addr_hse as node_id, count(distinct n.sub_acct_no_sbb) as total_accounts, count(distinct account_id_vol_dx) as vol_churners, count(distinct i.fmc_s_att_account) as invol_churners FROM customers_dna n LEFT JOIN vol_churners v ON cast(n.sub_acct_no_sbb as varchar) = cast(v.account_id_vol_dx as varchar) LEFT JOIN invol_churners i ON cast(n.sub_acct_no_sbb as varchar) = cast(i.fmc_s_att_account as varchar) GROUP BY 1)

, final_pre2 as (SELECT bridger_addr_hse, count(distinct account_id) as unique_act_with_tickets, count(distinct interaction_id) as num_tickets, sum(truckroll_flg) as num_truck_rolls FROM summary_tickets GROUP BY bridger_addr_hse)

, final as (SELECT *, unique_act_with_tickets, num_tickets, num_truck_rolls, case when cast(unique_act_with_tickets as double)/cast(total_accounts as double) > 0.06 then node_id else null end as nodes_with_ticket_rate FROM final_pre1 a LEFT JOIN final_pre2 b ON a.bridger_addr_hse = b.bridger_addr_hse WHERE a.total_accounts > 30)

, nodes_result as (SELECT 'LCPR' as Opco, cast(count(distinct nodes_with_ticket_rate) as double)/cast(count(distinct node_id) as double) as nodes_ticket_density_6pct FROM final)

, nodes_fmc as (SELECT month, a.Opco, Market, MarketSize, Product, Biz_Unit, facet, journey_waypoint, kpi_name, nodes_ticket_density_6pct as kpi_meas, kpi_num, kpi_den, Kpi_delay_display, Network FROM fmc_table a LEFT JOIN nodes_result b ON a.Opco = b. Opco)

-------------------------------------Churn Dashboard kpis-----------------------------------------------------
,GrossAdds_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display, Network from fmc_table)
,ActiveBase_Flag1 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from fmc_table)
,ActiveBase_Flag2 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,TechTickets_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,MRCChanges_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'pay' as journey_waypoint,'Customers_w_MRC_Changes_5%+_Excl_Plan' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,SalesSoftDx_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Sales_to_Soft_Dx' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,EarlyIssues_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Customer_Callers_2+calls_21Days' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,LongInstall_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Breech_Cases_Install_6+Days' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,EarlyTickets_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Early_Tech_Tix_-7Weeks' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,RepeatedCall_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'Repeat_Callers_2+Calls' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,OutlierRepair_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-tech' as journey_waypoint,'Breech_Cases_Repair_4+Days' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,MountingBill_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'pay' as journey_waypoint,'Customers_w_Mounting_Bills' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
-------------------------------------Service Delivery Kpis---------------------------------------------------
,installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'get' as journey_waypoint,'Installs' as kpi_name, Install as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
,MTTI as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI' as kpi_name, mtti as kpi_meas, null as kpi_num,null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,MTTI_SL as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI_SL' as kpi_name, installs_sl as kpi_meas, inst_sl as kpi_num,install as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,ftr_installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'get' as journey_waypoint,'FTR_Installs' as kpi_name, ftr_install as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,justrepairs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-tech' as journey_waypoint,'Repairs' as kpi_name, repairs as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_displa,Network from service_delivery)
,mttr as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR' as kpi_name, mttr as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,mttr_sl as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR_SL' as kpi_name, repairs_sl as kpi_meas, rep_sl as kpi_num,repairs as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,ftrrepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-tech' as journey_waypoint,'FTR_Repair' as kpi_name, ftr_repair as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
,repairs1k as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-tech' as journey_waypoint,'Repairs_per_1k_RGU' as kpi_name, Repairs_1k_rgu as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
-------------------------------------NPS Kpis-----------------------------------------------------
,tBuy as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas,null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tBuy')
,tinstall as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas,null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tInstall')
,tpay as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tPay' as kpi_name,kpi_meas as kpi_meas, null as kpi_num,	null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tpay')
,helpcare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tHelp_Care')
,helprepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tHelp_Repair' as kpi_name, kpi_meas as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tHelp_repair')
,pnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='pNPS')
,rnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas, null as kpi_num,null as kpi_den,Kpi_delay_display,Network from nps_kpis where kpi_name='rNPS')
-------------------------------------Wanda Kpis-----------------------------------------------------
,cccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='CC_SL_Care')
,cctech as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='CC_SL_Tech')
,chatbot as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Chatbot_Containment_Care')
,carecall as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Care_Calls_Intensity')
,techcall as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Tech_Calls_Intensity')
,chahtbottech as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-tech' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Chatbot_Containment_Tech')
,frccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-call' as journey_waypoint,'FCR_Care' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='FCR_Care')
,frctech as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-call' as journey_waypoint,'FCR_Tech' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='FCR_Tech')

-------------------------------------Other Kpis-----------------------------------------------------
,highrisk as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'use' as journey_waypoint,'High_Tech_Call_Nodes_+6%Monthly' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from nodes_fmc)
,payments as(
select distinct month,opco,market,marketsize,product,biz_unit,'digital_shift' as facet,'pay' as journey_waypoint,'Digital_Payments' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,ecommerce as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'buy' as journey_waypoint,'e-Commerce' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network,kpi_sla from digital_sales)
,ftr_billing as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'pay' as journey_waypoint,'FTR_Billing' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,installscalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'get' as journey_waypoint,'Install_Calls/Installs' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,MTTBTR as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'pay' as journey_waypoint,'MTTBTR' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,selfinstalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'get' as journey_waypoint,'Self_Installs' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,mttb as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'buy' as journey_waypoint,'MTTB' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,Buyingcalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'buy' as journey_waypoint,'Buying_Calls/GA' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,billbill as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'pay' as journey_waypoint,'Billing Calls per Bill Variation' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from fmc_table)
-----------------------------------------join--------------------------------------
,join_churn as (
select * from GrossAdds_Flag union all select * from ActiveBase_Flag1 union all select * from ActiveBase_Flag2 union all select * from TechTickets_Flag union all select * from MRCChanges_Flag union all select * from SalesSoftDx_Flag union all select * from EarlyIssues_Flag union all select * from LongInstall_Flag union all select * from EarlyTickets_Flag union all select * from RepeatedCall_Flag union all select * from OutlierRepair_Flag union all select * from MountingBill_Flag)
,join_service_delivery as(
select * from join_churn union all select * from installs union all select * from MTTI union all select * from ftr_installs union all select * from justrepairs union all select * from mttr union all select * from ftrrepair union all select * from repairs1k)
,join_nps as(
select * from join_service_delivery union all select * from tBuy union all select * from tinstall union all select * from tpay union all select * from helpcare union all select * from helprepair union all select * from pnps union all select * from rnps)
,join_wanda as(
select * from join_nps union all select * from billbill union all select * from cccare union all select * from cctech union all select * from chatbot union all select * from carecall union all select * from techcall union all select * from chahtbottech)
,join_others as(
select *,null as kpi_sla from(select * from join_wanda union all select * from highrisk union all select * from payments union all select * from frccare union all select * from frctech union all select * from ftr_billing union all select * from installscalls union all select * from MTTBTR union all select * from selfinstalls union all select * from  mttb union all select * from Buyingcalls union all select * from mtti_sl union all select * from mttr_sl)
union all select * from ecommerce
)
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den, kpi_sla,Kpi_delay_display,null as kpi_disclaimer_display,null as kpi_disclaimer_meas,Network,year(Month) as ref_year,month(month) as ref_mo,null as kpi_sla_below_threshold,null as kpi_sla_middling_threshold,null as kpi_sla_above_threshold,null as kpi_sla_far_below_threshold,null as kpi_sla_far_above_threshold
--facet,journey_waypoint,kpi_name
from join_others
where month=(SELECT input_month FROM parameters) -- and kpi_name = 'High_Tech_Call_Nodes_+6%Monthly'
order by 1,kpi_name
