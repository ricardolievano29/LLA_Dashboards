with parameters as (
select
--##############################################################
--### Change Date in this line to define paying period #########
date('2022-10-31') as start_date,
date('2022-11-23') as end_date,
90 as max_overdue_active_base

--##############################################################

),


fi_outs_tbl as (
select act_acct_cd, dt,
case when fi_outst_age = '90' then 1 else 0 end as day_90,
case when fi_outst_age is null then -1 else cast(fi_outst_age as int) end as fi_outst_age,
case when LENGTH(act_acct_cd)=8 THEN 'CERILLION' ELSE 'LIBERATE' END AS CRM,

first_value(date(dt)) over(partition by act_acct_cd order by dt) as first_dt,
first_value(date(dt)) over(partition by act_acct_cd order by dt desc) as last_dt,
first_value(act_cust_strt_dt) over(partition by act_acct_cd order by dt) as cust_start_dt,
--act_acct_inst_dt
first_value(pd_mix_cd) over(partition by act_acct_cd order by dt) as first_pd_mix_cd,
first_value(pd_mix_cd) over(partition by act_acct_cd order by dt desc) as last_pd_mix_cd,
first_value(pd_mix_nm) over(partition by act_acct_cd order by dt) as first_pd_mix_nm,
first_value(pd_mix_nm) over(partition by act_acct_cd order by dt desc) as last_pd_mix_nm,
CASE WHEN length(cast(act_acct_cd as varchar))=8 then 'HFC' 
    WHEN NR_FDP<>'' and NR_FDP<>' ' and NR_FDP is not null THEN 'FTTH' 
    WHEN pd_vo_tech='FIBER' THEN 'FTTH' 
    WHEN (pd_bb_prod_nm like '%GPON%'  OR pd_bb_prod_nm like '%FTT%') and 
    (pd_bb_prod_nm not like '%ADSL%' and pd_bb_prod_nm not like '%VDSL%') THEN 'FTTH' 
    ELSE 'COPPER' END as TECHNOLOGY_PROXY,
pd_bb_prod_nm, pd_tv_prod_nm, pd_mix_nm
from "db-analytics-prod"."dna_tbl_fixed"
where org_cntry = 'Jamaica' and ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
--and substring(dt, 1,7) ='2022-01' --and cast(dt as date) <=cast('2022-01-26' as date)
and date(dt) BETWEEN (select start_date from parameters) and (select end_date from parameters) 
and dt <> '202110'
),

user_tbl as (
select act_acct_cd,
max(day_90) as day_90_flag,
min(CRM) as CRM,
min(fi_outst_age) as min_fi_outst_age, 
max(fi_outst_age) as max_fi_outst_age,
min(first_fi_outst_age) as first_fi_outst_age,
min(last_fi_outst_age) as last_fi_outst_age,

min(first_dt) as first_dt,
min(last_dt) as last_dt,
case when DATE_DIFF('day', cast(min(cust_start_dt) as date), cast(min(first_dt) as date)) <= 183 THEN  '1. 0 to 6 months'
    when DATE_DIFF('day', cast(min(cust_start_dt) as date), cast(min(first_dt) as date)) <= 365 THEN  '2. 7 to 12 months'
    when DATE_DIFF('day', cast(min(cust_start_dt) as date), cast(min(first_dt) as date)) > 365 THEN  '3. More than 12 months'
end as tenure,
min(first_pd_mix_cd) as first_pd_mix_cd,
min(last_pd_mix_cd) as last_pd_mix_cd,
min(first_pd_mix_nm) as first_pd_mix_nm,
min(last_pd_mix_nm) as last_pd_mix_nm,
case when min(first_pd_mix_nm)  LIKE '%BO%' then 1 else 0 end as start_bb,
case when min(first_pd_mix_nm)  LIKE '%VO%' then 1 else 0 end as start_vo,
case when min(first_pd_mix_nm)  LIKE '%TV%' then 1 else 0 end as start_tv,
case when min(first_pd_mix_cd) = '1P' then 1
    when min(first_pd_mix_cd) = '2P' then 2
    when min(first_pd_mix_cd) = '3P' then 3 else null end as start_rgu_number,
case when min(last_pd_mix_nm)  LIKE '%BO%' then 1 else 0 end as end_bb,
case when min(last_pd_mix_nm)  LIKE '%VO%' then 1 else 0 end as end_vo,
case when min(last_pd_mix_nm)  LIKE '%TV%' then 1 else 0 end as end_tv,
case when max(last_dt) < (select end_date from parameters) then 
        (case when min(first_pd_mix_cd) = '1P' then 1 when min(first_pd_mix_cd) = '2P' then 2 when min(first_pd_mix_cd) = '3P' then 3 else 0 end)
    else 
        (case when min(first_pd_mix_nm)  LIKE '%BO%' and (min(last_pd_mix_nm) NOT LIKE '%BO%' or min(last_pd_mix_nm) is null) then 1 else 0 end +
        case when min(first_pd_mix_nm)  LIKE '%VO%' and (min(last_pd_mix_nm) NOT LIKE '%VO%' or min(last_pd_mix_nm) is null)then 1 else 0 end +
        case when min(first_pd_mix_nm)  LIKE '%TV%' and (min(last_pd_mix_nm) NOT LIKE '%TV%' or min(last_pd_mix_nm) is null)then 1 else 0 end) end AS net_rgu_loss,
case 
    when min(last_pd_mix_cd) is null then 0
    when min(last_pd_mix_cd) = '1P' then 1
    when min(last_pd_mix_cd) = '2P' then 2
    when min(last_pd_mix_cd) = '3P' then 3 else null end as close_rgu_number,
min(last_TECHNOLOGY_PROXY) as TECHNOLOGY_PROXY,
CASE WHEN (min(first_fi_outst_age) < 90 AND min(last_fi_outst_age) >= 90 )
    or (min(first_fi_outst_age) < 90 and try(filter(array_agg(fi_outst_age order by dt desc), x->x != -1)[1]) >= 90 and max(last_dt)<(select end_date from parameters ))
    THEN 1 ELSE 0 END as net_inv_churn_flag

    from (select *,
        first_value(fi_outst_age) over(partition by act_acct_cd order by dt) as first_fi_outst_age,
        first_value(fi_outst_age) over(partition by act_acct_cd order by dt desc) as last_fi_outst_age,
        first_value(TECHNOLOGY_PROXY) over(partition by act_acct_cd order by dt desc) as last_TECHNOLOGY_PROXY
        from fi_outs_tbl) 
where first_fi_outst_age < (select max_overdue_active_base from parameters) 
group by act_acct_cd
),



panel_so as (
    select account_id, order_id,
    case when max(lob_vo_count)> 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end as vol_lob_vo_count, 
    case when max(lob_bb_count) > 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end  as vol_lob_bb_count, 
    case when max(lob_tv_count) > 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end  as vol_lob_tv_count, 
--    case when max(lob_other_count) > 0 then 1 else 0 end  as vol_lob_other_count,
    --DATE_TRUNC('month',  order_start_date) as completed_month,
    DATE_TRUNC('month', completed_date) as completed_month,
    cease_reason_group,org_cntry,order_status,network_type, order_type, account_type
    from (
        select *
    FROM "db-stage-dev"."so_hdr_cwc"
    WHERE
        org_cntry = 'Jamaica'
        AND (cease_reason_group in ('Voluntary', 'Customer Service Transaction', 'Involuntary') or cease_reason_group is null)
        AND (network_type NOT IN ('LTE','MOBILE') or network_type is null)
        --AND order_status = 'COMPLETED'
        AND account_type = 'Residential'
        --AND order_type = 'DEACTIVATION'
        AND ((cease_reason_group in ('Voluntary', 'Involuntary') and date(completed_date) BETWEEN (select start_date from parameters) and (select end_date from parameters)) or
        ((cease_reason_group = 'Customer Service Transaction' or cease_reason_group is null) and date(completed_date) BETWEEN ((select start_date from parameters)- interval '20' day) and (select end_date from parameters))
        or date(order_start_date) between (select start_date from parameters) and (select end_date from parameters)
        )

        )
       
    group by account_id, order_id, 
    DATE_TRUNC('month', completed_date),
    cease_reason_group,org_cntry,order_status,network_type, order_type, account_type
    order by completed_month, account_id, order_id
),

user_panel_so as (
    select account_id,
    case when sum(case when cease_reason_group = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end) > 0 then 1 else 0 end as vol_churn_flag,
    case when max(vol_lob_bb_count)>0 then 1 else 0 end as vol_lob_bb_count,
    case when max(vol_lob_tv_count)>0 then 1 else 0 end as vol_lob_tv_count,
    case when max(vol_lob_vo_count)>0 then 1 else 0 end as vol_lob_vo_count,
    case when sum(case when (cease_reason_group = 'Customer Service Transaction') or (cease_reason_group is null)  then 1 else 0 end) > 0 then 1 else 0 end as cst_flag,
    case when sum(case when cease_reason_group = 'Involuntary' then 1 else 0 end) > 0 then 1 else 0 end as non_pay_so_flag
    from panel_so
    group by account_id
),

join_so_user as (
    select a.*, b.vol_lob_bb_count, b.vol_lob_tv_count,b.vol_lob_vo_count,
    case when a.net_inv_churn_flag <> 1 and b.vol_churn_flag = 1  and net_rgu_loss > 0 then 1 else 0 end as vol_churn_flag, 
    case when a.net_inv_churn_flag <> 1 and(b.vol_churn_flag = 0 or b.vol_churn_flag is null) and ((b.cst_flag = 1 and net_rgu_loss > 0) or (a.last_dt=(select end_date from parameters) and net_rgu_loss > 0)) then 1 else 0 end as cst_churn_flag, 
    case when b.non_pay_so_flag is null then 0 else b.non_pay_so_flag end as non_pay_so_flag,
--    case when b.vol_lob_bb_count>0 and start_bb>end_bb then 1 else 0 end +
---   case when b.vol_lob_tv_count>0 and start_tv>end_tv then 1 else 0 end +
---   case when b.vol_lob_vo_count>0 and start_vo>end_vo then 1 else 0 end as vol_churn_rgu,
    close_rgu_number - start_rgu_number  as delta_rgu_count,
    case when a.net_inv_churn_flag=0 and (b.vol_churn_flag = 0 or b.vol_churn_flag is null) and (b.cst_flag = 0 or b.cst_flag is null) and  a.last_dt<(select end_date from parameters )  and a.last_fi_outst_age<90  then 1 else 0 end as early_dx_flag
    --and (a.CRM = 'LIBERATE' OR (b.non_pay_so_flag = 1 AND a.CRM = 'CERILLION')) 
    from user_tbl a left join user_panel_so b
    on a.act_acct_cd = cast(b.account_id as varchar)
),

rgu_count as (
select *,
case when net_inv_churn_flag = 1 then start_rgu_number ELSE 0 end as net_inv_churn_rgu,
case when vol_churn_flag = 1 then net_rgu_loss else 0 end as vol_churn_rgu,
case when cst_churn_flag = 1 then net_rgu_loss else 0 end as cst_churn_rgu,
case when early_dx_flag = 1 then net_rgu_loss else 0 end as early_dx_churn_rgu

from join_so_user
),

summary_churn as (
  select TECHNOLOGY_PROXY, --Tenure,--pd_mix_cd,
  count(*) AS monthly_base,
  sum(vol_churn_flag) as vol_churners,
--  SUM(day_90_flag) AS churners,
  SUM(net_inv_churn_flag) AS net_inv_churners,
  SUM(cst_churn_flag) AS cst_churners,
  --and net_inv_churn_flag<>1 and start_rgu_number>close_rgu_number 
  --date_trunc('month', date '2022-04-01')
  SUM(early_dx_flag) as early_dx_churners,
  SUM(start_rgu_number) AS monthly_base_rgu,
  sum(case when vol_churn_flag=1 then vol_churn_rgu else 0 end) as vol_churners_rgu,
--  SUM(CASE WHEN day_90_flag = 1 THEN rgu_number ELSE 0 END) AS churners_rgu,
  SUM(CASE WHEN net_inv_churn_flag = 1 THEN net_inv_churn_rgu ELSE 0 END) AS net_inv_churners_rgu,  
--  SUM(case when cst_churn_flag=1 then net_rgu_loss else 0 end) AS cst_churners_rgu,
  sum(CASE WHEN cst_churn_flag = 1 THEN cst_churn_rgu ELSE 0 END)  as cst_churners_rgu,
  sum(CASE WHEN early_dx_flag = 1 THEN early_dx_churn_rgu ELSE 0 END)  as early_dx_churners_rgu,
  min(first_dt) as first_dt,
  max(last_dt) as last_dt
  from rgu_count
  group by TECHNOLOGY_PROXY--, Tenure
  --, pd_mix_cd 
  ORDER BY TECHNOLOGY_PROXY--, Tenure -- ,pd_mix_cd
)

--select * from panel_so  where account_id = 34127201
--select * from cst_churn where  act_acct_cd = '326150930000'--and cst_churn_flag = 1 and -- act_acct_cd = '292419780000' cst_flag = 1 and
--select * from "db-stage-dev"."so_hdr_cwc" where account_id = 326150930000
select * from summary_churn order by TECHNOLOGY_PROXY
--select * from rgu_count where early_dx_flag = 1 and TECHNOLOGY_PROXY = 'HFC' --act_acct_cd = '34127201' --early_dx_flag = 1
