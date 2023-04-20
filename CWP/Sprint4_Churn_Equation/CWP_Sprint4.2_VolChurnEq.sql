WITH FMC_Table AS
(select distinct month, fixedaccount,B_Final_TechFlag, B_FMCSegment, B_FMCType,E_Final_TechFlag, E_FMCSegment, E_FMCType,b_final_tenure,e_final_tenure,B_FixedTenure,E_FixedTenure,b_numrgus,e_numrgus,finalchurnflag,fixedchurntype,fixedchurnflag,fixedchurnsubtype,fixedmainmovement,waterfall_flag,finalaccount,f_activebom,f_activeeom,mobile_activeeom,mobilechurnflag
,case when b_bb is not null then 1 else 0 end as bb_bom
,case when b_tv is not null then 1 else 0 end as tv_bom
,case when b_vo is not null then 1 else 0 end as vo_bom
,case when e_bb is not null then 1 else 0 end as bb_eom
,case when e_tv is not null then 1 else 0 end as tv_eom
,case when e_vo is not null then 1 else 0 end as vo_eom
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev"
where 
    month=date(dt) 
    -- month=date('2023-03-01') 
    and f_activebom=1
)

,retention_prel as(
select distinct month_ret,account
,case when sum(day_vo)>0 then 1 else 0 end as churn_vo
,case when sum(day_bb)>0 then 1 else 0 end as churn_bb
,case when sum(day_tv)>0 then 1 else 0 end as churn_tv
,case when sum(prel_day_vo)>0 then 1 else 0 end as prev_vo
,case when sum(prel_day_bb)>0 then 1 else 0 end as prev_bb
,case when sum(prel_day_tv)>0 then 1 else 0 end as prev_tv
from(SELECT distinct date_trunc('month',date(date_parse(concat(rpad(year,4,'0'),lpad(cast(mes as varchar),2,'0'),lpad(cast("día" as varchar),2,'0')),'%Y%m%d'))) as month_ret,date(date_parse(concat(rpad(year,4,'0'),lpad(cast(mes as varchar),2,'0'),lpad(cast("día" as varchar),2,'0')),'%Y%m%d')) as date_ret,cast(account_number as varchar) as account
,case when (churn_fixed_voice_flag>0) then 1 else 0 end as day_vo
,case when (churn_broadband_flag>0) then 1 else 0 end as day_bb
,case when (churn_tv_flag>0) then 1 else 0 end as day_tv
,case when (cast(rgus_voz_antes as varchar)<>'0') then 1 else 0 end as prel_day_vo
,case when (cast(rgus_bb_antes as varchar)<>'0') then 1 else 0 end as prel_day_bb
,case when (cast(rgus_tv_antes as varchar)<>'0') then 1 else 0 end as prel_day_tv
FROM "lla_cco_int_san"."cwp_ext_reten_tatiana")
group by 1,2
)
,retention_base as(
select distinct month_ret,account,(prev_vo+prev_tv+prev_bb) as prev_rgus,(churn_vo+churn_tv+churn_bb) as churned_rgus,churn_vo,churn_bb,churn_tv
from retention_prel
)
,attempts as(
select f.*,prev_rgus,churned_rgus
,case when churn_vo=1 then 1 else 0 end as churn_vo
,case when churn_bb=1 then 1 else 0 end as churn_bb
,case when churn_tv=1 then 1 else 0 end as churn_tv
,case when r.account is not null then 1 else 0 end as RCOE
,case when (r.account is not null or fixedchurntype='1. Fixed Voluntary Churner') then fixedaccount else null end as Dx_Attempt
,case when (r.account is not null or fixedchurntype='1. Fixed Voluntary Churner') then b_numrgus else 0 end as Dx_Attempt_RGUs
,case when r.account is not null then fixedaccount else null end as Dx_Attempt_RCOE
,case when r.account is not null then b_numrgus else 0 end as Dx_Attempt_RCOE_RGU
from fmc_table f left join retention_base r on f.fixedaccount=r.account and f.month=r.month_ret
)
,disconnections as(
select f.*
,case when Dx_Attempt is not null and fixedchurntype is not null then fixedaccount else null end as All_Real_Dx
,case when Dx_Attempt_RCOE is not null and fixedchurntype is not null then fixedaccount else null end as RCOE_Real_Dx
--
,case when Dx_Attempt is not null and fixedchurntype is not null then b_numrgus 
      when Dx_Attempt is not null and fixedmainmovement='3.Downsell' then (COALESCE(B_NUMRGUS,0) - coalesce(E_numrgus,0)) 
else 0 end as All_Dx_rgus
,case when Dx_Attempt_RCOE is not null and fixedchurntype is not null then b_numrgus 
      when Dx_Attempt_RCOE is not null and fixedmainmovement='3.Downsell' then (COALESCE(B_NUMRGUS,0) - coalesce(E_numrgus,0)) 
else 0 end as All_Dx_rgus_RCOE
--
,case when RCOE=0 and fixedchurntype='1. Fixed Voluntary Churner' then fixedaccount else null end as Other_Vol_Dx
,case when RCOE=0 and fixedchurntype='1. Fixed Voluntary Churner' then b_numrgus else 0 end as Other_Vol_Dx_RGUs
--
,case when Dx_Attempt_RCOE is not null and fixedchurntype is null and churned_rgus>=b_numrgus then fixedaccount else null end as BajasNoCursadas
--,case when Dx_Attempt_RCOE is not null and fixedchurntype is null and fixedmainmovement<>'3.Downsell' and churned_rgus>=b_numrgus then churned_rgus else 0 end as BajasNoCursadas_RGUs
from attempts f
)
,retained as(
select f.*
,case when Dx_Attempt_RCOE is not null and fixedchurntype is null and churned_rgus<b_numrgus then fixedaccount else null end as Retained
,case when Dx_Attempt_RCOE is not null and fixedchurntype is null and fixedmainmovement<>'3.Downsell' then b_numrgus 
      when Dx_Attempt_RCOE is not null and fixedchurntype is null and fixedmainmovement='3.Downsell' then coalesce(e_numrgus,0)
else 0 end as Retained_RGUs
from disconnections f
)
,final_flags as(
select distinct *
,case when dx_attempt_RCOE is not null and retained is not null then '1. Retained'
      when dx_attempt is not null and retained is null then '2. Not Retained'
else null end as Ret_Flag_Users
,case when dx_attempt is not null and rcoe_real_dx is not null then '1. RCOE Dx'
      when dx_attempt is not null and bajasnocursadas is not null then '2. "Baja No Cursada"'
      when dx_attempt is not null and Other_Vol_Dx is not null then '3. Other Channel Dx'
else null end as Not_Retained_Flag_Users
,case when retained_rgus>0 then '1. Retained'
      when Dx_Attempt_RGUs>0 and retained_rgus=0 then '2. Not Retained'
else null end as Ret_Flag_RGUs
,case when All_Dx_rgus_RCOE>0 then '1. RCOE Dx'
      when Other_Vol_Dx_RGUs>0 then '2. Other Dx'
else null end as Not_Retained_Flag_RGUs
from retained
)

,FinalVolChurnTable as (select distinct month
,B_Final_TechFlag, B_FMCSegment, B_FMCType,E_Final_TechFlag, E_FMCSegment, E_FMCType,b_final_tenure,e_final_tenure,B_FixedTenure,E_FixedTenure,fixedchurntype
,rcoe
,b_numrgus,
-- e_numrgus
case when e_numrgus is null then 0 else e_numrgus end as e_numrgus
,Ret_Flag_Users,Not_Retained_Flag_Users
,Ret_Flag_RGUs,Not_Retained_Flag_RGUs
,bb_bom,tv_bom,vo_bom,bb_eom,tv_eom,vo_eom

,count(distinct dx_attempt) as all_attempts,count(distinct Dx_Attempt_RCOE) as rcoe_attempts,
count(distinct all_real_dx) as all_real_dx,count(distinct rcoe_real_dx) as rcoe_real_dx,
count(distinct Other_Vol_Dx) as Other_Vol_Dx,count(distinct BajasNoCursadas) as BajasNoCursadas,
count(distinct retained) as ret_users

from final_flags 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
)
-- select * from FinalVolChurnTable order by 1 desc 

SELECT
    sum(all_attempts) as all_attempts, 
    sum(rcoe_attempts) as rcoe_attempts, 
    sum(all_real_dx) as all_real_dx, 
    sum(rcoe_real_dx) as rcoe_real_dx, 
    sum(Other_Vol_Dx) as other_vol_dx, 
    sum(BajasNoCursadas) as Bajas_No_Cursadas, 
    sum(ret_users) as retained_users
FROM FinalVolChurnTable
