WITH 
parameters as (
select
    --##############################################################
    --### Change Date in this line to define paying period #########
    date('2023-03-01') as start_date,
    date_trunc('Month',date('2023-03-01'))  -- <--This date should be the same as in the previous line
    + interval '1' MONTH - interval '1' day as end_date
    --By default we will have last day of month as end_date, in case there is any data quality issue that date, uncomment the next line and modify it
    --date('2022-12-02') as end_date,
   )

,FMC_Table AS(
SELECT month,B_FMCSegment,B_FMCType,B_Final_TechFlag,E_FMCSegment,E_FMCType,E_Final_TechFlag,fixedchurnflag,fixedchurntype,fixedchurnsubtype,fixedmainmovement,waterfall_flag,fixedaccount,b_bb,b_tv,b_vo,e_bb,e_tv,e_vo,b_fixedtenure,e_fixedtenure
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev"
where month=date(dt) and date(dt) = (select start_date from parameters) --limit 20
)
-----------Involuntary KPIs Key Fields--------------------------
,Invol_Funnel_Fields AS(
select  *,first_value(LagDueDay_feb) over(partition by act_acct_cd,DATE(DATE_TRUNC('MONTH',dt)) order by date(dt) desc) as LastDueDay_feb
from(SELECT DISTINCT DATE(DATE_TRUNC('MONTH',date(d.dt))) AS Month,date(d.dt) AS dt,DATE(DATE_TRUNC('MONTH',fi_bill_dt_m0)) AS BillMonth,date(fi_bill_dt_m0) as BillDay,d.act_acct_cd,d.fi_outst_age AS DueDays
,CASE WHEN ACT_BLNG_CYCL IN('A','B','C') THEN 15 ELSE 28 END AS FirstOverdueDay
,case when DATE(DATE_TRUNC('MONTH',date(d.dt)))=date('2022-03-01') then date('2022-03-02') else DATE(DATE_TRUNC('MONTH',date(d.dt))) end as Backlog_Date
,first_value(fi_outst_age) over(partition by act_acct_cd,DATE(DATE_TRUNC('MONTH',date(d.dt))) order by date(dt) desc) as LastDueDay,oldest_unpaid_bill_dt
,lag(fi_outst_age) over(partition by act_acct_cd order by date(dt) asc) as LagDueDay_feb
FROM "db-analytics-prod"."fixed_cwp" d
WHERE act_cust_typ_nm = 'Residencial' and date(dt) between (select start_date from parameters ) and (select end_date from parameters )
))
-------------Cohort Approach-----------------------------------
,Cohort_FirstDayOverdue AS(
SELECT DISTINCT f.*
,CASE WHEN DueDays=FirstOverdueDay THEN act_acct_cd ELSE null END AS Overdue1Day
FROM FMC_Table f LEFT JOIN Invol_Funnel_Fields a ON f.fixedaccount=a.act_acct_cd AND f.month=a.month
)
,Cohort_SoftDx AS(
SELECT DISTINCT f.*
,CASE WHEN DueDays=46 THEN act_acct_cd ELSE null END AS SoftDx
FROM Cohort_FirstDayOverdue f LEFT JOIN Invol_Funnel_Fields b ON f.fixedaccount=b.act_acct_cd AND f.month=b.month
)
,Cohort_Backlog AS(
SELECT DISTINCT f.*
,CASE WHEN DueDays BETWEEN (90-(date_diff('day',date_trunc('Month', date(c.dt)),(select end_date from parameters)
--Previously we had for default last day of month, change to end_date parameters in case there is any data quality issue with that day
/*date_trunc('Month',date(c.dt)) + interval '1' MONTH - interval '1' day*/))) AND 90 
/*OR   
date_diff('day',date(date_parse(substring(cast(oldest_unpaid_bill_dt as varchar),1,8), '%Y%m%d')),date(c.dt)) 
 between (90-(date_diff('day',date_trunc('Month', date(c.dt)),(select end_date from parameters) /*date_trunc('Month',date(c.dt)) + interval '1' MONTH - interval '1' day))) AND 90 */
 THEN act_acct_cd ELSE null 
END AS Backlog
FROM Cohort_SoftDx f LEFT JOIN Invol_Funnel_Fields c ON f.fixedaccount=c.act_acct_cd AND f.month=c.month
WHERE date(c.dt)=c.backlog_date
)
,Cohort_HardDx AS(
SELECT DISTINCT f.*
,CASE WHEN f.month>date('2022-02-01') and DueDays>=90 and lastdueday>=90 THEN backlog 
      WHEN f.month=date('2022-02-01') and DueDays>=90 and lastdueday_feb>=90 THEN backlog 
    

ELSE null END AS HardDx
FROM Cohort_Backlog f LEFT JOIN Invol_Funnel_Fields d ON f.fixedaccount=d.act_acct_cd AND f.month=d.month
)
-----------------------RGUS------------------------------------------
--BB
,Cohort_All_BB AS(
SELECT DISTINCT f.*
,CASE WHEN Overdue1day IS NOT NULL AND E_BB IS NOT NULL THEN overdue1day ELSE null END AS Overdue1Day_BB
,CASE WHEN SoftDx IS NOT NULL AND E_BB IS NOT NULL THEN SoftDx ELSE null END AS SoftDx_BB
,CASE WHEN Backlog IS NOT NULL AND B_BB IS NOT NULL THEN Backlog ELSE null END AS Backlog_BB
,CASE WHEN HardDx IS NOT NULL AND B_BB IS NOT NULL THEN HardDx ELSE null END AS HardDx_BB
FROM Cohort_HardDx f 
)
,Cohort_All_TV AS(
SELECT DISTINCT f.*
,CASE WHEN Overdue1day IS NOT NULL AND E_TV IS NOT NULL THEN overdue1day ELSE null END AS Overdue1Day_TV
,CASE WHEN SoftDx IS NOT NULL AND E_TV IS NOT NULL THEN SoftDx ELSE null END AS SoftDx_TV
,CASE WHEN Backlog IS NOT NULL AND B_TV IS NOT NULL THEN Backlog ELSE null END AS Backlog_TV
,CASE WHEN HardDx IS NOT NULL AND B_TV IS NOT NULL THEN HardDx ELSE null END AS HardDx_TV
FROM Cohort_All_BB f 
)
,Cohort_All_VO AS(
SELECT DISTINCT f.*
,CASE WHEN Overdue1day IS NOT NULL AND E_VO IS NOT NULL THEN overdue1day ELSE null END AS Overdue1Day_VO
,CASE WHEN SoftDx IS NOT NULL AND E_VO IS NOT NULL THEN SoftDx ELSE null END AS SoftDx_VO
,CASE WHEN Backlog IS NOT NULL AND B_VO IS NOT NULL THEN Backlog ELSE null END AS Backlog_VO
,CASE WHEN HardDx IS NOT NULL AND B_VO IS NOT NULL THEN HardDx ELSE null END AS HardDx_VO
FROM Cohort_All_TV f 
)
,Cohort_Flag AS(
SELECT DISTINCT *
FROM Cohort_All_VO
)
--/*

, final_table as (
SELECT DISTINCT --month,count(distinct BACKLOG)
month as che_s_dim_month
,B_FMCSegment as che_b_fla_che_segment,B_FMCType as che_b_fla_che_type ,B_Final_TechFlag as che_b_fla_final_tech,b_fixedtenure as che_b_fla_final_tenure,
E_FMCSegment as che_e_fla_che_segment,E_FMCType as che_e_fla_che_type,E_Final_TechFlag as che_e_fla_final_tech,e_fixedtenure as che_e_fla_final_tenure,
fixedchurnflag as che_s_fla_churn,fixedchurntype as che_s_fla_churn_type/*fixedchurnsubtype as che_s_fla_churn_subtype*/ ,fixedmainmovement as che_s_dim_main_movement,waterfall_flag as che_s_fla_waterfall

,count(distinct fixedaccount) as che_s_mes_active_base
,count(distinct e_bb) as che_s_mes_total_bb
,count(distinct e_tv) as che_s_mes_total_tv
,count(distinct e_vo) as che_s_mes_total_vo
,count(distinct Overdue1Day) as che_s_mes_day1, count(distinct SoftDx) as che_s_mes_softdx,count(distinct backlog) AS che_s_mes_backlog
,count(distinct harddx) as che_s_mes_harddx 


,count(distinct Overdue1Day_BB) as che_s_mes_overdue1day_bb, count(distinct SoftDx_BB) as che_s_mes_softdx_bb
,count(distinct backlog_BB) AS che_s_mes_backlog_bb,count(distinct harddx_BB) as che_s_mes_harddx_bb, count(distinct Overdue1Day_TV) as che_s_mes_overdue1day_tv, count(distinct SoftDx_TV) as che_s_mes_softdx_tv
,count(distinct backlog_TV) AS che_s_mes_backlog_tv,count(distinct harddx_TV) as che_s_mes_harddx_tv, count(distinct Overdue1Day_VO) as che_s_mes_overdue1day_vo, count(distinct SoftDx_VO) as che_s_mes_softdx_vo
,count(distinct backlog_VO) AS che_s_mes_backlog_vo,count(distinct harddx_VO) as che_s_mes_harddx_vo
FROM Cohort_Flag
--WHERE Month=date('2022-02-01') and harddx IS NULL AND backlog IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
--order by users
order by 1
)

SELECT 
    sum(che_s_mes_active_base) as active_base, 
    sum(che_s_mes_day1) as day1, 
    sum(che_s_mes_softdx) as soft_dx, 
    sum(che_s_mes_backlog) as backlog, 
    sum(che_s_mes_harddx) as hard_dx
FROM final_table
