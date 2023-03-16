--- ########## CWC - SPRINT 4 (1) - INVOLUNTARY CHURN EQUATION - PAULA MORENO (GITLAB) ##########

WITH

--- Modify the following dates accoridng to your input date request
parameters as (
SELECT
date('2022-10-01') as start_date, 
date_trunc('month', date('2022-10-01')) + interval '1'  month - interval '1' day as end_date
)

, FMCTABLE as (
SELECT 
    B_FMC_Segment, 
    B_FMCType, 
    B_Final_Tech_Flag, 
    E_FMC_Segment, 
    E_FMCType, 
    E_Final_Tech_Flag, 
    B_FinalTenureSegment, 
    E_FinalTenureSegment, 
    fixedchurnflag, 
    fixedchurntypeflag, 
    mainmovement, 
    waterfall_flag, 
    fixed_account, 
    month, 
    dt, 
    BB_RGU_EOM, 
    TV_RGU_EOM, 
    VO_RGU_EOM, 
    bb_RGU_bOM, 
    tv_RGU_bOM, 
    vo_RGU_bOM
FROM "lla_cco_int_ana_dev"."cwc_fmc_churn_dev"
WHERE
    month = date(dt) and date(dt) = (SELECT start_date FROM parameters)
)
    
--- ### ### ### INVOLUNTARY KPIs KEY FIELDS ### ### ###

, INVOL_FUNNEL_FIELDS as (
SELECT
    date_trunc('month', date(dt)) as Month, 
    dt, 
    act_acct_cd, 
    fi_outst_age, 
    fi_bill_dt_m0, 
    fi_bill_due_dt_m0, 
    case when length(oldest_unpaid_bill_dt) < 8 then null else date(concat(substr(oldest_unpaid_bill_dt, 1, 4), '-', substring(oldest_unpaid_bill_dt, 5, 2), '-', substr(oldest_unpaid_bill_dt, 7, 2))) end as oldest_unpaid_bill_dt, 
    case when length(oldest_unpaid_due_dt) < 8 then null else date(concat(substr(oldest_unpaid_due_dt, 1, 4), '-', substr(oldest_unpaid_due_dt, 5, 2), '-', substr(oldest_unpaid_bill_dt, 7, 2))) end as oldest_unpaid_due_dt, 
    first_value(dt) over (partition by act_acct_cd, date_trunc('Month', date(dt)) order by dt desc) as dt_max, 
    first_value(fi_outst_age) over (partition by act_acct_cd, date_trunc('month', date(dt)) order by dt) as first_outst_age, 
    first_value(fi_outst_age) over (partition by act_acct_cd, date_trunc('month', date(dt)) order by dt desc) as last_outst_age
FROM "db-analytics-prod"."tbl_fixed_cwc"
WHERE 
    org_cntry = 'Jamaica' 
    and ACT_CUST_TYP_NM in ('Browse & Talk HFONE', 'Residence', 'Standard')
    and ACT_ACCT_STAT in ('B', 'D', 'P', 'SN', 'SR', 'T', 'W') 
    and date(dt) between (SELECT start_date FROM parameters) and (SELECT end_date FROM parameters)
)


--- ### ### ### KPIs CALCULATION ### ### ###

--- ### ### ### BILLING

--- Revise funnel approach - Is it right?

, BILLINGACCOUNTS as (
SELECT
    distinct date_trunc('Month', date(fi_bill_dt_m0)) as BillingMonth, 
    date(fi_bill_dt_m0) as BillDate, 
    act_acct_cd as BillCustomers
FROM INVOL_FUNNEL_FIELDS
WHERE 
    fi_bill_dt_m0 is not null
    and (fi_outst_age is null or cast(fi_outst_age as double) <= 90)
)

, BILLING_FLAG as (
SELECT
    f.*, 
    BillingMonth, 
    case when BillCustomers is not null then BillCustomers else null end as Bill_Flag
FROM FMCTable f 
LEFT JOIN BillingAccounts b 
    ON f.fixed_account = b.BillCustomers and f.Month = b.BillingMonth
)


--- ### ### ### DAY 1

, DAYONE_CUSTOMERS as (
SELECT
    distinct date_trunc('month', oldest_unpaid_bill_dt) as OverdueMonth, 
    date_trunc('Month', date(dt)) as day1_month, act_acct_cd as day1_customers
FROM Invol_Funnel_Fields
WHERE
    (length(act_acct_cd) = 8 and cast(fi_outst_age as double) = 31)
    or (length(act_acct_cd) = 12 and cast(fi_outst_age as double) = 21)
)

, DAYONE_FLAG as (
SELECT
    f.*, 
    OverdueMonth as Day1_OvMonth, 
    case when day1_customers is not null then day1_customers else null end as Day1_Flag, 
    case when day1_customers is not null then 1 else 0 end as Day1_Category
FROM Billing_Flag f
LEFT JOIN DayOne_Customers d
    ON f.fixed_account = d.day1_customers and f.Month = d.day1_month
)


--- ### ### ### Soft Dx

, SOFT_DX_CUSTOMERS as (
SELECT
    date_trunc('month', oldest_unpaid_bill_dt) as Overduemonth, 
    date_trunc('month', date(dt)) as softdx_month, 
    act_acct_cd as soft_dx, 
    *
FROM INVOL_FUNNEL_FIELDS
)

, SOFT_DX_FLAG as (
SELECT
    f.*, 
    OverdueMonth as SoftDx_OvMonth, 
    case when soft_dx is not null then soft_dx else null end as SoftD_flag,
    case when soft_dx is not null then 1 else 0 end as Soft_D_Category
FROM DayOne_Flag f
LEFT JOIN soft_dx_customers s 
    ON f.fixed_account = s.soft_dx and f.month = s.softdx_month
)


--- ### ### ### Backlog subject to Dx

, BACKLOG_SUBJECT_TO_DX as (
SELECT
    distinct date_trunc('month', oldest_unpaid_bill_dt) as Overduemonth, 
    date_trunc('Month', date(dt)) as backlog_month, 
    act_acct_cd as backlog_subjectdx
FROM INVOL_FUNNEL_FIELDS
WHERE
    oldest_unpaid_bill_dt is not null
    and date(dt) = date_trunc('Month', date(dt))
    and cast(fi_outst_age as int) between (90-date_diff('day', date_trunc('Month', date (dt)), date_trunc('Month', date(dt)) + interval '1' Month - interval '1' day)) and 89
)

, BACKLOG_DX_FLAG as (
SELECT
    f.*, 
    OverdueMonth as OvbacklogMonth,
    case when backlog_subjectdx is not null then backlog_subjectdx else null end as backlog_flag,
    case when backlog_subjectdx is not null then 1 else 0 end as Backlog_category
FROM soft_dx_flag f
LEFT JOIN backlog_subject_to_dx b 
    ON f.fixed_account = b.backlog_subjectdx and f.month = b.backlog_month
)


--- ### ### ### Hard Dx

, INVOL_FUNNEL_FIELDS_hdx as (
SELECT 
    oldest_unpaid_bill_dt, 
    dt, 
    act_acct_cd, 
    first_outst_age, 
    last_outst_age, 
    dt_max, 
    try(filter(array_agg(cast(fi_outst_age as int) order by dt desc), x->x != -1)[1]) as last_outst_age_v2
FROM Invol_Funnel_Fields
GROUP BY 1, 2, 3, 4, 5, 6
)

, HARD_DX_CUSTOMERS as (
SELECT 
    distinct date_trunc('month', oldest_unpaid_bill_dt) as OverdueMonth, 
    date_trunc('month', date (dt)) as harddx_month, 
    act_acct_cd as hard_dx
FROM INVOL_FUNNEL_FIELDS_hdx
WHERE 
    oldest_unpaid_bill_dt is not null
    and (cast(first_outst_age as double) <= 90 and cast(last_outst_age as double) >= 90 )
    or (cast(first_outst_age as double) <= 90 and cast(last_outst_age_v2 as int) >= 90 and date(dt_max) < (SELECT end_date FROM parameters))
)

, Hard_Dx_Flag as (
SELECT
    f.*, 
    OverdueMonth as OvHardDxMonth, 
    case when hard_dx is not null then hard_dx else null end as HardD_Flag, 
    case when hard_dx is not null then 1 else 0 end as HardD_category
FROM Backlog_Dx_Flag f
LEFT JOIN Hard_Dx_Customers h
    ON f.fixed_account = h.hard_dx and f.month = h.harddx_month
)

--- ### ### ### RGUs

, Cohort_All_BB as (
SELECT
    distinct f.*, 
    case when Day1_Flag is not null and BB_RGU_EOM is not null then Day1_Flag else null end as Overdue1Day_BB,
    case when SoftD_flag is not null and BB_RGU_EOM is not null then SoftD_flag else null end as SoftDx_BB, 
    case when backlog_flag is not null and BB_RGU_bOM is not null then backlog_flag else null end as Backlog_BB, 
    case when HardD_Flag is not null and BB_RGU_bOM is not null then HardD_Flag else null end as HardDx_BB
FROM Hard_Dx_Flag f
)

, Cohort_All_TV as (
SELECT
    distinct f.*, 
    case when Day1_Flag is not null and TV_RGU_EOM is not null then Day1_Flag else null end as Overdue1Day_TV,
    case when SoftD_flag is not null and TV_RGU_EOM is not null then SoftD_flag else null end as SoftDx_TV, 
    case when backlog_flag is not null and TV_RGU_bOM is not null then backlog_flag else null end as Backlog_TV, 
    case when HardD_Flag is not null and TV_RGU_bOM is not null then HardD_Flag else null end as HardDx_TV
FROM Cohort_All_BB f
)

, Cohort_All_VO as (
SELECT
    distinct f.*, 
    case when Day1_Flag is not null and vo_RGU_EOM is not null then Day1_Flag else null end as Overdue1Day_VO,
    case when SoftD_flag is not null and vo_RGU_EOM is not null then SoftD_flag else null end as SoftDx_VO, 
    case when backlog_flag is not null and vo_RGU_bOM is not null then backlog_flag else null end as Backlog_VO, 
    case when HardD_Flag is not null and vo_RGU_bOM is not null then HardD_Flag else null end as HardDx_VO
FROM Cohort_All_TV f
)

, Cohort_Flag as (
SELECT distinct * FROM Cohort_All_VO
)


--- ### ### ### Final results

, results as (
SELECT
    Month as che_s_dim_month, 
    B_FMC_Segment as che_b_fla_che_segment,
    B_FMCType as che_b_fla_che_type, 
    B_Final_Tech_Flag as che_b_fla_final_tech, 
    B_FinalTenureSegment as che_b_fla_final_tenure, 
    E_FMC_Segment as che_e_fla_che_segment, 
    E_FMCType as che_e_fla_che_type, 
    E_Final_Tech_Flag as che_e_fla_final_tech, 
    E_FinalTenureSegment as che_e_fla_final_tenure, 
    fixedchurnflag as che_s_fla_churn, 
    fixedchurntypeflag as che_s_fla_churn_type, 
    mainmovement as che_s_dim_main_movement, 
    waterfall_flag as che_s_fla_waterfall, 
    count(distinct fixed_account) as che_s_mes_active_base, 
    count(distinct BB_RGU_EOM) as che_s_mes_total_bb, 
    count(distinct TV_RGU_EOM) as che_s_mes_total_tv, 
    count(distinct VO_RGU_EOM) as che_s_mes_total_vo, 
    count(distinct day1_flag) as che_s_mes_day1, 
    count(distinct softd_flag) as che_s_mes_soft_dx, 
    count(distinct backlog_flag) as che_s_mes_backlog, 
    count(distinct hardd_flag) as che_s_mes_harddx, 
    count(distinct Overdue1Day_BB) as che_s_mes_overdue1day_bb, 
    count(distinct SoftDx_BB) as che_s_mes_softdx_bb, 
    count(distinct backlog_BB) as che_s_mes_backlog_bb, 
    count(distinct harddx_BB) as che_s_mes_harddx_bb, 
    count(distinct Overdue1Day_TV) as che_s_mes_overdue1day_tv, 
    count(distinct SoftDx_TV) as che_s_mes_softdx_tv, 
    count(distinct backlog_TV) AS che_s_mes_backlog_tv, 
    count(distinct harddx_TV) as che_s_mes_harddx_tv, 
    count(distinct Overdue1Day_VO) as che_s_mes_overdue1day_vo, 
    count(distinct SoftDx_VO) as che_s_mes_softdx_vo, 
    count(distinct backlog_VO) AS che_s_mes_backlog_vo,
    count(distinct harddx_VO) as che_s_mes_harddx_vo
FROM Cohort_Flag
WHERE Month = date(dt)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
)

SELECT * FROM results
