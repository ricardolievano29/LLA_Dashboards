--- ########## CWC - MOBILE FLAGS - PAULA MORENO (GITLAB) ##########

WITH

MobileFields as (
SELECT
    date_trunc('month', date(dt)) as Month, 
    account_id, 
    dt, 
    phone_no, 
    case 
        when IS_NAN (cast(total_mrc_mo as double)) then 0
        when not IS_NAN (cast(total_mrc_mo as double)) then round(cast(total_mrc_mo as double), 0)
    end as total_mrc_mo,
    date_diff('day', date(first_value(account_creation_date) over (partition by account_id order by dt desc)), date(first_value(dt) over (partition by account_id order by dt desc))) as MaxTenureDays, 
    first_value(account_creation_date) over (partition by account_id order by dt desc) as Mobile_MaxStart, 
    cast(concat(substr(oldest_unpaid_bill_dt, 1, 4), '-', substr(oldest_unpaid_bill_dt, 5, 2), '-', substr(oldest_unpaid_bill_dt, 7, 2)) as date) as oldes_unpaid_bill_dt_adj, 
    date_diff('day', cast(concat(substr(oldest_unpaid_bill_dt, 1, 4), '-', substr(oldest_unpaid_bill_dt, 5, 2), '-', substr(oldest_unpaid_bill_dt, 7, 2)) as date), cast(dt as date)) as Fi_outst_age
FROM "db-analytics-prod"."tbl_postpaid_cwc"
WHERE 
    org_id = '338'
    and account_type = 'Residential'
    and account_status not in ('Ceased', 'Closed',  'Recommended for cease')
    and date(dt) between (date('2022-09-01') + interval '1' month - interval '1' day - interval '2' month) and (date('2022-09-01') + interval '1' month - interval '1' day)
)

, MobileRGUsPerUser as ( 
SELECT 
    distinct date_trunc('month', date(dt)) as Month, 
    dt, 
    account_id, 
    count(distinct phone_no) as MobileRGUs
FROM MobileFields
GROUP BY Month, dt, account_id
)

, AverageMRC_Mobile as (
SELECT distinct 
    date_trunc('month', date(dt)) as Month, 
    account_id, 
    phone_no,  
    round(avg(total_mrc_mo), 0) as AvgMRC_Mobile
FROM MobileFields
WHERE total_mrc_mo is not null and total_mrc_mo != 0 and not is_nan(total_mrc_mo)
GROUP BY 1, account_id, phone_no
)

, MobileUsersBOM as (
SELECT
    distinct date_trunc('month', date_add('month', 1, date(m.dt))) as Mobile_Month, --- Why we add a month???
    m.account_id as mobileBOM, 
    m.dt as Mobile_B_Date, 
    total_mrc_mo as Mobile_MRC_BOM, 
    m.phone_no as B_Phone, 
    MaxTenureDays as Mobile_B_TenureDays, 
    Mobile_MaxStart as B_Mobile_MaxStart, 
    AvgMRC_Mobile as B_AvgMRC_Mobile, 
    MobileRGUs as B_MobileRGUs, 
    Fi_outst_age as B_MobileOutstAge
FROM MobileFields m
INNER JOIN MobileRGUsPerUser r
    ON m.account_id = r.account_id and m.dt = r.dt
LEFT JOIN AverageMRC_Mobile a
    ON m.account_id = a.account_id and m.month = a.Month and m.Phone_no = a.Phone_no
WHERE 
    date(m.dt) = date_trunc('month', date(m.dt)) + interval '1' month - interval '1' day
    and (fi_outst_age <= 90 or fi_outst_age is null)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)

, MobileUsersEOM as (
SELECT
    distinct date_trunc('month', date(m.dt)) as mobile_month, 
    m.account_id as mobileEOM, 
    m.dt as Mobile_E_Date, 
    total_mrc_mo as Mobile_MRC_EOM, 
    m.phone_no as E_Phone, 
    MaxTenureDays as Mobile_E_TenureDays, 
    Mobile_MaxStart as E_Mobile_MaxStart, 
    AvgMRC_Mobile as E_AvgMRC_Mobile, 
    MobileRGUs as E_MobileRGUs, 
    Fi_outst_age as E_MobileOutsAge
FROM MobileFields m
INNER JOIN MobileRGUsPerUser r 
    ON m.account_id = r.account_id and m.dt = r.dt
LEFT JOIN AverageMRC_Mobile a 
    ON m.account_id = a.account_id and m.Month = a.Month and m.Phone_no = a.Phone_no
WHERE 
    date(m.dt) = date_trunc('month', date(m.dt)) + interval '1' month - interval '1' day
    and (fi_outst_age <= 90 or fi_outst_age is null)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)

, MobileCustomerStatus as (
SELECT DISTINCT 
    case
        when (mobileBOM is not null and mobileEOM is not null) or (mobileBOM is not null and mobileEOM is null) then b.Mobile_Month
        when (mobileBOM is null and mobileEOM is not null) then e.Mobile_Month
    end as Mobile_Month, 
    case 
        when (mobileBOM is not null and mobileEOM is not null) or (mobileBOM is not null and mobileEOM is null) then mobileBOM
        when (mobileBOM is null and mobileEOM is not null) then mobileEOM
    end as Mobile_Account, 
    case 
        when (mobileBOM is not null and mobileEOM is not null) or (mobileBOM is not null and mobileEOM is null) then B_Phone
        when (mobileBOM is null and mobileEOM is not null) then E_Phone
    end as Mobile_Phone, 
    case 
        when (mobileBOM is not null and mobileEOM is not null) or (mobileBOM is not null and mobileEOM is null) then Mobile_B_TenureDays
        when (mobileBOM is null and mobileEOM is not null) then Mobile_E_TenureDays
    end as TenureDays, 
    case when mobileBOM is not null then 1 else 0 end as Mobile_ActiveBOM, 
    case when mobileEOM is not null then 1 else 0 end as Mobile_ActiveEOM,
    Mobile_B_Date, 
    Mobile_B_TenureDays, 
    B_Mobile_MaxStart, 
    case 
        when Mobile_B_TenureDays <= 180 then 'Early-Tenure'
        when Mobile_B_TenureDays > 180 and Mobile_B_TenureDays <= 360 then 'Mid-Tenure'
        when Mobile_B_TenureDays > 360 then 'Late-Tenure' 
    end as B_MobileTenureSegment, 
    Mobile_MRC_BOM, 
    B_AvgMRC_Mobile, 
    B_MobileRGUs, 
    B_MobileOutstage, 
    case 
        when B_MobileRGUs = 1 then 'Single-line'
        when B_MobileRGus > 1 then 'Multiple-lines'
    end as B_MobileCustomerType, 
    Mobile_E_Date, 
    Mobile_E_TenureDays, 
    E_Mobile_MaxStart, 
    case 
        when Mobile_E_TenureDays <= 180 then 'Early-Tenure'
        when Mobile_E_TenureDays > 180 and Mobile_B_TenureDays <= 360 then 'Mid-Tenure'
        when Mobile_E_TenureDays > 360 then 'Late-Tenure' 
    end as E_MobileTenureSegment,
    case when (Mobile_MRC_EOM = 0 or Mobile_MRC_EOM is null) then mobile_mrc_bom else Mobile_MRC_EOM end AS Mobile_MRC_EOM,
    E_AvgMRC_Mobile, 
    E_MobileRGUs, 
    E_MobileOutsAge
FROM MobileUsersBOM b
FULL OUTER JOIN MobileUsersEOM e
    ON b.mobileBOM = e.mobileEOM and b.Mobile_Month = e.Mobile_Month and b.B_Phone = e.E_Phone
) 

, MobileMovementClass as (
SELECT
    distinct *, 
    case
        when Mobile_ActiveBOM = 1 and Mobile_ActiveEOM = 1 then '1.Maintain'
        when Mobile_ActiveBOM = 1 and Mobile_ActiveEOM = 0 then '2. Loss'
        when Mobile_ActiveBOM = 0 and Mobile_ActiveEOM = 1 and date_trunc('month', date(E_Mobile_MaxStart)) = date('2022-09-01') then '3. New Customers' --- Here we can change the input date
        when Mobile_ActiveBOM = 0 and Mobile_ActiveEOM = 1 and date_trunc('month', date(E_Mobile_MaxStart)) != date('2022-09-01') then '4. Come Back to Life'
        else '5. Null' 
    end as MobileMovementFlag, 
    case 
        when Mobile_ActiveBOM = 1 and Mobile_ActiveEOM = 1 and B_MobileRGUs > E_MobileRGUs then 'Downsell'
        when Mobile_ActiveBOM = 1 and Mobile_ActiveEOM = 1 and B_MobileRGUs < E_MobileRGUs then 'Upsell'
        when Mobile_ActiveBOM = 1 and Mobile_ActiveEOM = 1 and B_MobileRGUs = E_MobileRGUs then 'No Change'
        else null
    end as Mobile_SecondaryMovementFlag
FROM MobileCustomerStatus
)    

, SpinClass as (
SELECT
    distinct *, 
    (Mobile_MRC_EOM - Mobile_MRC_BOM) as Mobile_MRC_Diff, 
    case
        when MobileMovementFlag = '1. Maintain' and (Mobile_MRC_EOM - Mobile_MRC_BOM) = 0 then '1. NoSpin'
        when MobileMovementFlag = '1. Maintain' and (Mobile_MRC_EOM - Mobile_MRC_BOM) > 75 then '2. Upspin'
        when MobileMovementFlag = '1. Maintain' and (Mobile_MRC_EOM - Mobile_MRC_BOM) < -75 then '3. Downspin'
        else '1. NoSpin' end as SpinFlag
    FROM MobileMovementClass
)

, MobileBase_ChurnFlag as (
SELECT distinct * FROM SpinClass
)

--- ### ### ### Churners ### ### ###

--- ### Voluntary Churners ###

, mobile_so as (
SELECT 
    *, 
    date_trunc('month', date(completed_date)) as ChurnMonth
FROM "db-stage-dev"."so_hdr_cwc"
WHERE
    org_cntry = 'Jamaica'
    and cease_reason_group in ('Voluntary', 'Customer Service Transaction', 'Involuntary')
    and network_type in ('LTE', 'MOBILE')
    and order_status = 'COMPLETED'
    and account_type = 'Residential'
    and order_type = 'DEACTIVATION'
    and date_trunc('month', date (completed_date)) = date('2022-09-01') --- Change the input month if needed
)

, Voluntary_Churn as (
SELECT 
distinct ChurnMonth, 
account_id
FROM mobile_so
WHERE cease_reason_group = 'Voluntary'
)

, VoluntaryChurners as (
SELECT
    distinct ChurnMonth as Month, 
    account_id as account, 
    '1. Mobile Voluntary Churner' as ChurnType
FROM Voluntary_Churn
GROUP BY 1, 2, 3
)

--- ### Involuntary Churners ####

, Customers_FirstLast_Record as (
SELECT
    distinct Month as Month, 
    account_id as Account, 
    Min(dt) as FirstCustRecord, 
    Max(dt) as LastCustRecord
FROM MobileFields
GROUP BY 1, 2
)

, No_Overdue as (
SELECT
    distinct r.Month as Month, 
    Account_id as Account, 
    fi_outst_age
FROM MobileFields t
INNER JOIN  Customers_FirstLast_Record r
    ON t.dt = r.FirstCustRecord and r.account = t.account_id
WHERE cast(fi_outst_age as double) <= 90
GROUP BY 1, 2, fi_outst_age
)

, OverdueLastDay as (
SELECT
    distinct r.Month as Month, 
    account_id as Account, 
    fi_outst_age, 
    (date_diff('day', date(Mobile_MaxStart), date(dt))) as ChurnTenureDays
FROM MobileFields t
INNER JOIN Customers_FirstLast_Record r
    ON t.dt = r.LastCustRecord and r.account = t.account_id
WHERE cast(fi_outst_age as double) >= 90
GROUP BY 1, 2, fi_outst_age, 4
)

, InvoluntaryNetChurners as (
SELECT
    distinct n.Month as Month, 
    n.account, 
    l.ChurnTenureDays, 
    l.fi_outst_age
FROM No_Overdue n
INNER JOIN OverdueLastDay l
    ON n.account = l.account and n.Month = l.Month
)

, InvoluntaryChurners as (
SELECT
    distinct Month, 
    cast(Account as varchar) as Account, 
    case when Account is  null then '2. Mobile Involuntary Churners' end as ChurnType
FROM InvoluntaryNetChurners
GROUP BY 1, 2, 3
)

--- ### All Mobile Churners ###

, AllMobileChurners as (
SELECT
    distinct Month, 
    Account, 
    Churntype
FROM (
    SELECT Month, cast(Account as double) as account, ChurnType FROM VoluntaryChurners a
    UNION ALL
    SELECT Month, cast(Account as double) as account, ChurnType FROM InvoluntaryChurners b
    )
)

, MobileBase_AllFlags as (
SELECT
    distinct m.*, 
    case when c.account is not null then '1. Mobile Churner' else '2. Mobile NonChurner' end as MobileChurnFlag, 
    case when c.account is not null then ChurnType else '2. Mobile NonChurner' end as MobileChurnType, 
    case
        when m.TenureDays <= 180 then 'Early-life'
        when m.TenureDays > 180 and m.TenureDays <= 360 then 'Mid-life'
        when m.TenureDays > 360 then 'Late-life'
    end as MobileChurnTenureSegment
FROM MobileBase_ChurnFlag m
LEFT JOIN AllMobileChurners c
    ON cast(m.mobile_account as double) = cast(c.account as double) and Mobile_Month = Month
)

--- ### ### ### Early Dx Flag ### ### ###

, join_so_mobilebase as (
SELECT 
    a.*, 
    case 
        when a.MobileChurnType = '1. Mobile Voluntary Churner' then 'Voluntary'
        when a.MobileChurnType = '2. Mobile Involuntary Churner' then 'Involuntary'
        when a.MobileChurnType = '2. Mobile NonChurner' and Mobile_ActiveEOM = 0 and cast(a.B_MobileOutstAge as integer) > 90 and ((length(a.mobile_account) = 12) or (b.cease_reason_group = 'Involuntary' and length(a.mobile_account) = 8)) then 'Early Dx'
    end as FinalMobileChurnFlag
FROM MobileBase_AllFlags a 
LEFT JOIN mobile_so b
    ON cast(a.mobile_account as varchar) = cast(b.account_id as varchar)
)

--- ### ### ### Rejoiners ### ### ###

, InactiveUsers as (
SELECT
    distinct Mobile_Month as ExitMonth, 
    Mobile_Account, 
    date_add('Month', 1, Mobile_Month) as RejoinerMonth
FROM MobileCustomerStatus
WHERE Mobile_ActiveBOM = 1 and Mobile_ActiveEOM = 0
)

, RejoinerPopulation as (
SELECT 
    f.*, 
    RejoinerMonth, 
    case when i.Mobile_Account is not null then 1 else 0 end as RejoinerPopFlag, 
    case when RejoinerMonth >= date('2022-09-01') and RejoinerMonth <= date_add('month', 1, date('2022-09-01')) then 1 else 0 end as Mobile_PRMonth --- Change input month when needed
FROM MobileBase_AllFlags f
LEFT JOIN InactiveUsers i
    ON f.Mobile_Account=i.Mobile_Account and Mobile_Month = ExitMonth
)

, MobileRejoinerMonthPopulation as (
SELECT
    distinct Mobile_Month, 
    RejoinerPopFlag, 
    Mobile_PRMonth, 
    Mobile_Account, 
    date('2022-09-01') as Month
FROM RejoinerPopulation
WHERE RejoinerPopFLag = 1 and Mobile_PRMonth = 1 and Mobile_Month != date('2022-09-01') --- Change input month when needed
GROUP BY 1, 2, 3, 4
    
)

--- ### ### ### Full Mobile base ### ### ###

, FullMobileBase_Rejoiners as (
SELECT
    f.*, 
    Mobile_PRMonth, 
    case when Mobile_PRMonth = 1 and MobileMovementFlag = '4. Come Back to Life' then 1 else 0 end as Mobile_RejoinerMonth
FROM join_so_mobilebase f
LEFT JOIN MobileRejoinerMonthPopulation r 
    ON f.Mobile_Account = r.Mobile_Account and f.Mobile_Month = cast(r.Month as date)
)

SELECT * 
FROM FullMobileBase_Rejoiners 
WHERE mobile_month = date('2022-09-01') --- Change input month when needed

-- SELECT distinct account_status FROM "db-analytics-prod"."tbl_postpaid_cwc"
