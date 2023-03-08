--- ########## CWC - FIXED FLAGS - PAULA MORENO (GITLAB) ##########

WITH

UsefulFields as (
SELECT 
    date_trunc('month', date(dt)) as Month, 
    first_value(dt) over (partition by act_acct_cd, date_trunc('month', date(dt)) order by dt desc) as MaxDateMonth, 
    dt, 
    act_acct_cd, 
    act_contact_phone_1, 
    act_contact_phone_2, 
    act_contact_phone_3, 
    pd_mix_cd, 
    pd_mix_nm, 
    pd_bb_prod_nm, 
    pd_tv_prod_nm, 
    pd_vo_prod_nm, 
    case 
        when IS_NAN (cast(fi_tot_mrc_amt as double)) then 0
        when not IS_NAN (cast(fi_tot_mrc_amt as double)) then round(cast(fi_tot_mrc_amt as double), 0)
        end as mrc_amt, 
    case
        when IS_NAN (cast(fi_bill_amt_m0 as double)) then 0
        when not IS_NAN (cast(fi_bill_amt_m0 as double)) then round(cast(fi_bill_amt_m0 as double), 0)
        end as bill_amtM0, 
    case
        when IS_NAN (cast(fi_bill_amt_m1 as double)) then 0
        when not IS_NAN (cast(fi_bill_amt_m1 as double)) then round(cast(fi_bill_amt_m1 as double), 0)
        end as bill_amtM1, 
    case when fi_outst_age is null then -1 else cast(fi_outst_age as integer) end as fi_outst_age, 
    fi_tot_srv_chrg_amt, 
    round(cast(fi_bb_mrc_amt as double), 0) as fi_bb_mrc_amt, 
    round(cast(fi_tv_mrc_amt as double), 0) as fi_tv_mrc_amt, 
    round(cast(fi_vo_mrc_amt as double), 0) as fi_vo_mrc_amt, 
    first_value(substring (act_cust_strt_dt, 1, 10)) over (partition by act_acct_cd order by dt desc) as MaxStart, 
    bundle_code, 
    bundle_name, 
    case when (pd_mix_nm like '%BO%') then 1 else 0 end as numBB, 
    case when (pd_mix_nm like '%TV%') then 1 else 0 end as numTV, 
    case when (pd_mix_nm like '%VO%') then 1 else 0 end as numVO, 
    case
        when length(cast(act_acct_cd as varchar)) = 8 then 'HFC'
        when NR_FDP != ' ' and NR_FDP is not null then 'FTTH'
        when pd_vo_tech = 'FIBER' then 'FTTH'
        when (pd_bb_prod_nm like '%GPON%' or pd_bb_prod_nm like '%FTT%') and (pd_bb_prod_nm not like '%ADSL%' and pd_bb_prod_nm not like '%VDSL%') then 'FTTH'
        else 'COPPER'
        end as Technology_Type,
    cst_cust_cd
FROM "db-analytics-prod"."tbl_fixed_cwc"
WHERE 
    org_cntry = 'Jamaica'
    and ACT_CUST_TYP_NM in ('Browse & Talk HFONE', 'Residence', 'Standard')
    and ACT_ACCT_STAT in ('B', 'D', 'P', 'SN', 'SR', 'T', 'W')
    and date(dt) between (date('2022-11-01') + interval '1' month - interval '1' day - interval '2' month) and (date('2022-11-01') + interval '1' month - interval '1' day) --- El mes de reporte toca variabilizarlo (no está automatizado).
)

, AverageMRC_User as (
SELECT
    distinct date_trunc('month', date(dt)) as Month, 
    act_acct_cd,
    round(avg(mrc_amt), 0) as AvgMRC, 
    round(avg(bill_amtM0), 1) as AvgBillM0, 
    round(avg(bill_amtM1), 1) AS AvgBillM1, 
    MaxDateMonth, 
    MaxStart
    FROM UsefulFields
-- WHERE mrc_amt is not null and mrc_amt != 0
GROUP BY 1, 2, MaxDateMonth, MaxStart
)

, filterbill as (
SELECT
    *, 
    case when  
        ((AvgBillM1 is not null and AvgBillM1 != 0 and date_diff('day', date(MaxStart), date(MaxDateMonth)) > 60) 
        or (date_diff('day', date(MaxStart), date(MaxDateMonth)) <= 60))
        or (AvgBillM0 is not null and AvgBillM0 != 0 and date_diff('day', date(MaxStart), date(MaxDateMonth)) > 60) 
    then 1 else 0 end as bill_filter
FROM AverageMRC_User 
)

, LastDayRGUs as (
SELECT
    act_acct_cd, 
    first_value (numBB + numTV + numVO) over (partition by act_acct_cd order by dt desc) as last_rgus
FROM UsefulFields
)

, ActiveUsersBOM as (
SELECT
    distinct date_trunc('month', date_add('month', 1, date(u.dt))) as Month, 
    u.act_acct_cd as accountBOM, 
    act_contact_phone_1 as PhoneBOM1, 
    act_contact_phone_2 as PhoneBOM2, 
    act_contact_phone_3 as PhoneBOM3, 
    u.dt as B_Date, 
    pd_mix_cd as B_MixCode,
    pd_mix_nm as B_MixName, 
    pd_bb_prod_nm as B_ProdBBName, 
    pd_tv_prod_nm as B_ProdTVName, 
    pd_vo_prod_nm as B_ProdVoName, 
    (NumBB+NumTV+NumVO) as B_NumRGUS, 
    case
        when NumBB = 1 and NumTV = 0 and NumVO = 0 then 'BO'
        when NumBB = 0 and NumTV = 1 and NumVO = 0 then 'TV' 
        when NumBB = 0 and NumTV = 0 and NumVO = 1 then 'VO' 
        when NumBB = 1 and NumTV = 1 and NumVO = 0 then 'BO+TV' 
        when NumBB = 1 and NumTV = 0 and NumVO = 1 then 'BO+VO' 
        when NumBB = 0 and NumTV = 1 and NumVO = 1 then 'TV+VO' 
        when NumBB = 1 and NumTV = 1 and NumVO = 1 then 'BO+TV+VO'
    end as B_MixName_Adj, 
    case when NumBB = 1 then u.act_acct_cd else null end as BB_RGU_BOM, 
    case when NumTV = 1 then u.act_acct_cd else null end as TV_RGU_BOM, 
    case when NumVO = 1 then u.act_acct_cd else null end as VO_RGU_BOM, 
    case 
        when (NumBB = 1 and NumTV = 0 and NumVO = 0) or (NumBB = 0 and NumTV = 1 and NumVO = 0) and (NumBB = 0 and NumTV = 0 and NumVO = 1) then '1P'
        when (NumBB = 1 and NumTV = 1 and NumVO = 0) or (NumBB = 1 and NumTV = 0 and NumVO = 1) and (NumBB = 0 and NumTV = 1 and NumVO = 1) then '2P'
        when (NumBB = 1 and NumTV = 1 and NumVO = 1) then '3P'
    end as B_MixCode_Adj, 
    mrc_amt as B_MRC, 
    fi_outst_age as B_OutstAge, 
    fi_tot_srv_chrg_amt as B_MRCAdj, 
    fi_bb_mrc_amt as B_MRCBB, 
    fi_tv_mrc_amt as B_MRCTV, 
    fi_vo_mrc_amt as B_MRCVO, 
    u.MaxStart as B_MaxStart, 
    Technology_type as B_Tech_Type, 
    bundle_code as B_bundlecode, 
    bundle_name as B_bundlename, 
    AvgMRC as B_Avg_MRC, 
    AvgBillM1 as B_Avg_Bill1, 
    AvgBillM0 as B_Avg_Bill0,
    min(last_rgus) as last_rgus
FROM UsefulFields u
LEFT JOIN filterbill a
    ON u.act_acct_cd = a.act_acct_cd and u.Month = a.Month
LEFT JOIN lastdayRGUs c
    ON u.act_acct_cd = c.act_acct_cd
WHERE
    (cast(fi_outst_age as double) < 90 or fi_outst_age is null) --- Customers with Hard DX are not taken into account
    and date(u.dt) = date_trunc('month', date(u.dt)) + interval '1' month - interval '1' day
    and bill_filter = 1 --- ???????
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19, 20, 21, 22, 12, 21, 22, 23, 13, 14, 24, 25, 26, 27, 28, 29, 30
)

, ActiveUsersEOM as (
SELECT
    distinct date_trunc('month', date(u.dt)) as Month, 
    u.act_acct_cd as accountEOM, 
    act_contact_phone_1 as PhoneEOM1, 
    act_contact_phone_2 as PhoneEOM2, 
    act_contact_phone_3 as PhoneEOM3, 
    u.dt as E_Date, 
    pd_mix_cd as E_MixCode, 
    pd_mix_nm as E_MixName, 
    pd_bb_prod_nm as E_prodBBName, 
    pd_tv_prod_nm as E_ProdTVName, 
    pd_vo_prod_nm as E_ProdVoName, 
    (NumBB + NumTV + NumVO) as E_NumRGUs, 
    case
        when NumBB = 1 and NumTV = 0 and NumVO = 0 then 'BO'
        when NumBB = 0 and NumTV = 1 and NumVO = 0 then 'TV' 
        when NumBB = 0 and NumTV = 0 and NumVO = 1 then 'VO' 
        when NumBB = 1 and NumTV = 1 and NumVO = 0 then 'BO+TV' 
        when NumBB = 1 and NumTV = 0 and NumVO = 1 then 'BO+VO' 
        when NumBB = 0 and NumTV = 1 and NumVO = 1 then 'TV+VO' 
        when NumBB = 1 and NumTV = 1 and NumVO = 1 then 'BO+TV+VO'
    end as E_MixName_Adj, 
    case when NumBB = 1 then u.act_acct_cd else null end as BB_RGU_EOM, 
    case when NumTV = 1 then u.act_acct_cd else null end as TV_RGU_EOM, 
    case when NumVO = 1 then u.act_acct_cd else null end as VO_RGU_EOM, 
    case 
        when (NumBB = 1 and NumTV = 0 and NumVO = 0) or (NumBB = 0 and NumTV = 1 and NumVO = 0) and (NumBB = 0 and NumTV = 0 and NumVO = 1) then '1P'
        when (NumBB = 1 and NumTV = 1 and NumVO = 0) or (NumBB = 1 and NumTV = 0 and NumVO = 1) and (NumBB = 0 and NumTV = 1 and NumVO = 1) then '2P'
        when (NumBB = 1 and NumTV = 1 and NumVO = 1) then '3P'
    end as E_MixCode_Adj, 
    mrc_amt as E_mrc, 
    fi_outst_age as E_OutstAge, 
    fi_tot_srv_chrg_amt as E_MRCAdj, 
    fi_bb_mrc_amt as E_MRCBB, 
    fi_tv_mrc_amt as E_MRCTV, 
    fi_vo_mrc_amt as E_MRCVO, 
    u.MaxStart as E_MaxStart, 
    Technology_type as E_Tech_Type, 
    bundle_code as E_bundlecode, 
    bundle_name as E_bundlename, 
    AvgMRC as E_Avg_MRC, 
    AvgBillM1 as E_Avg_Bill1, 
    AvgBillM0 as E_Avg_Bill0
FROM UsefulFields u
LEFT JOIN filterbill a
    ON u.act_acct_cd = a.act_acct_cd and u.Month = a.Month
WHERE
    (cast(fi_outst_age as double) <= 90 or fi_outst_age is null) --- Clients with Hard Dx are not taken into account
    and date(u.dt) = date_trunc('month', date(u.dt)) + interval '1' month - interval '1' day
    and bill_filter = 1 --- ????
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19, 20, 21, 22, 12, 22, 23, 24, 13, 14, 25, 26, 27, 28, 29, 30
)

, CustomerBase as (
SELECT distinct
    case 
        when (accountBOM is not null and accountEOM is not null) OR (accountBOM is not null and accountEOM is null) then b.Month
        when (accountBOM is null and accountEOM is not null) then e.Month
    end as Fixed_Month, 
    case
        when (accountBOM is not null and accountEOM is not null) or (accountBOM is not null and accountEOM is null) then accountBOM
        when (accountBOM is null and accountEOM is not null) then accountEOM
    end as Fixed_Account, 
    case
        when (accountBOM is not null and accountEOM is not null) or (accountBOM is not null and accountEOM is null) then phoneBOM1
        when (accountBOM is null and accountEOM is not null) then phoneEOM1
    end as f_contactphone1, 
    case
        when (accountBOM is not null and accountEOM is not null) or (accountBOM is not null and accountEOM is null) then phoneBOM2
        when (accountBOM is null and accountEOM is not null) then phoneEOM2
    end as f_contactphone2, 
    case
        when (accountBOM is not null and accountEOM is not null) or (accountBOM is not null and accountEOM is null) then phoneBOM3
        when (accountBOM is null and accountEOM is not null) then phoneEOM2
    end as f_contactphone3, 
    case when accountBOM is not null then 1 else 0 end as ActiveBOM, 
    case when accountEOM is not null then 1 else 0 end as ActiveEOM, 
    B_Date, 
    B_Tech_Type, 
    B_MixCode, 
    B_MixCode_Adj, 
    B_MixName, 
    B_MixName_Adj, 
    B_ProdBBName, 
    B_ProdTVName, 
    B_ProdVoName, 
    BB_RGU_BOM, 
    TV_RGU_BOM, 
    VO_RGU_BOM 
    B_NumRGUs, 
    B_bundlecode, 
    B_bundlename, 
    B_MRC, 
    B_OutstAge, 
    B_MRCAdj, 
    B_MRCBB, 
    B_MRCTV, 
    B_MRCVO, 
    B_Avg_MRC, 
    B_Avg_Bill1, 
    B_Avg_Bill0, 
    B_MaxStart, 
    date_diff('day', date(B_MaxStart), date(B_Date)) as B_TenureDays,
    case 
        when date_diff('day', date(B_MaxStart), date(B_Date)) <= 180 then 'Early-Tenure'
        when date_diff('day', date(B_MaxStart), date(B_Date)) > 180 and date_diff('day', date(B_MaxStart), date(B_date)) <= 360 then 'Mid-Tenure'
        when date_diff('day', date(B_MaxStart), date(B_Date)) > 360 then 'Late-Tenure'
    end as B_FixedTenureSegment, 
    E_Date, 
    E_Tech_Type, 
    E_MixCode, 
    E_MixCode_Adj, 
    E_MixName, 
    E_MixName_Adj, 
    E_ProdBBName, 
    E_ProdTVName, 
    E_ProdVoName, 
    BB_RGU_EOM, 
    TV_RGU_EOM, 
    VO_RGU_EOM 
    E_NumRGUs, 
    E_bundlecode, 
    E_bundlename, 
    case when (E_MRC = 0 or E_MRC is null) then B_MRC else E_MRC end as E_MRC, 
    E_OutstAge, 
    E_MRCAdj, 
    E_MRCBB, 
    E_MRCTV, 
    E_MRCVO, 
    E_Avg_MRC, 
    E_Avg_Bill1, 
    E_Avg_Bill0, 
    E_MaxStart,
    date_diff('day', date(E_MaxStart), date(E_Date)) as E_TenureDays, 
    case 
        when date_diff('day', date(E_MaxStart), date(E_Date)) <= 180 then 'Early-Tenure'
        when date_diff('day', date(E_MaxStart), date(E_Date)) > 180 and date_diff('day', date(E_MaxStart), date(E_date)) <= 360 then 'Mid-Tenure'
        when date_diff('day', date(E_MaxStart), date(E_Date)) > 360 then 'Late-Tenure'
    end as E_FixedTenureSegment, 
    last_rgus
FROM ActiveUsersBOM b
FULL OUTER JOIN ActiveUsersEOM e
    ON b.accountBOM = e.accountEOM and b.Month = e.Month
ORDER BY Fixed_Account
)

, MainMovementBase as (
SELECT
    a.*, 
    (E_MRC - B_MRC) as MRCDiff, 
    case
        when (cast(E_NumRGUs as int) - cast(B_NumRGUs as int)) = 0 then '1.SameRGUS'
        when (cast(E_NumRGUs as int) - cast(B_NumRGUs as int)) > 0 then '2.Upsell'
        when (cast(E_NumRGUs as int) - cast(B_NumRGUs as int)) < 0 then '3.Downsell'
        when (B_NumRGUs is null and cast(E_NumRGUs as int) > 0 and date_trunc('month', date(E_MaxStart)) = date('2022-11-01')) then '4.New Customer'
        when (B_NumRGUs is null and cast(E_NumRGUs as int) > 0 and date_trunc('month', date(E_MaxStart)) != date('2022-11-01')) then '5.Come Back to Life'
        when (cast(B_NumRGUs as int) > 0 and E_NumRGUs is null) then '6.Null last day'
        when (B_NumRGUs is null and E_NumRGUs is null) then '7.Always null'
    end as mainmovement_raw
FROM CustomerBase a
)

, SpinMovementBase as (
SELECT
    *, 
    case
        when mainmovement_raw = '1.SameRGUs' and (E_MRC - B_MRC) > 0 then '1. Up-spin'
        when mainmovement_raw = '1.SameRGUs' and (E_MRC - B_MRC) < 0 THEN '2. Down-spin'
        else '3. No Spin'
    end as SpinMovement
FROM MainMovementBase
)

--- ### ### ### ### ### Fixed Churn Flags ### ### ### ### ###

, panel_so as ( --- Service Orders Panel
SELECT
    account_id, 
    order_id, 
    case when max(lob_vo_count) > 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end as vol_lob_vo_count, 
    case when max(lob_bb_count) > 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end as vol_lob_bb_count, 
    case when max(lob_tv_count) > 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end as vol_lob_tv_count, 
    date_trunc('month', completed_date) as completed_month, 
    completed_date, 
    cease_reason_group, 
    org_cntry, 
    order_status, 
    network_type, 
    order_type, 
    account_type, 
    lob_VO_count, 
    lob_BB_count, 
    lob_TV_count, 
    customer_id
FROM (
    SELECT *
    FROM "db-stage-dev"."so_hdr_cwc"
    WHERE 
        org_cntry = 'Jamaica'
        and (cease_reason_group in ('Voluntary', 'Customer Service Transaction', 'Involuntary') or cease_reason_group is null)
        and (network_type not in ('LTE', 'MOBILE') or network_type is null)
        and order_status = 'COMPLETED' 
        and account_type = 'Residential'
    )
GROUP BY account_id, order_id, lob_vo_count, lob_bb_count, lob_tv_count, date_trunc('month', completed_date), completed_date, customer_id, cease_reason_group, org_cntry, order_status, network_type, order_type, account_type
ORDER BY completed_month, account_id, order_id
)

--- ##### Voluntary Churners #####

--- Voluntary churners base

, volchurners_so as (
SELECT 
    *, 
    case when lob_vo_count > 0 then 1 else 0 end as VO_Churn, 
    case when lob_BB_count > 0 then 1 else 0 end as BB_Churn, 
    case when lob_TV_count > 0 then 1 else 0 end as TV_Churn
FROM panel_so
WHERE
    org_cntry = 'Jamaica'
    and cease_reason_group in ('Voluntary')
    and network_type not in ('LTE', 'MOBILE')
    and order_status = 'COMPLETED'
    and account_type = 'Residential'
)

--- Number of churned RGUs on the maximun date - it doesn't consider mobile

, ChurnedRGUs_so as (
SELECT
    *, 
    (VO_Churn + BB_Churn + TV_Churn) as ChurnedRGUs
FROM volchurners_so
)

--- Number of RGUs a customer has on the last record of the month

, RGUsLastRecordDNA as (
SELECT
    distinct date_trunc('month', date(dt)) as Month, 
    act_acct_cd, 
    cst_cust_cd, 
    case 
        when last_value(pd_mix_nm) over (partition by act_acct_cd, date_trunc('month', date(dt)) order by dt) in ('VO', 'BO', 'TV') then 1
        when last_value(pd_mix_nm) over (partition by act_acct_cd, date_trunc('month', date(dt)) order by dt) in ('BO+VO', 'BO+TV', 'VO+TV') then 2
        when last_value(pd_mix_nm) over (partition by act_acct_cd, date_trunc('month', date(dt)) order by dt) in ('BO+VO+TV') then 3
        else 0
    end as NumRGUsLastRecord
FROM UsefulFields
WHERE (cast(fi_outst_age as double) <= 90 or fi_outst_age is null)
ORDER BY act_acct_cd
)

--- Date of the last record of the month per customer

, LastRecordDateDNA as (
SELECT
    distinct date_trunc('month', date(dt)) as Month, 
    act_acct_cd, 
    max(dt) as LastDate, 
    cst_cust_cd
FROM Usefulfields
WHERE (cast(fi_outst_age as double) <= 90 or fi_outst_age is null) --- Users with Hard Dx are omitted
GROUP BY 1, act_acct_cd, cst_cust_cd
ORDER BY act_acct_cd
)

--- Number of outstanding days on the last record data

, OverdueLastRecordDNA as (
SELECT
    distinct date_trunc('month', date(dt)) as Month, 
    t.act_acct_cd, 
    fi_outst_age as LastOverdueRecord, 
    t.cst_cust_cd, 
    (date_diff('day', date(dt), date(MaxStart))) as ChurnTenureDays
FROM UsefulFields t
INNER JOIN LastRecordDateDNA d
    ON t.act_acct_cd = d.act_acct_cd and t.dt = d.LastDate
)

--- Total Voluntary Churners considering number of churned RGUs, outsanding age and churn date

, VoluntaryTotalChurners as (
SELECT
    distinct l.Month, 
    l.act_acct_cd, 
    d.LastDate, 
    o.ChurnTenureDays, 
    case when length(cast(l.act_acct_cd as varchar)) = 12 then '1. Liberate' else '2. Cerilion' end as BillingSystem, 
    case when (date(d.LastDate) = date_trunc('month', date(d.LastDate)) or date(d.LastDate) = date_trunc('MONTH', date(d.LastDate)) + interval '1' month - interval '1' day) then '1. First/Last Day Churner' else '2. Other Date Churner' end as ChurnDateType, 
    case when cast(LastOverdueRecord as double) >= 90 then '2. Fixed Mixed Churner' else '1. Fixed Voluntary Churner' end as ChurnerType
FROM ChurnedRGUs_so v
INNER JOIN RGUsLastRecordDNA l
    ON cast(v.customer_id as double) = cast(l.cst_cust_cd as double) and v.ChurnedRGUs >= l.NumRGUslastRecord and date_trunc('month', v.completed_date) = l.Month
INNER JOIN LastRecordDateDNA d
    ON cast(l.act_acct_cd as double) = cast(d.act_acct_cd as double) and l.Month = d.Month
INNER JOIN OverDueLastRecordDNA o
    ON cast(l.act_acct_cd as double) = cast(o.act_acct_cd as double) and l.Month = o.Month
WHERE cease_reason_group = 'Voluntary'
)

, VoluntaryChurners as (
SELECT 
    Month, 
    cast(act_acct_cd as varchar) as Account, 
    ChurnerType, 
    ChurnTenureDays
FROM VoluntaryTotalChurners
WHERE ChurnerType = '1. Fixed Voluntary Churner'
GROUP BY Month, act_acct_cd, ChurnerType, ChurnTenureDays
)

--- ##### Involuntary Churners #####

, Customers_FirstLast_Record as (
SELECT
    distinct date_trunc('month', date(dt)) as Month, 
    act_acct_cd as Account, 
    min(dt) as FirstCustRecord, 
    max(dt) as LastCustRecord, 
    try(filter(array_agg(cast(fi_outst_age as int) order by dt desc), x->x != -1)[1]) as fi_outst_age_v2
FROM UsefulFields
GROUP BY 1, 2
)

, No_Overdue as (
SELECT
    distinct date_trunc('month', date(dt)) as Month, 
    act_acct_cd as Account, 
    fi_outst_age
FROM UsefulFields t
INNER JOIN Customers_FirstLast_Record r
    ON t.dt = r.FirstCustRecord and r.account = t.act_acct_cd
WHERE cast(fi_outst_age as double) <= 90
GROUP BY 1, 2, fi_outst_age
)

, OverdueLastDay as (
SELECT
    distinct date_trunc('month', date(dt)) as Month, 
    act_acct_cd as Account, 
    fi_outst_age, 
    (date_diff('day', date(MaxStart), date(dt))) as ChurnTenureDays
FROM UsefulFields t
INNER JOIN Customers_FirstLast_Record r
    ON t.dt = r.LastCustRecord and r.Account = t.act_acct_cd
WHERE cast(fi_outst_age as double) >= 90 or (fi_outst_age_v2 >= 90 and date(LastCustRecord) < date_trunc('month', date(dt)) + interval '1' month - interval '1' day)
GROUP BY 1, 2, fi_outst_age, 4
)

, InvoluntaryNetChurners as (
SELECT 
    distinct n.Month as Month, 
    n.Account, 
    l.ChurnTenureDays
FROM No_Overdue n
INNER JOIN OverdueLastDay l
    ON n.account = l.account and n.Month = l.Month
)

, InvoluntaryChurners as (
SELECT 
    distinct Month, 
    cast(Account as varchar) as Account, 
    ChurnTenureDays, 
    case when Account is not null then '2. Fixed Involuntary Churner' end as ChurnerType
FROM InvoluntaryNetChurners
GROUP BY Month, 2, 4, ChurnTenureDays
)

--- ##### Voluntary and Involuntary Churners #####

, AllChurners as (
SELECT 
    distinct Month, 
    Account, 
    ChurnerType, 
    ChurnTenureDays
FROM (
    SELECT 
        Month, 
        Account, 
        ChurnerType, 
        ChurnTenureDays
    FROM (
        SELECT Month, Account, ChurnerType, ChurnTenureDays 
        FROM VoluntaryChurners a 
        UNION ALL 
            SELECT MONTH, Account, ChurnerType, ChurnTenureDays 
            FROM InvoluntaryChurners b)
        )
    
)

, FixedBase_AllFlags as (
SELECT
    s.*, 
    case when c.account is not null then '1. Fixed Churner' else '2. Non-churner' end as FixedChurnFlag, 
    case when c.account is not null then ChurnerType else '2. Non-churners' end as FixedChurnTypeFlag, 
    ChurnTenureDays, 
    case 
        when ChurnTenureDays <= 180 then '0. Early-tenure Churner'
        when ChurnTenureDays > 180 and ChurnTenureDays <= 360 then '1. Mid-tenure Churner'
        when ChurnTenureDays > 360 then '2. Late-tenure churner'
        when ChurnTenureDays is null then '3. Non-Churner'
    end as ChurnTenureSegment
FROM SpinMovementBase s
LEFT JOIN AllChurners c
    ON cast(s.Fixed_Account as bigint) = cast(c.Account as bigint) and s.Fixed_Month = c.Month
)

SELECT * FROM AllChurners LIMIT 10
