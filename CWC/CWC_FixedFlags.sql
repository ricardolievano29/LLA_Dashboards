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
    end as B_Mix_Code_Adj, 
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
    end as E_Mix_Code_Adj, 
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

SELECT * FROM ActiveUsersEOM LIMIT 100
