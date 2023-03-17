--- ########## CWC - SPRINT 4 (2) - VOLUNTARY CHURN EQUATION - PAULA MORENO (GITLAB) ##########

WITH

--- Calling the main
FMCTABLE as (
SELECT *
FROM "lla_cco_int_ana_dev"."cwc_fmc_churn_dev"
WHERE month = date(dt)
)

--- ### ### ### Calling the bases for RGUs ### ### ###

--- ### ### ### Disconnection

, diss as (
SELECT 
    account_number as diss_id, 
    lower(disconnected_services) as diss_services, 
    date_trunc('month', date_parse(service_end_dt, '%m/%d/%Y')) as mth, 
    department as department, 
    case
        when lower(disconnected_services) = 'Click' THEN 'BO'
		when lower(disconnected_services) = 'watch' THEN 'TV'
		when lower(disconnected_services) = 'Talk' THEN 'VO'
		when lower(disconnected_services) = 'NA' THEN 'NA' --HAY NA
		when lower(disconnected_services) = 'na,click' THEN 'BO' --HAY NA
		when lower(disconnected_services) = 'Watch,click' THEN 'BO+TV'
		when lower(disconnected_services) = 'talk,click' THEN 'BO+VO'
		when lower(disconnected_services) = 'Watch,talk' THEN 'VO+TV'
		when lower(disconnected_services) = 'Watch,talk,click ' THEN 'BO+VO+TV'
		when lower(disconnected_services) = 'talk,mobile,click' THEN 'BO+VO' --HAY MOBILE
    end as dis_mixname
FROM "lla_cco_int_ext_dev"."cwc_ext_disconnections"
WHERE disconnected_services != 'MOBILE'
)

, mvm as (
SELECT
    cc as ID, 
    'completion mth' as mth,
    case
        when play_cl >= play_op then 'total_retention'
        when play_cl < play_op then 'partial_retention'
    end as TypeRet
FROM "lla_cco_int_ext_dev"."cwc_ext_retention"
)

, final_mvm as (
SELECT
    id as mvm_id, 
    TypeRet, 
    date_parse(mth, '%m/%d/%Y') as mth
FROM mvm
)

, joint_diss as (
SELECT * 
FROM fmctable t
    LEFT JOIN diss d
        ON cast(d.diss_id as varchar) = t.fixed_account and d.mth = t.fixed_month
)

, joint_diss_ret as (
SELECT * 
FROM joint_diss t
LEFT JOIN final_mvm d
    ON cast(d.mvm_id as varchar) = t.fixed_account and d.mth = t.fixed_month
)

SELECT * FROM joint_diss LIMIT 10



