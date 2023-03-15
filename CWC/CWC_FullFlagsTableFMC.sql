--- ########## CWC - SPRINT 2 - FULL FLAGS TABLE - PAULA MORENO (GITLAB) ##########

WITH 
Fixed_Base AS(
  SELECT * FROM "lla_cco_int_san"."cwc_fix_stg_dashboardinput_sep_bn"
)

,Mobile_Base AS(
  SELECT * FROM "lla_cco_int_san"."cwc_mob_stg_dashboardinput_sep_bn"
)

--- ### ### ### FMC MATCH ### ### ###

, FMC_Base as(
SELECT
    date_trunc('MONTH',DATE( fix_dna.dt)) as fix_month,
    fix_dna.act_acct_cd, 
    fix_dna.bundle_code,
    fix_dna.bundle_name,
    fix_dna.bundle_inst_date,
    fix_dna.fmc_flag as fix_fmcflag,
    fix_dna.fmc_status,
    fix_dna.fmc_start_date,
    date_trunc('MONTH',DATE( mob_dna.dt)) as mob_month,
    mob_dna.account_id,
    mob_dna.subscription_id,
    mob_dna.plan_code, 
    mob_dna.phone_no,
    mob_dna.plan_name,
    mob_dna.plan_activation_date,
    mob_dna.fmc_flag as mob_fmcflag,
    mob_dna.fmc_household_id as mob_household
FROM "db-analytics-prod"."tbl_fixed_cwc" fix_dna
INNER join "db-analytics-prod"."tbl_postpaid_cwc" mob_dna 
    ON cast(mob_dna.org_id as int) = 338
        and cast(mob_dna.run_id as int) = cast(to_char(cast(fix_dna.dt as date),'yyyymmdd') as int) 
        and mob_dna.fmc_household_id = fix_dna.fmc_household_id
WHERE 
    fix_dna.org_cntry = 'Jamaica'
    and mob_dna.fmc_flag = 'Y'
    and fix_dna.dt = mob_dna.dt
)


SELECT * FROM FMC_Base LIMIT 100
