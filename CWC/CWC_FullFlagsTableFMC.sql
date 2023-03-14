--- ########## CWC - FMC FLAGS TABLE - PAULA MORENO (GITLAB) ##########

WITH

parameters as (
--- Input month
SELECT date_trunc('month', date('2022-10-01')) as input_month
)

, fmc_table as (
SELECT *
FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod"
WHERE 
    month = date(dt)
    and date(dt) = (SELECT input_month FROM parameters)
)

SELECT * FROM fmc_table LIMIT 100
