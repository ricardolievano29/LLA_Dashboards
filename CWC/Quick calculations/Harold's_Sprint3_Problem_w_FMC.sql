-----------------------------------------------------------------------------------------
------------------------- SPRINT 3 PARAMETRIZADO - V1 -----------------------------------
-----------------------------------------------------------------------------------------

-- Version 1.0: 12-04-2023: Integración de lógica de NPN 30/60/90. Se agregan flags de NPN 30/60/90 sobre una tabla creada a partir de la lógica creada por CB.


WITH 

parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT DATE_TRUNC('month',DATE('2022-09-01')) AS input_month
)

,fmc_table AS (
SELECT * FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod"
WHERE month = date(dt) AND date(dt) = (SELECT input_month FROM parameters)
)

-------------------- Sales ------------------

,previous_months_dna AS (
-- Se guardan las cuentas que aparecen en los 3 meses anteriores
SELECT  DATE_TRUNC('month',CAST(dt AS DATE)) AS month
        ,act_acct_cd
FROM "db-analytics-prod"."tbl_fixed_cwc"
WHERE org_cntry = 'Jamaica'
    AND act_cust_typ_nm IN ('Browse & Talk HFONE', 'Residence', 'Standard')
    AND act_acct_stat IN ('B','D','P','SN','SR','T','W') 
    AND DATE_TRUNC('month',DATE(dt)) BETWEEN ((SELECT input_month FROM parameters) - interval '6' month /* interval '3' month*/) AND ((SELECT input_month FROM parameters) - interval '1' month)
GROUP BY 1,2
ORDER BY 1,2
)

,sales AS (
SELECT  DATE_TRUNC('month',DATE(dt)) AS month
        ,act_acct_cd
FROM "db-analytics-prod"."tbl_fixed_cwc"
WHERE org_cntry = 'Jamaica'
    AND act_cust_typ_nm IN ('Browse & Talk HFONE', 'Residence', 'Standard')
    AND act_acct_stat IN ('B','D','P','SN','SR','T','W')
    AND DATE_TRUNC('month', DATE(dt)) = (SELECT input_month FROM parameters)
    AND act_acct_cd NOT IN (SELECT DISTINCT act_acct_cd FROM previous_months_dna)
)

,sales_w_flags AS (
        SELECT a.*
                ,b.npn_30_flag AS npn_30_flag
                ,b.npn_60_flag AS npn_60_flag
                ,b.npn_90_flag AS npn_90_flag
                -- ,b.npn_flag AS npn_flag_2
        FROM (SELECT DISTINCT * FROM sales) a
        LEFT JOIN "test_hu"."cwj_npn_cohorts" b
        ON a.act_acct_cd = b.act_acct_cd
)

-- 831 act_acct_cd
-- SELECT *
-- FROM sales_w_flags
-- WHERE npn_90_flag IS NOT NULL

-- SELECT *
-- FROM fmc_table
-- WHERE CAST(fixed_account AS VARCHAR) IN  (SELECT DISTINCT act_acct_cd FROM sales_w_flags WHERE npn_90_flag IS NOT NULL)

-------------------- Soft Dx + Never Paid ------------------
, first_bill_pre as (
SELECT 
    act_acct_cd
    ,dt
    ,FIRST_VALUE(DATE(substring(act_acct_inst_dt,1,10))) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_inst_dt
    ,FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_oldest_unpaid_bill_dt
FROM "db-analytics-prod"."tbl_fixed_cwc"
WHERE 
    org_cntry = 'Jamaica'
    AND act_cust_typ_nm IN ('Browse & Talk HFONE', 'Residence', 'Standard')
    AND act_acct_stat IN ('B','D','P','SN','SR','T','W')
    AND act_acct_cd IN (SELECT act_acct_cd FROM sales)
    AND DATE(dt) BETWEEN (DATE_TRUNC('Month', DATE(substring(act_acct_inst_dt,1,10))) - interval '6' month) AND (DATE_TRUNC('Month', DATE(substring(act_acct_inst_dt,1,10))) + interval '2' month)
    AND oldest_unpaid_bill_dt <> '19000101'
        
)

,first_bill AS (
SELECT  DATE_TRUNC('month',DATE(first_inst_dt)) AS month
        ,act_acct_cd
        ,CONCAT(MAX(act_acct_cd),'-',MIN(first_oldest_unpaid_bill_dt)) AS act_first_bill
FROM first_bill_pre
GROUP BY 1,2
)

, max_overdue_first_bill_pre_pre as (
SELECT 
    act_acct_cd
    ,FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_oldest_unpaid_bill_dt
    ,FIRST_VALUE(DATE(substring(act_acct_inst_dt,1,10))) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_inst_dt
    ,FIRST_VALUE(DATE(substring(act_cust_strt_dt,1,10))) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_act_cust_strt_dt
    ,fi_outst_age
    ,dt
    ,pd_mix_cd
FROM "db-analytics-prod"."tbl_fixed_cwc"
WHERE org_cntry = 'Jamaica'
    AND act_cust_typ_nm IN ('Browse & Talk HFONE', 'Residence', 'Standard')
    AND act_acct_stat IN ('B','D','P','SN','SR','T','W')
    AND concat(act_acct_cd,'-',oldest_unpaid_bill_dt) IN (SELECT act_first_bill FROM first_bill)
    AND DATE(dt) BETWEEN DATE_TRUNC('Month', DATE(substring(act_acct_inst_dt,1,10))) AND (DATE_TRUNC('Month', DATE(substring(act_acct_inst_dt,1,10))) + interval '5' month)
)

, max_overdue_first_bill_pre as (
SELECT  
    DATE_TRUNC('month',DATE(MIN(first_inst_dt))) as month
    ,act_acct_cd as bill_acct_cd
    ,MIN(DATE(DATE_PARSE(first_oldest_unpaid_bill_dt, '%Y%m%d'))) AS first_oldest_unpaid_bill_dt
    ,MIN(first_inst_dt) AS first_inst_dt, MIN(first_act_cust_strt_dt) AS first_act_cust_strt_dt
    ,CONCAT(MAX(act_acct_cd),'-',MIN(first_oldest_unpaid_bill_dt)) AS act_first_bill
    ,MAX(fi_outst_age) AS max_fi_outst_age
    ,MAX(DATE(dt)) AS max_dt
    ,MAX(CASE WHEN pd_mix_cd IS NULL THEN 0 ELSE CAST(REPLACE(pd_mix_cd,'P','') AS INT) END) AS RGUs
    -- ,CASE WHEN MAX(CAST(fi_outst_age AS INT)) >= 90 THEN 1 ELSE 0 END AS never_paid_flg
    ,CASE WHEN MAX(CAST(fi_outst_age AS INT)) >=36 THEN 1 ELSE 0 END AS soft_dx_flg
    ,CASE WHEN (MIN(DATE(DATE_PARSE(first_oldest_unpaid_bill_dt, '%Y%m%d'))) + interval '90' day) < current_date THEN 1 ELSE 0 END AS neverpaid_window
    ,CASE WHEN (MIN(DATE(DATE_PARSE(first_oldest_unpaid_bill_dt, '%Y%m%d'))) + interval '36' day) < current_date THEN 1 ELSE 0 END AS softdx_window
FROM max_overdue_first_bill_pre_pre
GROUP BY act_acct_cd
)

,max_overdue_first_bill AS (
SELECT 
    b.*, 
    act_acct_cd
    ,CASE WHEN a.npn_30_flag IS NULL THEN 0 ELSE 1 END AS NeverPaid_Flag_30
    ,CASE WHEN a.npn_60_flag IS NULL THEN 0 ELSE 1 END AS NeverPaid_Flag_60
    ,CASE WHEN a.npn_90_flag IS NULL THEN 0 ELSE 1 END AS NeverPaid_Flag_90
        -- ,CASE WHEN b.npn_flag_2 IS NULL THEN 0 ELSE 1 END as NeverPaid_Flag
FROM sales_w_flags a
LEFT JOIN max_overdue_first_bill_pre b
ON a.act_acct_cd = b.bill_acct_cd
)

-- 5181 act_acct_cd AND with 601 npn90
-- SELECT COUNT(DISTINCT act_acct_cd )
-- FROM max_overdue_first_bill
-- WHERE act_acct_cd IN  (SELECT DISTINCT act_acct_cd FROM sales_w_flags WHERE npn_90_flag IS NOT NULL) 


,so_inst_date_search AS (
SELECT  account_id
        ,MIN(DATE(CAST(completed_date AS TIMESTAMP))) AS completed_so_dt
FROM "db-stage-prod"."so_headers_cwc"
WHERE order_status = 'COMPLETED'
    AND order_type = 'INSTALLATION'
    AND network_type NOT IN ('LTE','MOBILE') OR network_type IS NULL
    AND CAST(account_id AS VARCHAR) IN (SELECT act_acct_cd FROM max_overdue_first_bill)
    AND DATE_TRUNC('month',DATE(order_start_date)) BETWEEN ((SELECT input_month FROM parameters) - interval '2' month) AND ((SELECT input_month FROM parameters) + interval '1' month)
GROUP BY 1
)

-- 1718 account_id and 214 npn 90
-- SELECT COUNT(DISTINCT account_id )
-- FROM so_inst_date_search
-- WHERE CAST(account_id AS VARCHAR) IN  (SELECT DISTINCT act_acct_cd FROM sales_w_flags WHERE npn_90_flag IS NOT NULL)

,final_inst_dt AS (
SELECT  *
        ,Soft_Dx_flg AS SoftDx_Flag
        -- ,never_paid_flg AS NeverPaid_Flag
        -- ,npn_30_flag
        -- ,npn_60_flag
        -- ,npn_90_flag
        ,CASE WHEN completed_so_dt > first_inst_dt THEN completed_so_dt ELSE first_inst_dt END AS first_inst_dt_final
FROM max_overdue_first_bill M 
LEFT JOIN so_inst_date_search S ON M.act_acct_cd = CAST(S.account_id AS VARCHAR) AND M.month = DATE_TRUNC('Month',S.completed_so_dt)
)

--601 with npn90
-- SELECT COUNT(DISTINCT act_acct_cd )
-- FROM final_inst_dt
-- WHERE CAST(act_acct_cd AS VARCHAR) IN  (SELECT DISTINCT act_acct_cd FROM sales_w_flags WHERE npn_90_flag IS NOT NULL)
        
,final AS (
SELECT  *
        ,CASE WHEN DATE_ADD('day',90,first_oldest_unpaid_bill_dt) < current_date THEN 1 ELSE 0 END AS never_paid_window_completed
        ,DATE_ADD('day',90,first_oldest_unpaid_bill_dt) AS threshold_never_paid_date
        ,CASE WHEN DATE_ADD('day',36,first_oldest_unpaid_bill_dt) < current_date THEN 1 ELSE 0 END AS soft_dx_window_completed
        ,DATE_ADD('day',36,first_oldest_unpaid_bill_dt) AS threshold_soft_dx_date
        ,current_date AS current_date_analysis
        ,DATE_TRUNC('month',DATE_ADD('day',90,first_oldest_unpaid_bill_dt)) AS never_paid_month
        ,DATE_TRUNC('month',DATE_ADD('day',36,first_oldest_unpaid_bill_dt)) AS soft_dx_month
FROM final_inst_dt
)

,final_w_fmc AS (
SELECT  F.*
        ,A.act_acct_cd
        ,A.first_oldest_unpaid_bill_dt
        ,A.first_inst_dt
        ,A.first_act_cust_strt_dt
        ,A.act_first_bill
        ,A.max_fi_outst_age
        ,A.max_dt
        ,A.RGUs
        ,A.NeverPaid_Flag_30
        ,A.NeverPaid_Flag_60
        ,A.NeverPaid_Flag_90
        ,A.SoftDx_Flag
        ,A.neverpaid_window
        ,A.softdx_window
        ,A.completed_so_dt
        ,A.first_inst_dt_final
        ,A.never_paid_window_completed
        ,A.threshold_never_paid_date
        ,A.soft_dx_window_completed
        ,A.threshold_soft_dx_date
        ,A.current_date_analysis
        ,A.never_paid_month
        ,A.soft_dx_month
        ,CASE WHEN A.act_acct_cd IS NOT NULL THEN 1 ELSE 0 END AS monthsale_flag
FROM fmc_table F LEFT JOIN final A ON F.fixed_account = A.act_acct_cd AND F.month = A.month
)


SELECT 
    fixed_account, 
    act_acct_cd,
    case when (fixed_account is null and act_acct_cd is not null) then act_acct_cd else null end as missing_sales
FROM fmc_table F FULL OUTER JOIN final A ON F.fixed_account = A.act_acct_cd AND F.month = A.month
ORDER BY missing_sales desc
