WITH

parameters as (SELECT date('2022-12-01') as input_month)

,service_orders_late_inst AS (
SELECT  *
        ,DATE_TRUNC('Month', DATE(order_start_date)) AS month
        ,DATE(order_start_date) AS StartDate
        ,DATE(completed_date) AS EndDate
        ,DATE_DIFF('DAY',DATE(order_start_date),DATE(completed_date)) AS installation_lapse
        FROM "db-stage-prod"."so_headers_cwc"
WHERE order_type = 'INSTALLATION' AND ACCOUNT_TYPE='Residential' AND ORDER_STATUS='COMPLETED' AND org_cntry = 'Jamaica'
    AND DATE_TRUNC('MONTH',CAST(order_start_date AS DATE)) = (SELECT input_month FROM parameters)
)

SELECT month, count(distinct order_id) FROM service_orders_late_inst GROUP BY 1
