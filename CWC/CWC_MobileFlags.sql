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

SELECT * FROM MobileFields LIMIT 100

-- SELECT distinct account_status FROM "db-analytics-prod"."tbl_postpaid_cwc"
