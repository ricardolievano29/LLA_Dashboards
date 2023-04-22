--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 3 - OPERATIONAL DRIVERS - NEVER PAID ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
WITH

parameters as (SELECT date('2023-03-01') as input_month)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- New customers --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, new_customers_pre as (
SELECT
    date_trunc('month', date(dt)) as dna_month,
    cast(cast(first_value(connect_dte_sbb) over (partition by sub_acct_no_sbb order by DATE(dt) DESC) as timestamp) as date) as fix_b_att_maxstart,   
    SUB_ACCT_NO_SBB as fix_s_att_account, 
    bill_from_dte_sbb, 
    dt
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type != '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(CONNECT_DTE_SBB)) between ((SELECT input_month FROM parameters) - interval '3' month) and (SELECT input_month FROM parameters)
ORDER BY 1
)

, new_customers3m as (   
SELECT 
    dna_month,
    date_trunc('month', fix_b_att_maxstart) as install_month, 
    fix_b_att_maxstart,  
    fix_s_att_account as new_sales3m_flag,
    fix_s_att_account, 
    bill_from_dte_sbb, 
    dt
FROM new_customers_pre
WHERE date_trunc('month', date(fix_b_att_maxstart)) = ((SELECT input_month FROM parameters) - interval '3' month)
)

, new_customers as (   
SELECT 
    dna_month,
    date_trunc('month', fix_b_att_maxstart) as install_month, 
    fix_b_att_maxstart,  
    fix_s_att_account as new_sales_flag,
    fix_s_att_account
FROM new_customers_pre
WHERE date_trunc('month', date(fix_b_att_maxstart)) = (SELECT input_month FROM parameters)
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Never Paid --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

,bills_of_interest AS (
SELECT 
    dna_month,
    fix_s_att_account,
    /* I take the equivalent to oldest_unpaid_bill because no much more alternative info is available */
    first_value(bill_from_dte_sbb) over (partition by fix_s_att_account order by dt asc) as first_bill_created
    -- DATE(TRY(FILTER(ARRAY_AGG(bill_from_dte_sbb ORDER BY DATE(dt)), x -> x IS NOT NULL)[1])) AS first_bill_created
FROM new_customers3m
)


--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Final flags --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

SELECT
    distinct first_bill_created
FROM bills_of_interest

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Tests --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

-- SELECT 
--     count(distinct day_85s), 
--     count(distinct fix_s_att_account) 
-- FROM never_paid
