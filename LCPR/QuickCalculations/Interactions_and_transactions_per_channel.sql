WITH

parameters as (SELECT date_trunc('month', date('2023-01-01')) as input_month)

, interactions as (
SELECT 
    date_trunc('month', date(interaction_start_time)) as interaction_month,
    account_id, 
    interaction_id, 
    interaction_start_time, 
    interaction_channel
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE 
    -- date_trunc('month', date(interaction_start_time)) = (SELECT input_month FROM parameters)
    date_trunc('month', date(interaction_start_time)) between date('2022-12-01') and date('2023-03-31')
)

, transactions as (
SELECT
    date_trunc('month', date(create_dte_ocr)) as transaction_month, 
    sub_acct_no_ooi, 
    order_no_ooi, 
    create_dte_ocr, 
    oper_area
FROM "lcpr.sandbox.dev"."transactions_orderactivity" 
WHERE
    date_trunc('month', date(create_dte_ocr)) between date('2022-12-01') and date('2023-03-31')
)

, join_interactions_transactions as (
SELECT
    case 
        when interaction_month is not null then date(interaction_month)
        when create_dte_ocr is not null then date(create_dte_ocr)
        else null
    end as final_month, 
    account_id, 
    sub_acct_no_ooi,
    case
        when account_id is not null then cast(account_id as varchar)
        when sub_acct_no_ooi is not null then cast(sub_acct_no_ooi as varchar)
        else null
    end as final_account, 
    case
        when account_id is not null and sub_acct_no_ooi is not null then account_id
        else null
    end as match_flag,
    interaction_channel, 
    oper_area, 
    case 
        when interaction_channel = oper_area and account_id is not null then cast(account_id as varchar)
        when interaction_channel = oper_area and sub_acct_no_ooi is not null then cast(sub_acct_no_ooi as varchar)
    end as same_channel_flag
FROM interactions a
FULL OUTER JOIN transactions b
    ON cast(a.account_id as varchar) = cast(b.sub_acct_no_ooi as varchar) and date(interaction_start_time) = date(create_dte_ocr)
)

SELECT
    count(distinct account_id) as total_interactions, 
    count(distinct sub_acct_no_ooi) as total_transactions, 
    count(distinct match_flag) as total_matches, 
    count(distinct same_channel_flag) as total_match_channel
FROM join_interactions_transactions
    
    
