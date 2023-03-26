--- ##### LCPR SPRINT 5  OPERATIONAL DRIVERS - FULL FLAGS TABLE #####

--- ### ### ### Initial steps (Common in most of the calculations)

WITH

--- --- --- Month you wish the code run for
parameters as (SELECT date_trunc('month', date ('2023-02-01')) as input_month)

--- --- --- FMC table
, fmc_table as (
SELECT
    *
FROM "db_stage_dev"."lcpr_fmc_table_feb_mar17" --- Make sure to set the month accordindly to the input month of parameters
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters)
)

, repeated_accounts as (
SELECT 
    fmc_s_dim_month, 
    fix_s_att_account, --- Operational Drivers are focused just in Fixed dynamics, not Mobile. I don't take the FMC account because sometimes in concatenates Fixed and Mobile accounts ids, which alters the results when joining with other bases using the account id.
    count(*) as records_per_user
FROM fmc_table
WHERE 
    fix_s_att_account is not null --- Making sure that we are focusing just in Fixed.
    and fix_e_att_active = 1 --- The denominator of most of the Sprint 5 is the active base.
GROUP BY 1, 2
ORDER BY 3 desc
)

, fmc_table_adj as (
SELECT 
    F.*,
    records_per_user
FROM fmc_table F
LEFT JOIN repeated_accounts R
    ON F.fix_s_att_account = R.fix_s_att_account and F.fmc_s_dim_month = R.fmc_s_dim_month
)
-- SELECT * FROM fmc_table_adj LIMIT 100
--- --- --- Interactions
, clean_interaction_time as (
SELECT *
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    (cast(interaction_start_time as varchar) != ' ') 
    and (interaction_start_time is not null)
    and date_trunc('month', cast(substr(cast(interaction_start_time as varchar),1,10) as date)) between ((SELECT input_month FROM parameters)) and ((SELECT input_month FROM parameters) + interval '1' month - interval '1' day)
    and account_type = 'RES'
)

, interactions_fields as (
SELECT
    *,
    cast(substr(cast(interaction_start_time as varchar), 1, 10) as date) as interaction_date, 
    date_trunc('month', cast(substr(cast(interaction_start_time as varchar), 1, 10) as date)) as month
FROM clean_interaction_time
)

, interactions_not_repeated as (
SELECT
    first_value(interaction_id) OVER(PARTITION BY account_id, interaction_date, interaction_channel, interaction_agent_id, interaction_purpose_descrip ORDER BY interaction_date DESC) AS interaction_id2
FROM interactions_fields
)

, interactions_fields2 as (
SELECT *
FROM interactions_not_repeated a
LEFT JOIN interactions_fields b
    ON a.interaction_id2 = b.interaction_id
)

--- --- --- External file: Truckrolls
, truckrolls as (
SELECT 
    create_dte_ojb, 
    job_no_ojb, 
    sub_acct_no_sbb
FROM "lcpr.stage.dev"."truckrolls"
)


--- ### ### ### Repeated callers

, interactions_count as (
SELECT
    date_trunc('month', interaction_date) as interaction_month, 
    account_id, 
    count(distinct interaction_id) as interactions
FROM interactions_fields2
WHERE
    interaction_date between date_add('day', -60, interaction_date) and interaction_date --- This is the Moving Window
GROUP BY 1, 2
)

, interactions_tier as (
SELECT
    *, 
    case 
        when interactions = 1 then '1'
        when interactions = 2 then '2'
        when interactions >= 3 then '>3'
        else null
    end as interaction_tier
FROM interactions_count
)


--- ### ### ### Joining all flags

, flag1_repeated_callers as(
SELECT 
    F.*, 
    case when I.account_id is not null then F.fix_s_att_account else null end as interactions, 
    interaction_tier
FROM fmc_table_adj F
LEFT JOIN interactions_tier I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fmc_s_dim_month = I.interaction_month
WHERE
    fix_e_att_active = 1 
)

--- --- ---
SELECT * FROM flag1_repeated_callers LIMIT 100

--- ### Specific numbers

-- SELECT
--     interaction_tier,
--     count(distinct fix_s_att_account) as num_cliets
-- FROM flag1_repeated_callers
-- GROUP BY 1
