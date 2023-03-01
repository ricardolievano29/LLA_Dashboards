--- ##### LCPR SPRINT 5  OPERATIONAL DRIVERS - NODES TICKET DENSITY #####

--- ### Initial steps

WITH

 parameters as (
 SELECT date_trunc('month', date('2022-12-01')) as input_month --- Input month you wish the code run for
 )



, fmc_table as ( --- This actually is the Fixed Table, it is called fmc just to get ready for when that table is ready
SELECT
    fix_s_dim_month, --- month
    fix_b_fla_tech, --- B_Final_TechFlag
    fix_b_fla_fmc, --- B_FMCSegment
    fix_b_fla_mixcodeadj, --- B_FMCType
    fix_e_fla_tech, --- E_Final_Tech_Flag
    fix_e_fla_fmc, --- E_FMCSegment
    fix_e_fla_mixcodeadj, --- E_FMCType
    fix_b_fla_tenure, -- b_final_tenure
    fix_e_fla_tenure, --- e_final_tenure
    --- B_FixedTenure
    --- E_FixedTenure
    --- finalchurnflag
    fix_s_fla_churntype, --- fixedchurntype
    fix_s_fla_churnflag, --- fixedchurnflag
    fix_s_fla_mainmovement, --- fixedmainmovement
    --- waterfall_flag
    --- finalaccount
    fix_s_att_account, -- fixedaccount
    fix_e_att_active --- f_activebom
    --- mobile_activeeom
    --- mobilechurnflag
FROM "db_stage_dev"."lcpr_fixed_table_dec_feb28" --- Keep this updated to the latest version!
WHERE 
    fix_s_dim_month = (SELECT input_month FROM parameters)
)

, repeated_accounts as (
SELECT 
    fix_s_dim_month, 
    fix_s_att_account, 
    count(*) as records_per_user
FROM fmc_table
GROUP BY 1, 2
ORDER BY 3 desc
)

, fmc_table_adj as (
SELECT 
    F.*,
    records_per_user
FROM fmc_table F
LEFT JOIN repeated_accounts R
    ON F.fix_s_att_account = R.fix_s_att_account and F.fix_s_dim_month = R.fix_s_dim_month
)

, clean_interaction_time as (
SELECT *
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    (cast(interaction_start_time as varchar) != ' ') 
    and (interaction_start_time is not null)
    and date_trunc('month', cast(substr(cast(interaction_start_time as varchar),1,10) as date)) between ((SELECT input_month FROM parameters)) and ((SELECT input_month FROM parameters) + interval '1' month)
        
)

, interactions_fields as (
SELECT
    *, 
    cast(substr(cast(interaction_start_time as varchar), 1, 10) as date) as interaction_date, 
    date_trunc('month', cast(substr(cast(interaction_start_time as varchar), 1, 10) as date)) as month
FROM clean_interaction_time
)

--- ### Nodes

, nodes_data as (
SELECT 
    sub_acct_no_sbb,
    bridger_addr_hse 
    -- *
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
)

--- ### Callers

, last_interaction as (
SELECT 
    account_id as last_account, 
    first_value(interaction_date) over (partition by account_id, date_trunc('month', interaction_date) order by interaction_date desc) as last_interaction_date
FROM interactions_fields
)

, join_last_interaction as (
SELECT
    account_id, 
    interaction_id, 
    interaction_date, 
    date_trunc('month', last_interaction_date) as interaction_month, 
    last_interaction_date, 
    date_add('MONTH', -1, last_interaction_date) as window_day
FROM interactions_fields W
INNER JOIN last_interaction L
    ON W.account_id = L.last_account
)

, interactions_count as (
SELECT
    interaction_month, 
    account_id, 
    count(distinct interaction_id) as interactions
FROM join_last_interaction
WHERE
    interaction_date between window_day and last_interaction_date
GROUP BY 1, 2
)


--- ### Flags for HFC nodes and callers

, hfcnode_flag as (
SELECT 
    F.*, 
    bridger_addr_hse
FROM fmc_table_adj F
LEFT JOIN nodes_data N
    ON cast(F.fix_s_att_account as varchar) = cast(N.sub_acct_no_sbb as varchar)

)

, callers_flag as (
SELECT 
    F.*, 
    interactions
FROM hfcnode_flag F
LEFT JOIN interactions_count I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fix_s_dim_month = I.interaction_month
)

--- ### Final results
, final_table as(
SELECT
    bridger_addr_hse as node, 
    cast(count(distinct fix_s_att_account) as double) as num_clients, 
    cast(count(distinct case when interactions != 0 then fix_s_att_account else null end) as double) as num_callers
FROM callers_flag
GROUP BY 1
)

SELECT cast(num_callers/num_clients as double) FROM final_table

-- SELECT distinct interactions FROM callers_flag LIMIT 100
