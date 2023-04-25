--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 5 - OPERATIONAL DRIVERS - REPEATED CALLERS ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
WITH
  
 parameters as (
 SELECT date_trunc('month', date('2023-03-01')) as input_month --- Input month you wish the code run for
 )

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- FMC Table --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, fmc_table as (
SELECT
    *
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters)
)

, repeated_accounts as (
SELECT 
    fmc_s_dim_month, 
    fix_s_att_account,
    count(*) as records_per_user
FROM fmc_table
WHERE 
    fix_s_att_account is not null
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

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Interactions --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, clean_interaction_time as (
SELECT *
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    (cast(interaction_start_time as varchar) != ' ') 
    and (interaction_start_time is not null)
    and date_trunc('month', date(interaction_start_time)) between ((SELECT input_month FROM parameters) - interval '2' month) and ((SELECT input_month FROM parameters))
    and account_type = 'RES'
)

, interactions_fields as (
SELECT
    *, 
    date(interaction_start_time) as interaction_date, 
    date_trunc('month', date(interaction_start_time)) as month
FROM clean_interaction_time
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Repeated callers --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

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
    date_trunc('month', last_interaction_date) as last_interaction_month, 
    last_interaction_date, 
    date_add('DAY', -60, last_interaction_date) as window_day
FROM interactions_fields W
INNER JOIN last_interaction L
    ON W.account_id = L.last_account
)

, interactions_count as (
SELECT
    distinct account_id,
    last_interaction_month, 
    count(distinct interaction_id) as interactions
FROM join_last_interaction
WHERE
    interaction_date between window_day and last_interaction_date
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

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Final flags --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, interaction_tier_flag as(
SELECT 
    F.*, 
    case when I.account_id is not null then F.fix_s_att_account else null end as interactions, 
    interaction_tier
FROM fmc_table_adj F
LEFT JOIN interactions_tier I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) 
       and F.fmc_s_dim_month = I.last_interaction_month
        -- and F.fmc_s_dim_month between I.interaction_month and (I.interaction_month + interval '2' month)
WHERE
    fix_e_att_active = 1

)

, final_table as (
SELECT
    fmc_s_dim_month as opd_s_dim_month,
    fix_b_fla_tech as fmc_b_fla_tech_type,
    fix_b_fla_fmc as fmc_b_fla_fmc_status,
    fix_b_fla_mixcodeadj as fmc_b_dim_mix_code_adj,
    fix_e_fla_tech as fmc_e_fla_tech_type,
    fix_e_fla_fmc as fmc_e_fla_fmc_status,
    fix_e_fla_mixcodeadj as fmc_e_dim_mix_code_adj,
    fix_b_fla_tenure as fmc_b_fla_final_tenure,
    fix_e_fla_tenure as fmc_e_fla_final_tenure,
    fix_s_fla_churntype as fmc_s_fla_churn_type,
    interaction_tier as odr_s_fla_interaction_tier,
    sum(interactions) as odr_s_mes_user_interactions,
    count(distinct fix_s_att_account) as odr_s_mes_total_accounts
FROM interaction_tier_flag
WHERE 
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', 'Fixed Base Exception', 'Churn Exception') 
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)

SELECT * FROM final_table

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Tests --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

-- SELECT
--     odr_s_fla_interaction_tier,
--     sum(odr_s_mes_total_accounts) as num_cliets
-- FROM final_table
-- GROUP BY 1
