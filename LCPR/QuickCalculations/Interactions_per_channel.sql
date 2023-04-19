WITH

-- parameters as (
-- SELECT
--     date_trunc('month', date('2023-01-01')) as input_month
-- )

january_calls as (
SELECT 
    interaction_channel, 
    count(distinct interaction_id)
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    date_trunc('month', date(interaction_start_time)) = date('2023-01-01')
GROUP BY 1
)

, interactions as (
SELECT 
    interaction_channel,
    case when date_trunc('month', date(interaction_start_time)) = date('2023-01-01') then interaction_id else null end as Jan2023, 
    case when date_trunc('month', date(interaction_start_time)) = date('2023-02-01') then interaction_id else null end as Feb2023, 
    case when date_trunc('month', date(interaction_start_time)) = date('2023-03-01') then interaction_id else null end as Mar2023
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    date_trunc('month', date(interaction_start_time)) between date('2023-01-01') and date('2023-03-01')
ORDER BY interaction_channel desc
)

, interactions_per_channel as (
SELECT
    distinct interaction_channel, 
    count(distinct Jan2023) as Jan2023, 
    count(distinct Feb2023) as Feb2023, 
    count(distinct Mar2023) as Mar2023
FROM interactions
GROUP BY 1
ORDER BY interaction_channel asc
)

SELECT * FROM interactions_per_channel
