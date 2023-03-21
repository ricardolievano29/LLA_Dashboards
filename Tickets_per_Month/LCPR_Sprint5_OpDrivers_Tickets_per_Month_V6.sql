--- ##### LCPR SPRINT 5  OPERATIONAL DRIVERS - TICKETS PER MONTH (NUMBER OF TICKETS) #####

--- ### Initial steps

WITH

 parameters as (
 SELECT date_trunc('month', date('2023-01-01')) as input_month --- Input month you wish the code run for
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
FROM "db_stage_dev"."lcpr_fixed_table_jan_mar17" --- Make sure the right table is being used accordingly to the month requested.
WHERE 
    fix_s_dim_month = (SELECT input_month FROM parameters)
    and fix_e_att_active = 1
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

--- ### Tickets per month

, users_tickets as (
SELECT
    distinct account_id, 
    interaction_id, 
    interaction_date, 
    interaction_purpose_descrip,
    case when (
        lower(interaction_purpose_descrip) like '%ppv%problem%'
        or lower(interaction_purpose_descrip) like '%hsd%problem%'
        or lower(interaction_purpose_descrip) like '%cable%problem%'
        or lower(interaction_purpose_descrip) like '%tv%problem%'
        or lower(interaction_purpose_descrip) like '%video%problem%'
        or lower(interaction_purpose_descrip) like '%tel%problem%'
        or lower(interaction_purpose_descrip) like '%phone%problem%'
        or lower(interaction_purpose_descrip) like '%int%problem%'
        or lower(interaction_purpose_descrip) like '%line%problem%'
        or lower(interaction_purpose_descrip) like '%hsd%issue%'
        or lower(interaction_purpose_descrip) like '%ppv%issue%'
        or lower(interaction_purpose_descrip) like '%video%issue%'
        or lower(interaction_purpose_descrip) like '%tel%issue%'
        or lower(interaction_purpose_descrip) like '%phone%issue%'
        or lower(interaction_purpose_descrip) like '%int%issue%'
        or lower(interaction_purpose_descrip) like '%line%issue%'
        or lower(interaction_purpose_descrip) like '%cable%issue%'
        or lower(interaction_purpose_descrip) like '%tv%issue%'
        or lower(interaction_purpose_descrip) like '%bloq%'
        or lower(interaction_purpose_descrip) like '%slow%'
        or lower(interaction_purpose_descrip) like '%service%'
        or lower(interaction_purpose_descrip) like '%hsd%'
        or lower(interaction_purpose_descrip) like '%no%browse%'
        or lower(interaction_purpose_descrip) like '%phone%cant%'
        or lower(interaction_purpose_descrip) like '%phone%no%'
        or lower(interaction_purpose_descrip) like '%no%connect%'
        ot lower(interaction_purpose_descrip) like '%no%conect%'
        or lower(interaction_purpose_descrip) like '%no%start%'
        or lower(interaction_purpose_descrip) like '%equip%'
        or lower(interaction_purpose_descrip) like '%intermit%'
        or lower(interaction_purpose_descrip) like '%no%dat%'
        or lower(interaction_purpose_descrip) like '%dat%serv%'
        or lower(interaction_purpose_descrip) like '%int%data%'
        or lower(interaction_purpose_descrip) like '%tech%'
        or lower(interaction_purpose_descrip) like '%supp%'
        or lower(interaction_purpose_descrip) like '%outage%'
        or lower(interaction_purpose_descrip) like '%mass%'
        ) then interaction_id else null
    end as techticket_flag,
    cast(job_no_ojb as varchar) as truckroll_flag
FROM interactions_fields a
LEFT JOIN (SELECT * FROM "lcpr.stage.dev"."truckrolls" WHERE substr(create_dte_ojb, 1, 1) != '"' ) b
        -- and cast(sub_acct_no_sbb as varchar) not in ('', ' ') and sub_acct_no_sbb is not null) b 
    ON a.interaction_date = cast(create_dte_ojb as date) and cast(a.account_id as varchar) = cast(b.sub_acct_no_sbb as varchar)
WHERE 
    interaction_purpose_descrip not in ('Work Order Status', 'Default Call Wrapup', 'G:outbound Calls', 'Eq: Cust. First', 'Eq: Audit', 'Eq: Code Error', 'Downgrade Service', 'Disconnect Service', 'Rt: Dowgrde Service', 'Cust Service Calls')
)

, tickets_per_month as (
SELECT
    date_trunc('month', interaction_date) as month, 
    account_id, 
    count(distinct 
        case 
            when techticket_flag is null and truckroll_flag is null then null
            when techticket_flag is not null and truckroll_flag is null then techticket_flag
            when techticket_flag is null and truckroll_flag is not null then truckroll_flag
            when techticket_flag is not null and truckroll_flag is not null then techticket_flag
        end)
    as number_tickets
FROM users_tickets
WHERE interaction_id is not null
GROUP BY 1, 2
)

--- ### Tickets per month flag (number of tickets)

, number_tickets_flag as (
SELECT
    F.*, 
    number_tickets
FROM fmc_table_adj F 
LEFT JOIN tickets_per_month I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fix_s_dim_month = I.month
)

, final_fields as (
SELECT
    distinct fix_s_dim_month, -- month
    fix_b_fla_tech, -- B_Final_TechFlag
    fix_b_fla_fmc, -- B_FMCSegment
    fix_b_fla_mixcodeadj, -- B_FMCType
    fix_e_fla_tech, -- E_Final_TechFlag
    fix_e_fla_fmc, -- E_FMCSegment
    fix_e_fla_mixcodeadj, -- E_FMCType
    -- b_final_tenure
    -- e_final_tenure
    fix_b_fla_tenure, -- B_FixedTenure
    fix_e_fla_tenure, -- E_FixedTenure
    -- finalchurnflag
    fix_s_fla_churnflag, -- fixedchurnflag
    fix_s_fla_churntype, -- fixedchurntype
    fix_s_fla_mainmovement, -- fixedmainmovement
    -- waterfall_flag
    -- mobile_activeeom
    -- mobilechurnflag
   -- finalaccount
    fix_s_att_account, -- fixedaccount
    records_per_user,
    number_tickets
FROM number_tickets_flag
WHERE fix_s_fla_churnflag = '2. Fixed NonChurner'
)

SELECT
     fix_s_dim_month, -- month
     fix_b_fla_tech, -- B_Final_TechFlag
     fix_b_fla_fmc, -- B_FMCSegment
     fix_b_fla_mixcodeadj, -- B_FMCType
     fix_e_fla_tech, -- E_Final_TechFlag
     fix_e_fla_fmc, -- E_FMCSegment
     fix_e_fla_mixcodeadj, -- E_FMCType
     -- b_final_tenure
     -- e_final_tenure
     fix_b_fla_tenure, -- B_FixedTenure
     fix_e_fla_tenure, -- E_FixedTenure
     -- finalchurnflag
     -- fixedchurnflag
     -- waterfall_flag
     count(distinct fix_s_att_account) as Total_Accounts,
     count(distinct fix_s_att_account) as Fixed_Accounts, 
     sum(number_tickets) as number_tickets
FROM final_fields
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

-- ### ### ### ### Specific numbers

--- ### KPI calculation
-- SELECT
--     sum(number_tickets) as number_tickets,
--     count(distinct fix_s_att_account) as active_base, 
--     round(cast(sum(number_tickets) as double)/(cast(count(distinct fix_s_att_account) as double)/100), 2) as tickets_per_100_users
-- FROM final_fields

--- ### Interactions categories
-- SELECT
-- distinct interaction_purpose_descrip, 
-- other_interaction_info10, 
-- count(distinct interaction_id)
-- FROM interactions_fields
-- GROUP BY 1, 2
-- ORDER BY 1, 2, 3 desc
