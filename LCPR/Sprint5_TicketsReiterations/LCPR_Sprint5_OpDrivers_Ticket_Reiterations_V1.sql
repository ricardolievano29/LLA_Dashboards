--- ##### LCPR SPRINT 5  OPERATIONAL DRIVERS - TICKET REITERATIONS #####

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
FROM "db_stage_dev"."lcpr_fixed_table_jan_feb28" --- Keep this updated to the latest version!
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

--- ### Reiterative tickets

, users_tickets as (
SELECT
    distinct account_id, 
    interaction_id, 
    interaction_date
FROM interactions_fields
WHERE
    interaction_purpose_descrip in ( 
    --- First technical (tickets)
    'Antivirus Calls', 'Bloq Red Wifi', 'Bloqueo Red Wifi', 'Cable Card Install', 'Cable Problem', 'Call Fwd Activaci??n', 'Call Fwd Activacin', 'Call Fwd Activacion', 'Call Fwd Desactivar', 'Cambio Ssid', 'Ci: Cable Card Req', 'Commercial Accounts', 'Configuration', 'Cs: Change Name(fco)', 'Cs: Offer Correction', 'Cs: Portability', 'Email Issues', 'Eq: Deprogrammed', 'Eq: Intermittence', 'Eq: Lost', 'Eq: Lost/Stolen', 'Eq: No Conection', 'Eq: No Start', 'Eq: Not Working', 'Eq: Notif Letter', 'Eq: Pixels', 'Eq: Ref. By Csr', 'Eq: Replace Remote', 'Eq: Return Equip.', 'Eq: Up/Dwn/Side', 'Eq: Up/Side', 'Fast Busy Tone', 'Functions Oriented', 'G:disconect Warning', 'G:follow Up', 'G:order Entry Error', 'Headend Issues', 'Headend Power Outage', 'HSD Intermittent', 'HSD Issues', 'HSD No Browsing', 'HSD Problem', 'HSD Slow Service', 'Ibbs-Ip Issues', 'Integra 5', 'Internet Calls', 'Lnp Complete', 'Lnp In Process', 'Lnp Not Complete', 'Mcafee', 'Mi Liberty Problem', 'Nc Busy Tone', 'Nc Cancel Job', 'Nc Message V. Mail', 'Nc No Answer', 'Nc Ok Cust Confirmed', 'Nc Rescheduled', 'Nc Wrong Phone No.', 'No Browse', 'No Retent Relate', 'No Retention Call', 'No Service All', 'Non Pay', 'Np: Restard Svc Only', 'Nret- Contract', 'Nret- Diss Cust Serv', 'Nret- Equipment', 'Nret- Moving', 'Nret- No Facilities', 'Outages', 'Phone Cant Make Call', 'Phone Cant Recv Call', 'Phone No Tone', 'PPV Order', 'PPV/Vod Problem', 'Reconnection', 'Refered Same Day', 'Restard Svc Only', 'Restart 68-69 Days', 'Restart Svc Only', 'Ret-Serv Education', 'Ret-Sidegrade', 'Ret-Upgrade', 'Retained Customer', 'Retent Effort Call', 'Retention', 'Return Mail', 'Rt: Dowgrde Convert', 'Rt: Dowgrde Premium', 'Schd Appiont 4 Tech', 'Self-Inst Successful', 'Self-Install', 'Self-Install N/A', 'Self-Int Rejected', 'Service Techs', 'Sl: Advanced Prod.', 'Sl: Install/Cos Conf', 'Sl: Outbound Sale', 'Sl: Product Info', 'Sl: Restart', 'Sl: Upg Addon/Tiers', 'Sl: Upg Events', 'Sl: Upg Service', 'Sl: Upgrade Tn', 'Sol Contrasena Wifi', 'Solicitud Contrasena', 'Solicitud Num Cuent', 'Solicitud Num Cuenta', 'Sp: Aee-No Liberty', 'Vd: Tech.Service', 'Video Issues', 'Video Problem', 'Video Programming', 'Voice Issues', 'Voice Outages', 'Wifi Password', 'Work Order Status', 
    --- Now truckroll ones
        'Ci: Inst/Tc Status', 'Ci: Install Stat', 'Ci: Installer / Tech', 'Create Trouble Call', 'Cs: Transfer', 'Dialtone/Line Issues', 'Eq: Not Recording', 'Eq: Port Damage', 'Eq: Ref By Tech/Inst', 'Eq: Vod No Access', 'Eq:error E1:26 E1:36', 'Equipment Problem', 'Equipment Swap', 'Fiber Outages', 'Maintenance Techs', 'Provision/Contractor', 'Sl: New Sales', 'Sl: Upgrade HSD A/O', 'Sp: Already Had Tc', 'Sp: Cancelled Tc', 'Sp: Drops Issues', 'Sp: HSD-Intermit.', 'Sp: HSD-No Browse', 'Sp: HSD-No Connect', 'Sp: HSD-Speed Issues', 'Sp: No Signal-3 Pack', 'Sp: Pending Mr-Sro', 'Sp: Poste Ca?-do', 'Sp: Poste Cado', 'Sp: PPV', 'Sp: Precortes Issues', 'Sp: Recent Install', 'Sp: Referred To Noc', 'Sp: Tel-Cant Make', 'Sp: Tel-Cant Receive', 'Sp: Tel-No Tone', 'Sp: Video-Intermit.', 'Sp: Video-No Signal', 'Sp: Video-Tiling', 'Sp: Vod', 'Sp:hsd- Ip Issues', 'Sp:hsd- Wifi Issues', 'Sp:tc/Mr Confirm', 'Sp:tel- Voice Mail', 'Status Of Install', 'Status/Trouble Calls', 'Tel Issues', 'Tel Problem', 'Telephony Calls', 'Transfer', 'Vd: Transferred')
)

, last_ticket as (
SELECT 
    account_id as last_account, 
    first_value(interaction_date) over(partition by account_id, date_trunc('month', interaction_date) order by interaction_date desc) as last_interaction_date
FROM users_tickets
)

, join_last_ticket as (
SELECT
    account_id, 
    interaction_id, 
    interaction_date, 
    date_trunc('month', last_interaction_date) as interaction_month, 
    last_interaction_date, 
    date_add('day', -60, last_interaction_date) as window_day
FROM users_tickets W
INNER JOIN last_ticket L
    ON W.account_id = L.last_account)

, tickets_count as (
SELECT 
    interaction_month, 
    account_id, 
    count(distinct interaction_id) as tickets
FROM join_last_ticket
WHERE interaction_date between window_day and last_interaction_date
GROUP BY 1, 2
)

, tickets_tier as (
SELECT 
    *,
    case
        when tickets = 1 then '1'
        when tickets = 2 then '2'
        when tickets >= 3 then '>3'
    else null end as ticket_tier
FROM tickets_count
)

, tickets_per_month as (
SELECT
    date_trunc('month', interaction_date) as month, 
    account_id, 
    count(interaction_date) as number_tickets
FROM users_tickets
WHERE interaction_id is not null
GROUP BY 1, 2
)

--- ### Reiterative tickets flag

, ticket_tier_flag as (
SELECT 
    F.*, 
    case when I.account_id is not null then fix_s_att_account else null end as tickets, --- replace finalaccount (when available) instead of fix_s_att_account
    ticket_tier
FROM fmc_table_adj F
LEFT JOIN tickets_tier I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fix_s_dim_month = I.interaction_month
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
    -- fixedchurnflag
    fix_s_fla_churntype, -- fixedchurntype
    fix_s_fla_mainmovement, -- fixedmainmovement
    -- waterfall_flag
    -- mobile_activeeom
    -- mobilechurnflag
    ticket_tier,
   -- finalaccount
    fix_s_att_account, -- fixedaccount
    tickets,
    records_per_user
FROM ticket_tier_flag
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
    fix_s_fla_churntype, -- fixedchurntype
    fix_s_fla_mainmovement, -- fixedmainmovement
    -- waterfall_flag
    -- mobile_activeeom
    -- mobilechurnflag
    ticket_tier,
    count(distinct fix_s_att_account) as Total_Accounts,
    count(distinct fix_s_att_account) as Fixed_Accounts, 
    count(distinct tickets) as Userstickets
FROM final_fields
-- WHERE ((fix_s_fla_churntype != '2. Fixed Involuntary Churner' and fix_s_fla_churntype != '1. Fixed Voluntary Churner') or fix_s_fla_churntype is null) and fix_s_fla_churntype != 'Fixed Churner'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12


--- ### Specific numbers

-- SELECT
--   count(distinct fix_s_att_account) as num_clients
-- FROM final_fields
-- WHERE ticket_tier = '1'

--- ### Duplicates validation

-- SELECT count(distinct fix_s_att_account) as num_clients FROM "db_stage_dev"."lcpr_fixed_table_jan_feb23"
-- --- Result: 513,317

-- --- Number of different accounts recorded
-- SELECT count(distinct fix_s_att_account) as num_clients FROM (SELECT first_value(fix_s_att_account) over (partition by fix_s_att_account) as fix_s_att_account FROM "db_stage_dev"."lcpr_fixed_table_jan_feb23")
-- --- Result: 513,317


-- --- Number of interactions recorded
-- SELECT count(distinct interaction_id) as num_records FROM "lcpr.stage.prod"."lcpr_interactions_csg"
-- --- Result: 984,858

-- --- Number of different interactions recorded
-- SELECT count(distinct interaction_id) as num_records FROM (SELECT first_value(interaction_id) over (partition by interaction_id) as interaction_id FROM "lcpr.stage.prod"."lcpr_interactions_csg")
-- --- Result: 984,858
-- --- Number of different interactions recorded
-- SELECT count(distinct interaction_id) as num_records FROM (SELECT first_value(interaction_id) over (partition by interaction_id) as interaction_id FROM "lcpr.stage.prod"."lcpr_interactions_csg")
-- --- Result: 984,858
