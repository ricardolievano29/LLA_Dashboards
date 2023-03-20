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
-- WHERE account_id = '8211990051973527' --- Just for experiments
)

--- ### Tickets per month

, users_tickets as (
SELECT
    distinct account_id, 
    interaction_id, 
    interaction_date, 
    interaction_purpose_descrip,
    case when interaction_purpose_descrip in ('Outages', 'Voice Outages', 'Fiber Outages') then 'outages' else 'not outages' end as outages,
    case when (
        lower(interaction_purpose_descrip) like '%hsd%' 
        or lower(interaction_purpose_descrip) like '%eq:%'
        or lower(interaction_purpose_descrip) like '%%g:%'
        or lower(interaction_purpose_descrip) like '%serv%'
        or lower(interaction_purpose_descrip) like '%phone%'
        or lower(interaction_purpose_descrip) like '%wifi%'
        or lower(interaction_purpose_descrip) like '%video%'
        or lower(interaction_purpose_descrip) like '%liberty%'
        or lower(interaction_purpose_descrip) like '%ppv%'
        or lower(interaction_purpose_descrip) like '%solic%'
        ) then interaction_id else null
    end as techticket_flag
    -- case when interaction_purpose_descrip in ( 'Adjustment Request', 'Approved Adjustment', 'Billing', 'Cancelled Np', 'Chuito Retained', 'Ci: Bil.Orientation', 'Ci: Cycle Change', 'Ci: Direct Debit', 'Ci: Ebill/Epay', 'Ci: Obj. Late Charge', 'Ci: Obj. Orientation', 'Ci: Obj. Rec Charge', 'Ci: Offer Correct', 'Ci: Offer Oriented', 'Ci: PPV/Vod', 'Ci: Reconnect Charge', 'Ci: Recurrent Charge', 'Ci: Red Flags', 'Ci: Refund Stat', 'Ci: Req.  Adjustment', 'Ci: Req. Adjustment', 'Ci: Req. Paym Inv', 'Ci: Req. Payment Inv', 'Ci: Req. Refund', 'Ci: Stolen Identity', 'Ci: Transworld Iss.', 'Cl: Balance Letter', 'E-Bill', 'G:customer Billable', 'Not Retained', 'Np: Cancelled Np', 'Np: Payment Plan', 'Np: Promise To Pay', 'Payment', 'Payment Ath', 'Payment Cash', 'Payment Credit Card', 'Payment Eft', 'Payment Plan', 'Promise To Pay', 'Ret- Adjustment','Ret- Promise-To-Pay', 'Ret-Bill Expln', 'Ret-Direct Debit', 'Ret-Pay Meth Expln', 'Ret-Payment', 'Ret-Right Pricing', 'Rt: Price Increase', 'Rt: Rate Pricing', 'Vd: Billing', 'Vd: Cant Afford', 'Vd: Closed Buss', 'Vd: Deceased') then interaction_id else null end as billing, 
    -- case when interaction_purpose_descrip in ('Acp Completada', 'Acp Incompleta', 'Act Num Serie Equipo', 'Admin Interaction', 'Audit', 'Aut/Remocion De Pers', 'Aut/Remocion Pers', 'Cambios En Cpni', 'Cmts', 'Cust Service Calls', 'Customer Billable', 'Default Call Wrapup', 'Disconnect Service', 'Downgrade', 'Downgrade Service', 'Dta Lost Equipment', 'Dta Made Return', 'Dta Made Swap', 'Dta No Quiere Hub TV', 'Dta No Quiere Swap', 'Ebb Orientacion', 'Ebb Status', 'Email Update', 'Eq: Audit', 'Eq: Code Error', 'Eq: Cust. First', 'Focal Group', 'G:outbound Calls', 'Inbound Sales', 'Info Acp Nuevo', 'Office Info', 'Outbound Sales Offer', 'PPV', 'Product/Offer Inform', 'Ret- Discount', 'Ret- Down Serv', 'Ret- Lib Espa??ol', 'Ret- Lib Espaol', 'Rt: Dowgrde Service', 'Rt: Orientaci??n', 'Rt: Orientacin', 'Rt: Seasonal', 'Rt: Sidegrde Same', 'Rt: Upgrade', 'Sale', 'Sales', 'Sp:tel-Feat. Config.', 'Sp:tel. - Hunting', 'Speed Upgrade', 'Transicion Ebb A Acp', 'Unsupported Hardware', 'Update Dir Postal', 'Update Telefonos', 'Upgrade', 'Upgrade/Svc Order', 'Vd: Competition', 'Vd: Cust Service', 'Vd: Low Usage', 'Vd: Mov. Active Act.', 'Vd: Mov. No Facility', 'Vd: Mov. Out Coverag', 'Vd: Vol/Dissatified', 'Vd:voluntary Disco', 'Vod', 'Voice Feature Issues', 'Voice Mail', 'Voluntary Disconnect') then interaction_id else null end as no_technical, 
    -- case when interaction_purpose_descrip in ('Antivirus Calls', 'Bloq Red Wifi', 'Bloqueo Red Wifi', 'Cable Card Install', 'Cable Problem', 'Call Fwd Activaci??n', 'Call Fwd Activacin', 'Call Fwd Activacion', 'Call Fwd Desactivar', 'Cambio Ssid', 'Ci: Cable Card Req', 'Commercial Accounts', 'Configuration', 'Cs: Change Name(fco)', 'Cs: Offer Correction', 'Cs: Portability', 'Email Issues', 'Eq: Deprogrammed', 'Eq: Intermittence', 'Eq: Lost', 'Eq: Lost/Stolen', 'Eq: No Conection', 'Eq: No Start', 'Eq: Not Working', 'Eq: Notif Letter', 'Eq: Pixels', 'Eq: Ref. By Csr', 'Eq: Replace Remote', 'Eq: Return Equip.', 'Eq: Up/Dwn/Side', 'Eq: Up/Side', 'Fast Busy Tone', 'Functions Oriented', 'G:disconect Warning', 'G:follow Up', 'G:order Entry Error', 'Headend Issues', 'Headend Power Outage', 'HSD Intermittent', 'HSD Issues', 'HSD No Browsing', 'HSD Problem', 'HSD Slow Service', 'Ibbs-Ip Issues', 'Integra 5', 'Internet Calls', 'Lnp Complete', 'Lnp In Process', 'Lnp Not Complete', 'Mcafee', 'Mi Liberty Problem', 'Nc Busy Tone', 'Nc Cancel Job', 'Nc Message V. Mail', 'Nc No Answer', 'Nc Ok Cust Confirmed', 'Nc Rescheduled', 'Nc Wrong Phone No.', 'No Browse', 'No Retent Relate', 'No Retention Call', 'No Service All', 'Non Pay', 'Np: Restard Svc Only', 'Nret- Contract', 'Nret- Diss Cust Serv', 'Nret- Equipment', 'Nret- Moving', 'Nret- No Facilities', 'Outages', 'Phone Cant Make Call', 'Phone Cant Recv Call', 'Phone No Tone', 'PPV Order', 'PPV/Vod Problem', 'Reconnection', 'Refered Same Day', 'Restard Svc Only', 'Restart 68-69 Days', 'Restart Svc Only', 'Ret-Serv Education', 'Ret-Sidegrade', 'Ret-Upgrade', 'Retained Customer', 'Retent Effort Call', 'Retention', 'Return Mail', 'Rt: Dowgrde Convert', 'Rt: Dowgrde Premium', 'Schd Appiont 4 Tech', 'Self-Inst Successful', 'Self-Install', 'Self-Install N/A', 'Self-Int Rejected', 'Service Techs', 'Sl: Advanced Prod.', 'Sl: Install/Cos Conf', 'Sl: Outbound Sale', 'Sl: Product Info', 'Sl: Restart', 'Sl: Upg Addon/Tiers', 'Sl: Upg Events', 'Sl: Upg Service', 'Sl: Upgrade Tn','Sol Contrasena Wifi', 'Solicitud Contrasena', 'Solicitud Num Cuent', 'Solicitud Num Cuenta', 'Sp: Aee-No Liberty', 'Vd: Tech. Service', 'Video Issues', 'Video Problem', 'Video Programming', 'Voice Issues', 'Voice Outages', 'Wifi Password', 'Work Order Status') then interaction_id else null end as technical, 
    -- case when interaction_purpose_descrip in ('Ci: Inst/Tc Status', 'Ci: Install Stat', 'Ci: Installer / Tech', 'Create Trouble Call', 'Cs: Transfer', 'Dialtone/Line Issues', 'Eq: Not Recording', 'Eq: Port Damage', 'Eq: Ref By Tech/Inst', 'Eq: Vod No Access', 'Eq:error E1:26 E1:36', 'Equipment Problem', 'Equipment Swap', 'Fiber Outages', 'Maintenance Techs', 'Provision/Contractor', 'Sl: New Sales', 'Sl: Upgrade HSD A/O', 'Sp: Already Had Tc', 'Sp: Cancelled Tc', 'Sp: Drops Issues', 'Sp: HSD-Intermit.', 'Sp: HSD-No Browse', 'Sp: HSD-No Connect', 'Sp: HSD-Speed Issues', 'Sp: No Signal-3 Pack', 'Sp: Pending Mr-Sro', 'Sp: Poste Ca?-do', 'Sp: Poste Cado', 'Sp: PPV', 'Sp: Precortes Issues', 'Sp: Recent Install', 'Sp: Referred To Noc', 'Sp: Tel-Cant Make', 'Sp: Tel-Cant Receive', 'Sp: Tel-No Tone', 'Sp: Video-Intermit.', 'Sp: Video-No Signal', 'Sp: Video-Tiling', 'Sp: Vod', 'Sp:hsd- Ip Issues', 'Sp:hsd- Wifi Issues', 'Sp:tc/Mr Confirm', 'Sp:tel- Voice Mail', 'Status Of Install', 'Status/Trouble Calls', 'Tel Issues', 'Tel Problem', 'Telephony Calls','Transfer', 'Vd: Transferred') then interaction_id else null end as truckrolls, 
    -- case when interaction_purpose_descrip in ('Wowfi Ext Guidance', 'Wowfi Ext Issues', 'Acp Downgraded Cust', 'Dmca Orientation', 'Gamer Call', 'Abuse Calls') then interaction_id else null end as others, 
    -- case when interaction_purpose_descrip = 'Work Order Status' then interaction_id else null end as not_relevant
FROM interactions_fields
WHERE 
    interaction_purpose_descrip not in ('Work Order Status', 'Default Call Wrapup', 'G:outbound Calls', 'Eq: Cust. First', 'Eq: Audit', 'Eq: Code Error', 'Downgrade Service', 'Disconnect Service', 'Rt: Dowgrde Service', 'Cust Service Calls')
    -- and interaction_purpose_descrip in ( --- There may be a problem identifying tickets with interaction_purpose_descrip -> Outages considerations
    --- Tech tickets
    -- 'Antivirus Calls', 'Bloq Red Wifi', 'Bloqueo Red Wifi', 'Cable Card Install', 'Cable Problem', 'Call Fwd Activaci??n', 'Call Fwd Activacin', 'Call Fwd Activacion', 'Call Fwd Desactivar', 'Cambio Ssid', 'Ci: Cable Card Req', 'Commercial Accounts', 'Configuration', 'Cs: Change Name(fco)', 'Cs: Offer Correction', 'Cs: Portability', 'Email Issues', 'Eq: Deprogrammed', 'Eq: Intermittence', 'Eq: Lost', 'Eq: Lost/Stolen', 'Eq: No Conection', 'Eq: No Start', 'Eq: Not Working', 'Eq: Notif Letter', 'Eq: Pixels', 'Eq: Ref. By Csr', 'Eq: Replace Remote', 'Eq: Return Equip.', 'Eq: Up/Dwn/Side', 'Eq: Up/Side', 'Fast Busy Tone', 'Functions Oriented', 'G:disconect Warning', 'G:follow Up', 'G:order Entry Error', 'Headend Issues', 'Headend Power Outage', 'HSD Intermittent', 'HSD Issues', 'HSD No Browsing', 'HSD Problem', 'HSD Slow Service', 'Ibbs-Ip Issues', 'Integra 5', 'Internet Calls', 'Lnp Complete', 'Lnp In Process', 'Lnp Not Complete', 'Mcafee', 'Mi Liberty Problem', 'Nc Busy Tone', 'Nc Cancel Job', 'Nc Message V. Mail', 'Nc No Answer', 'Nc Ok Cust Confirmed', 'Nc Rescheduled', 'Nc Wrong Phone No.', 'No Browse', 'No Retent Relate', 'No Retention Call', 'No Service All', 'Non Pay', 'Np: Restard Svc Only', 'Nret- Contract', 'Nret- Diss Cust Serv', 'Nret- Equipment', 'Nret- Moving', 'Nret- No Facilities', 'Outages', 'Phone Cant Make Call', 'Phone Cant Recv Call', 'Phone No Tone', 'PPV Order', 'PPV/Vod Problem', 'Reconnection', 'Refered Same Day', 'Restard Svc Only', 'Restart 68-69 Days', 'Restart Svc Only', 'Ret-Serv Education', 'Ret-Sidegrade', 'Ret-Upgrade', 'Retained Customer', 'Retent Effort Call', 'Retention', 'Return Mail', 'Rt: Dowgrde Convert', 'Rt: Dowgrde Premium', 'Schd Appiont 4 Tech', 'Self-Inst Successful', 'Self-Install', 'Self-Install N/A', 'Self-Int Rejected', 'Service Techs', 'Sl: Advanced Prod.', 'Sl: Install/Cos Conf', 'Sl: Outbound Sale', 'Sl: Product Info', 'Sl: Restart', 'Sl: Upg Addon/Tiers', 'Sl: Upg Events', 'Sl: Upg Service', 'Sl: Upgrade Tn', 'Sol Contrasena Wifi', 'Solicitud Contrasena', 'Solicitud Num Cuent', 'Solicitud Num Cuenta', 'Sp: Aee-No Liberty', 'Vd: Tech.Service', 'Video Issues', 'Video Problem', 'Video Programming', 'Voice Issues', 'Voice Outages', 'Wifi Password', 
        -- 'Work Order Status', 
    --- Now truckrolls
        -- 'Ci: Inst/Tc Status', 'Ci: Install Stat', 'Ci: Installer / Tech', 'Create Trouble Call', 'Cs: Transfer', 'Dialtone/Line Issues', 'Eq: Not Recording', 'Eq: Port Damage', 'Eq: Ref By Tech/Inst', 'Eq: Vod No Access', 'Eq:error E1:26 E1:36', 'Equipment Problem', 'Equipment Swap', 'Fiber Outages', 'Maintenance Techs', 'Provision/Contractor', 'Sl: New Sales', 'Sl: Upgrade HSD A/O', 'Sp: Already Had Tc', 'Sp: Cancelled Tc', 'Sp: Drops Issues', 'Sp: HSD-Intermit.', 'Sp: HSD-No Browse', 'Sp: HSD-No Connect', 'Sp: HSD-Speed Issues', 'Sp: No Signal-3 Pack', 'Sp: Pending Mr-Sro', 'Sp: Poste Ca?-do', 'Sp: Poste Cado', 'Sp: PPV', 'Sp: Precortes Issues', 'Sp: Recent Install', 'Sp: Referred To Noc', 'Sp: Tel-Cant Make', 'Sp: Tel-Cant Receive', 'Sp: Tel-No Tone', 'Sp: Video-Intermit.', 'Sp: Video-No Signal', 'Sp: Video-Tiling', 'Sp: Vod', 'Sp:hsd- Ip Issues', 'Sp:hsd- Wifi Issues', 'Sp:tc/Mr Confirm', 'Sp:tel- Voice Mail', 'Status Of Install', 'Status/Trouble Calls', 'Tel Issues', 'Tel Problem', 'Telephony Calls', 'Transfer', 'Vd: Transferred'
        -- )
)

, tickets_per_month as (
SELECT
    date_trunc('month', interaction_date) as month, 
    account_id, 
    count(distinct techticket_flag) as number_tickets
    -- count(distinct interaction_id) as num_interactions,
    -- count(distinct billing) as num_billing, 
    -- count(distinct no_technical) as num_no_technical, 
    -- count(distinct technical) as num_technical,
    -- count(distinct truckrolls) as num_truckrolls, 
    -- count(distinct others) as num_others,
    -- count(distinct not_relevant) as num_not_relevant,
    -- count(distinct case when (technical is not null or truckrolls is not null) and interaction_purpose_descrip != 'Work Order Status' then interaction_id else null end) as num_relevant_tickets
    -- count(interaction_date) as number_tickets
FROM users_tickets
WHERE interaction_id is not null
GROUP BY 1, 2
)

--- ### Tickets per month flag (number of tickets)

, number_tickets_flag as (
SELECT
    F.*, 
    -- num_interactions,
    -- num_billing, 
    -- num_no_technical, 
    -- num_technical, 
    -- num_truckrolls, 
    -- num_others, 
    -- num_not_relevant,
    -- num_relevant_tickets
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
    -- num_interactions,
    -- num_billing, 
    -- num_no_technical, 
    -- num_technical, 
    -- num_truckrolls, 
    -- num_others, 
    -- num_not_relevant,
    -- num_relevant_tickets
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
    --  outages,
     count(distinct fix_s_att_account) as Total_Accounts,
     count(distinct fix_s_att_account) as Fixed_Accounts, 
     sum(number_tickets) as number_tickets
     
    --  sum(num_interactions) as num_interactions, 
    --  sum(num_billing) as num_billing,  
    --  sum(num_no_technical) as num_no_technical, 
    --  sum(num_technical) as num_technical, 
    --  sum(num_truckrolls) as num_truckrolls, 
    --  sum(num_others) as num_others, 
    --  sum(num_not_relevant) as num_not_relevant,
    --  sum(num_relevant_tickets) as num_relevant_tickets
    
FROM final_fields
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

-- ### ### ### ### Specific numbers

--- ### Number of clients per tickets
-- SELECT distinct number_tickets, count(distinct fix_s_att_account) FROM final_fields GROUP BY 1 ORDER BY 1 asc, 2 desc

--- ### Total tickets segmented by outages
-- SELECT outages, sum(number_tickets) FROM final_fields GROUP BY 1 --- Just 7 of the tickets are clasified as outages-related

--- ### Top 5 clients with most tickets
-- SELECT fix_s_att_account, number_tickets FROM final_fields ORDER BY 2 desc LIMIT 5

--- ### KPI calculation
-- SELECT 
--     -- sum(num_relevant_tickets) as num_relevant_tickets, 
--     sum(number_tickets) as number_tickets,
--     count(distinct fix_s_att_account) as active_base, 
--     round(cast(sum(number_tickets) as double)/(cast(count(distinct fix_s_att_account) as double)/100), 2) as tickets_per_100_users
-- FROM final_fields

--- ### Ticket Equation
-- SELECT
--      sum(num_interactions), 
--      sum(num_billing) as num_billing,  
--      sum(num_no_technical) as num_no_technical, 
--      sum(num_technical) as num_technical, 
--      sum(num_truckrolls) as num_truckrolls, 
--      sum(num_others) as num_others, 
--      (sum(num_technical) + sum(num_truckrolls)) as num_raw_tickets,
--      sum(num_not_relevant) as num_not_relevant,
--      sum(num_relevant_tickets) as num_relevant_tickets
-- FROM final_fields

--- ### Interactions categories
-- SELECT
-- distinct interaction_purpose_descrip, 
-- other_interaction_info10, 
-- count(distinct interaction_id)
-- FROM interactions_fields
-- GROUP BY 1, 2
-- ORDER BY 1, 2, 3 desc
