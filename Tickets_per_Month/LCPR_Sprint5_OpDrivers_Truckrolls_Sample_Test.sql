WITH

truckrolls_sample as (
SELECT * FROM "lcpr.stage.dev"."truckrolls_sample"
)

, clean_interaction_time as (
SELECT *
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    (cast(interaction_start_time as varchar) != ' ') 
    and (interaction_start_time is not null)
    and date_trunc('month', cast(substr(cast(interaction_start_time as varchar),1,10) as date)) = date('2023-01-01') --- The sample is just available for January
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
    case when interaction_purpose_descrip in ('Outages', 'Voice Outages', 'Fiber Outages') then 'outages' else 'not outages' end as outages
FROM interactions_fields
WHERE 
    interaction_purpose_descrip != 'Default Call Wrapup'
    and interaction_purpose_descrip != 'Work Order Status'
    and interaction_purpose_descrip in ( --- Just truckrolls-related purposes for comparing
        'Ci: Inst/Tc Status', 'Ci: Install Stat', 'Ci: Installer / Tech', 'Create Trouble Call', 'Cs: Transfer', 'Dialtone/Line Issues', 'Eq: Not Recording', 'Eq: Port Damage', 'Eq: Ref By Tech/Inst', 'Eq: Vod No Access', 'Eq:error E1:26 E1:36', 'Equipment Problem', 'Equipment Swap', 'Fiber Outages', 'Maintenance Techs', 'Provision/Contractor', 'Sl: New Sales', 'Sl: Upgrade HSD A/O', 'Sp: Already Had Tc', 'Sp: Cancelled Tc', 'Sp: Drops Issues', 'Sp: HSD-Intermit.', 'Sp: HSD-No Browse', 'Sp: HSD-No Connect', 'Sp: HSD-Speed Issues', 'Sp: No Signal-3 Pack', 'Sp: Pending Mr-Sro', 'Sp: Poste Ca?-do', 'Sp: Poste Cado', 'Sp: PPV', 'Sp: Precortes Issues', 'Sp: Recent Install', 'Sp: Referred To Noc', 'Sp: Tel-Cant Make', 'Sp: Tel-Cant Receive', 'Sp: Tel-No Tone', 'Sp: Video-Intermit.', 'Sp: Video-No Signal', 'Sp: Video-Tiling', 'Sp: Vod', 'Sp:hsd- Ip Issues', 'Sp:hsd- Wifi Issues', 'Sp:tc/Mr Confirm', 'Sp:tel- Voice Mail', 'Status Of Install', 'Status/Trouble Calls', 'Tel Issues', 'Tel Problem', 'Telephony Calls', 'Transfer', 'Vd: Transferred'
        
        --- Technical issues (just for comparing)
        , 'Antivirus Calls', 'Bloq Red Wifi', 'Bloqueo Red Wifi', 'Cable Card Install', 'Cable Problem', 'Call Fwd Activaci??n', 'Call Fwd Activacin', 'Call Fwd Activacion', 'Call Fwd Desactivar', 'Cambio Ssid', 'Ci: Cable Card Req', 'Commercial Accounts', 'Configuration', 'Cs: Change Name(fco)', 'Cs: Offer Correction', 'Cs: Portability', 'Email Issues', 'Eq: Deprogrammed', 'Eq: Intermittence', 'Eq: Lost', 'Eq: Lost/Stolen', 'Eq: No Conection', 'Eq: No Start', 'Eq: Not Working', 'Eq: Notif Letter', 'Eq: Pixels', 'Eq: Ref. By Csr', 'Eq: Replace Remote', 'Eq: Return Equip.', 'Eq: Up/Dwn/Side', 'Eq: Up/Side', 'Fast Busy Tone', 'Functions Oriented', 'G:disconect Warning', 'G:follow Up', 'G:order Entry Error', 'Headend Issues', 'Headend Power Outage', 'HSD Intermittent', 'HSD Issues', 'HSD No Browsing', 'HSD Problem', 'HSD Slow Service', 'Ibbs-Ip Issues', 'Integra 5', 'Internet Calls', 'Lnp Complete', 'Lnp In Process', 'Lnp Not Complete', 'Mcafee', 'Mi Liberty Problem', 'Nc Busy Tone', 'Nc Cancel Job', 'Nc Message V. Mail', 'Nc No Answer', 'Nc Ok Cust Confirmed', 'Nc Rescheduled', 'Nc Wrong Phone No.', 'No Browse', 'No Retent Relate', 'No Retention Call', 'No Service All', 'Non Pay', 'Np: Restard Svc Only', 'Nret- Contract', 'Nret- Diss Cust Serv', 'Nret- Equipment', 'Nret- Moving', 'Nret- No Facilities', 'Outages', 'Phone Cant Make Call', 'Phone Cant Recv Call', 'Phone No Tone', 'PPV Order', 'PPV/Vod Problem', 'Reconnection', 'Refered Same Day', 'Restard Svc Only', 'Restart 68-69 Days', 'Restart Svc Only', 'Ret-Serv Education', 'Ret-Sidegrade', 'Ret-Upgrade', 'Retained Customer', 'Retent Effort Call', 'Retention', 'Return Mail', 'Rt: Dowgrde Convert', 'Rt: Dowgrde Premium', 'Schd Appiont 4 Tech', 'Self-Inst Successful', 'Self-Install', 'Self-Install N/A', 'Self-Int Rejected', 'Service Techs', 'Sl: Advanced Prod.', 'Sl: Install/Cos Conf', 'Sl: Outbound Sale', 'Sl: Product Info', 'Sl: Restart', 'Sl: Upg Addon/Tiers', 'Sl: Upg Events', 'Sl: Upg Service', 'Sl: Upgrade Tn', 'Sol Contrasena Wifi', 'Solicitud Contrasena', 'Solicitud Num Cuent', 'Solicitud Num Cuenta', 'Sp: Aee-No Liberty', 'Vd: Tech.Service', 'Video Issues', 'Video Problem', 'Video Programming', 'Voice Issues', 'Voice Outages', 'Wifi Password'
        )
)

SELECT 
    -- account_id, 
    -- order_no_ojb,
    -- interaction_id, 
    -- interaction_date, 
    -- create_dte_ojb, 
    -- interaction_purpose_descrip, 
    -- ord_rsn_otr, 
    -- job_type_description 
    
    count(distinct account_id), 
    count(distinct order_no_ojb)
    
FROM users_tickets a 
-- FULL OUTER JOIN truckrolls_sample b 
INNER JOIN truckrolls_sample b
    ON a.account_id = b.sub_acct_no_sbb

--- ### ### FULL OUTER JOIN

--- ### Just taking our truckrolls vs OpCo's truckrolls
--- 493 interactions (truckrolls) vs 39.947 truckrolls

--- ### Taking all of our tickets vs OpCo's truckrolls
--- 59.562 tickets (all) vs 39.947 truckrolls

--- ### ### INNER JOIN

--- ### Just taking our truckrolls vs OpCo's truckrolls
--- 170 interactions (truckrolls) vs 255 truckrolls

--- ### Taking all of our tickets vs OpCo's truckrolls
--- 18.197 tickets (all) vs 23.611 truckrolls
