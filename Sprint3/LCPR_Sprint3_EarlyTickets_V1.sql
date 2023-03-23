with parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT DATE_TRUNC('month',DATE('2023-01-01')) AS input_month
)
---New customer directly from the DNA
,new_customers_pre as (
SELECT (CAST(CAST(first_value(connect_dte_sbb) over (PARTITION BY sub_acct_no_sbb order by DATE(dt) DESC) AS TIMESTAMP) AS DATE)) as fix_b_att_maxstart,   
    SUB_ACCT_NO_SBB as fix_s_att_account FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" WHERE play_type <> '0P'
    AND cust_typ_sbb = 'RES' and delinquency_days = 0 having date_trunc('month',date(CONNECT_DTE_SBB)) 
    -- > date('2022-10-01') 
    = (select input_month from parameters) 
    order by 1)
    
 ,new_customer as (   select date_trunc('month', fix_b_att_maxstart) as install_month,fix_b_att_maxstart,  fix_s_att_account from new_customers_pre )

,interactions as (
select customer_id,interaction_start_time,date_trunc('month',date(interaction_start_time)) as interaction_start_month from "lcpr.stage.prod"."lcpr_interactions_csg" where interaction_purpose_descrip in ( 'Antivirus Calls', 'Bloq Red Wifi', 'Bloqueo Red Wifi', 'Cable Card Install', 'Cable Problem', 'Call Fwd Activaci??n', 'Call Fwd Activacin', 'Call Fwd Activacion', 'Call Fwd Desactivar', 'Cambio Ssid', 'Ci: Cable Card Req', 'Commercial Accounts', 'Configuration', 'Cs: Change Name(fco)', 'Cs: Offer Correction', 'Cs: Portability', 'Email Issues', 'Eq: Deprogrammed', 'Eq: Intermittence', 'Eq: Lost', 'Eq: Lost/Stolen', 'Eq: No Conection', 'Eq: No Start', 'Eq: Not Working', 'Eq: Notif Letter', 'Eq: Pixels', 'Eq: Ref. By Csr', 'Eq: Replace Remote', 'Eq: Return Equip.', 'Eq: Up/Dwn/Side', 'Eq: Up/Side', 'Fast Busy Tone', 'Functions Oriented', 'G:disconect Warning', 'G:follow Up', 'G:order Entry Error', 'Headend Issues', 'Headend Power Outage', 'HSD Intermittent', 'HSD Issues', 'HSD No Browsing', 'HSD Problem', 'HSD Slow Service', 'Ibbs-Ip Issues', 'Integra 5', 'Internet Calls', 'Lnp Complete', 'Lnp In Process', 'Lnp Not Complete', 'Mcafee', 'Mi Liberty Problem', 'Nc Busy Tone', 'Nc Cancel Job', 'Nc Message V. Mail', 'Nc No Answer', 'Nc Ok Cust Confirmed', 'Nc Rescheduled', 'Nc Wrong Phone No.', 'No Browse', 'No Retent Relate', 'No Retention Call', 'No Service All', 'Non Pay', 'Np: Restard Svc Only', 'Nret- Contract', 'Nret- Diss Cust Serv', 'Nret- Equipment', 'Nret- Moving', 'Nret- No Facilities', 'Outages', 'Phone Cant Make Call', 'Phone Cant Recv Call', 'Phone No Tone', 'PPV Order', 'PPV/Vod Problem', 'Reconnection', 'Refered Same Day', 'Restard Svc Only', 'Restart 68-69 Days', 'Restart Svc Only', 'Ret-Serv Education', 'Ret-Sidegrade', 'Ret-Upgrade', 'Retained Customer', 'Retent Effort Call', 'Retention', 'Return Mail', 'Rt: Dowgrde Convert', 'Rt: Dowgrde Premium', 'Schd Appiont 4 Tech', 'Self-Inst Successful', 'Self-Install', 'Self-Install N/A', 'Self-Int Rejected', 'Service Techs', 'Sl: Advanced Prod.', 'Sl: Install/Cos Conf', 'Sl: Outbound Sale', 'Sl: Product Info', 'Sl: Restart', 'Sl: Upg Addon/Tiers', 'Sl: Upg Events', 'Sl: Upg Service', 'Sl: Upgrade Tn', 'Sol Contrasena Wifi', 'Solicitud Contrasena', 'Solicitud Num Cuent', 'Solicitud Num Cuenta', 'Sp: Aee-No Liberty', 'Vd: Tech.Service', 'Video Issues', 'Video Problem', 'Video Programming', 'Voice Issues', 'Voice Outages', 'Wifi Password', 'Work Order Status') and date_trunc('month',date(interaction_start_time)) between (select input_month from parameters) and (select input_month + interval '2' month from parameters)
)

,new_customer_interactions_info AS (
SELECT fix_s_att_account,install_month,interaction_start_month,fix_b_att_maxstart,
        case when DATE_DIFF('week',CAST(fix_b_att_maxstart AS DATE),CAST(interaction_start_time AS DATE)) <= 7 then fix_s_att_account else null end as early_interaction_flag
FROM new_customer A LEFT JOIN interactions B ON A.fix_s_att_account = cast(B.customer_id as bigint) 
    --AND interaction_type = 'Technical'
-- GROUP BY 1,2,3
)
select date_add('month', 0,install_month), count(distinct early_interaction_flag) as opd_s_mes_uni_early_tickets
,count(distinct fix_s_att_account) as fixed_Account from new_customer_interactions_info group by 1 order by 1
