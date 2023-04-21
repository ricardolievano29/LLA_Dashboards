--Calling the main
WITH FMCTABLE AS(
	SELECT *
	FROM ("lla_cco_int_ana_dev"."cwc_fmc_churn_dev")
	where 
	   -- month = date(dt)
	   month = date('2022-11-01')
)
,
--calling the bases for rgus
--Dissconnection
diss as (
	SELECT account_number as diss_id,
		lower(disconnected_services) as diss_services,
		date_trunc('Month', date_parse(service_end_dt, '%m/%d/%Y')) as mth,
		department as department,
		CASE
			WHEN lower(disconnected_services) = 'Click' THEN 'BO'
			when lower(disconnected_services) = 'watch' THEN 'TV'
			when lower(disconnected_services) = 'Talk' THEN 'VO'
			when lower(disconnected_services) = 'NA' THEN 'NA' --HAY NA
			when lower(disconnected_services) = 'na,click' THEN 'BO' --HAY NA
			when lower(disconnected_services) = 'Watch,click' THEN 'BO+TV'
			when lower(disconnected_services) = 'talk,click' THEN 'BO+VO'
			when lower(disconnected_services) = 'Watch,talk' THEN 'VO+TV'
			when lower(disconnected_services) = 'Watch,talk,click ' THEN 'BO+VO+TV'
			when lower(disconnected_services) = 'talk,mobile,click' THEN 'BO+VO' --HAY MOBILE
		end as dis_mixname
	FROM "lla_cco_int_ext_dev"."cwc_ext_disconnections"
	where disconnected_services <> 'MOBILE'
),


mvm as (
	SELECT cc as ID,
		"completion mth" as mth,
		case
			when play_cl >= play_op then 'total_retention'
			when play_cl < play_op then 'partial_retention'
		end as TypeRet
	 FROM "lla_cco_int_ext_dev"."cwc_ext_retention"
),


final_mvm as (
	select id as mvm_id,
		TypeRet,
		date_parse(mth, '%m/%d/%Y') as mth
	from mvm
)
,
joint_diss as (
	SELECT *
	FROM fmctable t
		left join diss d ON cast(d.diss_id as varchar) = t.fixed_account
		AND d.mth = t.fixed_month
)

,joint_diss_ret as (
	select *
	from joint_diss t
		left join final_mvm d on cast(d.mvm_id as varchar) = t.fixed_account
		AND d.mth = t.fixed_month
)

--Final joint for the rgus
,entire_funnel as (
	select f.*,
	case when mvm_id is not null or diss_id is not null then fixed_account end as intents,
		case
			when mainmovement = '3.Downsell' and finalfixedchurnflag <> 'Voluntary' then fixed_account
			when finalfixedchurnflag = 'Voluntary' then fixed_account
		end as Customers_diss,
		case
			when finalfixedchurnflag = 'Voluntary' then fixed_account
		end as volchurn,
		case
			when mvm_id is not null then mvm_id
			when Department = 'RCOE' then diss_id
		end as retention_isle,
		case
			when diss_id is not null
			or mvm_id is not null then 1 else 0
		end as ext_tbl
	from joint_diss_ret f
	where month = date(dt)
)

,intention_adj as (
	select *,
		case
			when diss_id is not null then diss_id
			when mvm_id is not null then mvm_id
			when finalfixedchurnflag = 'Voluntary' then cast(fixed_account as bigint)
			when finalfixedchurnflag in('Incomplete CST') and (activeeom = 0 or activeeom is null) then cast(fixed_account as bigint)
			when finalfixedchurnflag in('CST Churner') and (mainmovement = '6.Null last day' or mainmovement = '3.Downsell') then cast(fixed_account as bigint)
			
			end as Intention_id
	from entire_funnel
)

--Inserting the different things for the Persons  (flags)
,union_mvm_vol as (
	select a.mvm_ID as mvm_id, date(mth) as mth
	from final_mvm a
	union 
	select b.diss_id, date(b.mth) as mth
	from diss b
),
--final
union_final as (
	select distinct mvm_id,
		mth
	from union_mvm_vol
) 

,
intention_flag as (
	SELECT f.*,
		case
			when o.mvm_id is not null then o.mvm_id else null
		end as intent_flag,
		case
			when o.mvm_id is not null then 1 else 0
		end as intent_category,
		f.month as mth
	FROM intention_adj f
		left join union_final o on f.fixed_account = cast(o.mvm_id as varchar)
		and f.month = o.mth
),
--retention flag and category
retained_table as (
	SELECT f.*,
		case
			when retention_isle is not null then 1 else 0
		end as retention_isle_category,
		case
			when volchurn is not null then 1 else 0
		end as volchurn_category,
		case
			when o.mvm_id is not null
			and finalfixedchurnflag is null then o.mvm_id else null
		end as retained_flag,
		case
			when o.mvm_id is not null then 1 else 0
		end as retained_category
	FROM intention_flag f
		left join final_mvm o on cast(f.fixed_account as bigint) = o.mvm_id
		and f.month = o.mth
),
dxflags as (
	select *,
		case
			when finalfixedchurnflag = 'Voluntary' then 'Voluntary'
		--	when (diss_id is not null or mvm_id is not null)
		--	and (finalfixedchurnflag is not null and intention_id is not null and finalfixedchurnflag <> 'Incomplete CST'
		--		or (finalfixedchurnflag = 'Incomplete CST' and activeeom = 0)
		--	) then 'OtherChurner'
			when finalfixedchurnflag in ('CST Churner','Incomplete CST') then 'CSTChurner'
			
			when intention_id is not null
			and retained_flag is null
			and finalfixedchurnflag is null and activeeom = 1 then 'Bajas_no_cursadas'
		end as dx_type
	from retained_table
)

,num_rgus as (
	select *,
		case
			when mainmovement = '3.Downsell' then b_numrgus - e_numrgus
			when mainmovement = '6.Null last day' then b_numrgus
			when dx_type is not null
			and intention_id is not null then b_numrgus
		end as Num_rgus_diss,
		case
			when mainmovement = '3.Downsell'
			and intention_id is not null then 1
			when dx_type is not null
			and intention_id is not null then 1 else 0
		end as Num_p_diss,
	case when dx_type is not null then 1 else 0 end as Dx_category
	from dxflags
) 

,FinalVolChurnTable as(
select distinct fixed_month,dx_type,
	--Key FMC Flags
	e_final_tech_flag,e_fmc_segment,e_fmctype,e_finaltenuresegment,b_final_tech_flag,b_fmc_segment,
	activebom,b_finaltenuresegment,b_fmctype,
	--RGUs variables
	num_rgus_diss,num_p_diss,b_numrgus,e_numrgus,
	--Categories of the funnel
	intent_category,retention_isle_category,retained_category,volchurn_category,
	--Pair of exceptions
	--"RetainedChurners"
	case
		when retained_flag is not null
		and lower(finalfixedchurnflag) is not null then 1 else 0
	end as Ex_Retained_Churners,
	--"Bajas no cursadas"
	case
		when retention_isle_category = 1
		and retained_category = 1
		and volchurn_category = 1 then 1 else 0
	end as Ex_Bajas_nocursadas,
	--Counts for the funnel
	case
		when retained_flag = 1 then e_numrgus else null
	end as Retained_rgus, 
	count(distinct customers_diss) as customerdiss,count(distinct volchurn) as volchurners,
	count(distinct fixed_account) as activebase,count(distinct retained_flag) as retained,
	count(distinct intent_flag) as intents,count(distinct intention_id) as intention_id_count,
	count(distinct retention_isle) as retention_isle_intents, count(distinct mvm_id) as efective_retention_count,
	count(distinct bb_rgu_bom) as bb_rgus_bom, count(distinct tv_rgu_bom) as tv_rgus_bom, count(distinct vo_rgu_bom) as vo_rgus_bom, count(distinct bb_rgu_eom) as bb_rgus_eom, 
	count(distinct tv_rgu_eom) as tv_rgus_eom, count(distinct vo_rgu_eom) as vo_rgus_eom
from num_rgus
where month = date(dt)
	and waterfall_flag <> 'Fixed Base Exceptions' 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20, 21, 22

order by 1,2,3 
)

-- select *
-- from finalvolchurntable

SELECT 
    sum(intent_category*b_numrgus) as total_intents,
    sum(retention_isle_category*b_numrgus) as intent_cc_rgus,
    sum(retained_category*b_numrgus) as retained_rgus, 
    sum(retention_isle_category*volchurn_category*b_numrgus) as completed_cc_rgus, 
    sum(retention_isle_category*(case
		when retention_isle_category = 1
		and retained_category = 1
		and volchurn_category = 1 then 1 else 0
	end)*b_numrgus) as noncompleted_cc_rgus
    -- sum(b_numrgus)
FROM num_rgus
-- WHERE retention_isle is not null
-- WHERE mvm_id is not null
-- WHERE dx_type = 'CSTChurner'


-- SELECT
--     sum(volchurn_category*b_numrgus)
-- FROM num_rgus
