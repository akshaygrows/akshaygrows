with base as (
    select 
        awb
    ,   pickuptime
    ,   last_mile_hub
	,	warehouse_city
	,	shipping_city
	,	shipping_zone
	,   shipment_status
	,   sort_code
    ,   created_date
    from ops_main
    where shipping_partner = 'Hyperlocal'
--     and awb = 'GS1511464251'
    and created_date::date >= '2023-12-01'
    and pickuptime is not null
)
, all_attempts as (
    select 
        awb
    ,   node_name
	,	shipping_city
    ,   concat(awb,'-zero-attempt') as task_id
    ,   created_date as dispatch_time
	,   0 as attempt_number
	,	case 	when warehouse_city = 'Bangalore' and shipping_city = 'Hyderabad' then 
				(case 	when extract(hour from pickuptime) < 22 then pickuptime::date + interval '1 day' + interval '18 hours' 
						else pickuptime::date + interval '2 day' + interval '18 hours'
						end)
				when shipping_zone = 'Within Zone' then
				(case 	when extract(hour from pickuptime) < 21 then pickuptime::date + interval '1 day' + interval '12 hours'
						else pickuptime::date + interval '2 day' + interval '12 hours'
						end)
				when shipping_zone = 'Metro' then
				(case 	when extract(hour from pickuptime) < 15 then pickuptime::date + interval '1 day' + interval '12 hours'
						else pickuptime::date + interval '2 day' + interval '12 hours'
						end)
				when shipping_zone = 'Intracity' then
				(case 	when extract(hour from pickuptime) < 3 then pickuptime::date + interval '12 hours'
				 		when extract(hour from pickuptime) < 11 then pickuptime::date + interval '18 hours'
						else pickuptime::date + interval '1 day' + interval '12 hours'
						end)
				else pickuptime::date + interval '1 day' + interval '18 hours'
		end as min_reattempt_time
    ,   null as status
    ,   null as cancellation_remarks
    ,   shipment_status
    ,   sort_code
    from base as o
    left join application_db.node as n on o.last_mile_hub = trim(split_part(n.sort_codes,'/',2))
    where node_name <> 'GSCAN TEST'
    
    UNION ALL 
    
    select 
        o.awb
    ,   node_name
	,	shipping_city
    ,   l.task_id
    ,   l.dispatch_time
    ,   rank() over (partition by o.awb order by dispatch_time,task_id) as attempt_number
    ,   case    when status = 'COMPLETED' then null 
            when lower(cancellation_remarks) like '%wrong address%' then dispatch_time::date + interval '2 day' + interval '12 hours'
			when extract(hour from dispatch_time) <14 then dispatch_time::date + interval '1 day' + interval '12 hours'
            else dispatch_time::date + interval '1 day' + interval '18 hours' end as min_reattempt_time
	,   status
	,   cancellation_remarks
	,   shipment_status
	,   l.sort_code
    from base as o 
    inner join locus_task_brief as l on o.awb = l.awb
    left join application_db.node as n on l.location_id = n.locus_home_base_id
    where node_name <> 'GSCAN TEST'
	and dispatch_time is not null
)
,   all_attempts_data as (
    select 
        ct.*
    ,   pt.dispatch_time as previous_trip_time
    ,   nt.dispatch_time as next_trip_time
        
    from all_attempts as ct
    left join all_attempts as pt on ct.awb = pt.awb and ct.attempt_number - 1 = pt.attempt_number
    left join all_attempts as nt on ct.awb = nt.awb and ct.attempt_number + 1 = nt.attempt_number
)
,   ndr_details_data as (
    select 
        awb
    ,   timestamp + interval '5.5 hours' as timestamp
    ,   message
    ,   remarks
	,	ndr_id
    ,   case    
                when message = 'Reattempt Requested' and (deferred_date is null or deferred_date = '') then timestamp::date + interval '1 day' 
                when extract(hour from (timestamp + interval '5.5 hours')) >= 11 and date_trunc('day',timestamp + interval '5.5 hours') = to_date(deferred_date,'YYYY-MM-DD') then to_date(deferred_date,'YYYY-MM-DD') + interval '1 day'
                else to_date(deferred_date,'YYYY-MM-DD') end as deferred_date
    
    ,   case 	when re_attempt_slot in ('Evening','Afternoon','4PM - 10PM') then 'evening' 
				when re_attempt_slot in ('9AM - 4PM','Morning') then 'morning'
				else null end as re_attempt_slot
        
    from application_db.ndr_details
    where shipper_id = 301
    and timestamp::date >= '2023-12-01'
)
, final as (
    select
       awb
    ,   node_name
	,	shipping_city
	,   sort_code
    ,   task_id
    ,   dispatch_time
    ,   attempt_number
    ,   status as task_status
    ,   shipment_status
    ,   cancellation_remarks
    ,   min_reattempt_time
    -- ,   previous_trip_time
    ,   next_trip_time
    -- ,   message
    -- ,   timestamp
    ,   deferred_time
    ,   re_attempt_slot
	,	case 	when attempt_number = 0 and deferred_time is null then min_reattempt_time
				when deferred_time is null then null
				when deferred_time >= min_reattempt_time then deferred_time
				when deferred_time < min_reattempt_time and re_attempt_slot is null then min_reattempt_time
				when deferred_time < min_reattempt_time then deferred_time + date_trunc('day', age(min_reattempt_time,deferred_time)) + interval '1 day' end as ideal_next_attempt_time
    -- ,   remarks
    
    from
        (select 
                a.* 
            ,   ndr.message
            ,   ndr.timestamp
		 	,	case when deferred_date is null then null
		 			when re_attempt_slot = 'evening' then deferred_date + interval '18 hours'
		 			else deferred_date + interval '12 hours' end as deferred_time
            ,   ndr.remarks
            ,   rank() over(partition by task_id order by ndr_id desc) as ndr_rank
            ,   re_attempt_slot
            
        from all_attempts_data as a
        left join ndr_details_data as ndr
            on a.dispatch_time <= ndr.timestamp 
            and (case when a.next_trip_time is null then true else a.next_trip_time>=ndr.timestamp end)  
            and a.awb = ndr.awb) as full_data
    
    where ndr_rank = 1)
, final2 as (
    SELECT
    awb,
    node_name,
    shipping_city,
    sort_code,
    task_id,
    dispatch_time,
    attempt_number,
    task_status,
    shipment_status,
    cancellation_remarks,
    min_reattempt_time,
    next_trip_time,
    deferred_time,
    re_attempt_slot,
    ideal_next_attempt_time,
    CASE WHEN date_part('hour', ideal_next_attempt_time) = 12 THEN 'Morning' ELSE 'Evening' END AS ideal_next_attempt_slot,
    final.ideal_next_attempt_time::date AS ideal_next_attempt_date,
    case    when shipment_status = 'Cancelled' then null
                when next_trip_time <= ideal_next_attempt_time then 1 
                when ideal_next_attempt_time is null then null 
             else 0 end as ot_ofd,
    case    
                when shipment_status like '%RTO%' or shipment_status in ('Cancelled','Lost','Pickup Failed','Delivered') then 'Done' 
                when ideal_next_attempt_time is not null and next_trip_time is null then 'Pending' 
                else 'Done' end as pendency,
    case when attempt_number = 0 then 'Fresh' else 'Reattempt' end as order_type
    
    from final
)
select * from final2
order by shipping_city, ideal_next_attempt_date, ideal_next_attempt_slot, attempt_number