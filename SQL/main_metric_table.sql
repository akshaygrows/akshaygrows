with base AS (
	 SELECT ops_main.awb,
		ops_main.shipping_city,
		n.node_name AS last_mile_hub,
		ops_main.shipment_status,
		date_trunc('day', ops_main.pickuptime) AS pickup_date,
		CASE WHEN date_trunc('day', ops_main.delivertime) <= ops_main.edd THEN 1 ELSE 0 END AS otd,
		CASE WHEN ops_main.op_owner = 'GS' OR ops_main.user_name = 'HealthKart' THEN ops_main.created_date ELSE ops_main.pickuptime END AS datetime,
		CASE WHEN date_trunc('day', ops_main.first_ofd_time) <= ops_main.edd THEN 1 ELSE 0 END AS ofd,
		ops_main.edd,
		first_ofd_time,
		ops_main.next_delivery_date AS ndd,
		ops_main.last_failed_date::date AS last_failed_date,
		ops_main.modified_date::date AS modified_date,
		tracking_events.rto_freeze_time::date AS rto_freeze_date,
		tracking_events.rto_ofd_time::date AS rto_ofd_date,
			CASE
				WHEN (ops_main.shipment_status in ('Awb Generated', 'Order Assigned','Picked-up','Packed') )
					AND 
					(CASE WHEN ops_main.op_owner = 'GS' OR ops_main.user_name = 'HealthKart' THEN ops_main.created_date ELSE ops_main.pickuptime END ) is not null
					 THEN 'pickup'
				WHEN ops_main.shipment_status = 'Reached Nearest Hub' THEN 'fresh_attempt'
				WHEN ops_main.shipment_status = 'NDR Initiated' THEN 'reattempt'
				WHEN ops_main.shipment_status = 'Failed Delivered' THEN 'ndr_required'
				WHEN ops_main.shipment_status = 'Out for Delivery' AND tracking_events.last_ofd::date = CURRENT_DATE THEN 'action_taken'
				WHEN ops_main.shipment_status = 'Out for Delivery' AND ops_main.delivery_attempts >= 1 THEN 'reattempt'
				WHEN ops_main.shipment_status = 'Out for Delivery' THEN 'fresh_attempt'
				WHEN ops_main.shipment_status = 'RTO Freeze' THEN 'RTO OFD'
				WHEN ops_main.shipment_status = 'RTO Out for Delivery' THEN 'RTO Delivery'
				WHEN ops_main.shipment_status = 'Lost' then 'Lost'
				ELSE 'action_taken'
			END AS action_bucket,
		ops_main.last_scan_time::date AS last_scan_date,
		ops_main.last_scan_time,
		tracking_events.lost_mark_time as lost_mark_time,
		greatest(a.shipment_chargeable_weight, greatest(( b.length*b.breadth*b.height/5000.0), b.weight)) as chargeable_weight,
		closure_status,
		rating_logs.shipper_rating AS rating
	   FROM ops_main
		LEFT JOIN 	tracking_events ON ops_main.shipment_id = tracking_events.shipment_id
		LEFT JOIN   shipment_order_details as a on a.shipment_id = ops_main.shipment_id
		LEFT JOIN   application_db.shipment_weight as b on ops_main.awb = b.shipment_awb
		LEFT JOIN 	application_db.rating_logs rating_logs ON ops_main.awb = rating_logs.awb
		LEFT JOIN 	application_db.node n ON btrim(split_part(n.sort_codes::text, '/'::text, 2)) = ops_main.last_mile_hub
	  WHERE 1 = 1 
		AND ops_main.shipping_partner = 'Hyperlocal' 
		AND ops_main.created_date >= (date_trunc('month', now() + interval '5.5 hours') -interval  '2 months') 
		AND ops_main.shipping_city IS NOT NULL
		AND n.node_name <> 'GSCAN TEST'
	)
	
, missing as (
	select
		shipping_city
	,	last_mile_hub
	,	last_scan_date as missing_action_date
	,	count(distinct(awb)) as missing_shipment
	from base
	where 	1=1
	and 	last_scan_time <= (now() + interval '5.5 hours' - interval '24 hours')
	and 	closure_status <> 'closed' 
	and	 	pickup_date IS NOT NULL
	group by 1,2,3
)

, locus_base AS (
	 SELECT locus_task_brief.task_id,
		base.shipping_city,
		base.last_mile_hub,
		date_trunc('day', locus_task_brief.dispatch_time) AS dispatch_date,
		rank() OVER (PARTITION BY locus_task_brief.awb ORDER BY locus_task_brief.dispatch_time) AS attempt_number,
		locus_task_brief.status,
		locus_task_brief.rider_id,
		CASE WHEN locus_task_brief.cod_amount > 0 THEN 'COD' ELSE 'Prepaid' END AS mop,
        CASE WHEN NOT lower(t.collected_by) ~~ '%manual%' THEN t.cod_amount ELSE 0 END AS upi_collection,
        CASE WHEN lower(t.collected_by) ~~ '%manual%' THEN t.cod_amount ELSE 0 END AS cash_collection
	   FROM locus_task_brief
		RIGHT JOIN base ON locus_task_brief.awb = base.awb
	  	LEFT JOIN application_db.trip t ON locus_task_brief.task_id = t.locus_trip_id
	  WHERE 1 = 1
		and dispatch_time is not null
	)

, all_hubs as (
		select 
			date_time as datetime
		,	shipping_city
		,	last_mile_hub
		from (	SELECT dd::date as date_time
				FROM generate_series
					( date_trunc('month',now()+interval '5.5 hours') - interval '2 months'
					, date_trunc('day',now()+interval '5.5 hours') + interval '10 days'
					, '1 day'::interval) as dd) as date_data
		cross join (select 
					shipping_city
				,	last_mile_hub
				,	count(awb)
				from base
				where shipping_city is not null and last_mile_hub is not null and last_mile_hub <> ''
				group by 1,2) as hub_list
	)

, edd_based AS (
	 SELECT base.shipping_city,
		base.last_mile_hub,
		base.edd,
		sum(base.otd) AS otd,
		sum(base.ofd) AS ofd,
		sum( case when shipment_status = 'Delivered' then 1 else 0 end) AS delivered,
		count(DISTINCT base.awb) AS efd,
		sum(base.otd) / NULLIF(count(DISTINCT base.awb), 0) AS "OTD%",
		sum(base.ofd) / NULLIF(count(DISTINCT base.awb), 0) AS "OFD%",
		sum( CASE WHEN base.shipment_status = 'Delivered' THEN 1 ELSE 0 END) / NULLIF(count(DISTINCT base.awb), 0) AS "DEL%",
	   	count(base.rating) AS no_ratings,
        avg(base.rating)::double precision AS avg_rating
	   FROM base
	  WHERE 1 = 1
	  and base.pickup_date is not null
	  GROUP BY base.shipping_city, base.last_mile_hub, base.edd
	)

, pickup_based AS (
	 SELECT base.shipping_city,
		base.last_mile_hub,
		date_trunc('day', base.datetime) AS datetime,
		count(DISTINCT base.awb) AS drr,
		sum(case when chargeable_weight >= 2000 then 1 else 0 end) as heavy_shipments
	   FROM base
	  WHERE 1 = 1 AND base.datetime IS NOT NULL
	  GROUP BY base.shipping_city, base.last_mile_hub, (date_trunc('day', base.datetime))
	)
	
, locus_based AS (
	 SELECT locus_base.shipping_city,
		locus_base.last_mile_hub,
		locus_base.dispatch_date,
		sum( CASE WHEN locus_base.attempt_number = 1 AND locus_base.status = 'COMPLETED' THEN 1 ELSE 0 END) AS fresh_delivered,
		sum( CASE WHEN locus_base.attempt_number = 1 THEN 1 ELSE 0 END) AS fresh_attempted,
		sum( CASE WHEN locus_base.attempt_number = 1 AND locus_base.mop = 'Prepaid' AND locus_base.status = 'COMPLETED' THEN 1 ELSE 0 END) as prepaid_fresh_delivered,
		sum( CASE WHEN locus_base.attempt_number = 1 AND locus_base.mop = 'Prepaid' THEN 1 ELSE 0 END) as prepaid_fresh_attempted,
		sum( CASE WHEN locus_base.attempt_number = 1 AND locus_base.mop = 'COD' AND locus_base.status = 'COMPLETED' THEN 1 ELSE 0 END) as cod_fresh_delivered,
		sum( CASE WHEN locus_base.attempt_number = 1 AND locus_base.mop = 'COD' THEN 1 ELSE 0 END) as cod_fresh_attempted,
		sum( CASE WHEN locus_base.attempt_number = 1 AND locus_base.status = 'COMPLETED' THEN 1 ELSE 0 END)::double precision / NULLIF(sum( CASE WHEN locus_base.attempt_number = 1 THEN 1 ELSE 0 END), 0)::double precision AS "FASR%",
		count(DISTINCT locus_base.rider_id) AS mandays,
		sum(locus_base.upi_collection) AS upi_collection,
		sum(locus_base.cash_collection) AS cash_collection
	   FROM locus_base
	  WHERE 1 = 1
	  GROUP BY locus_base.shipping_city, locus_base.last_mile_hub, locus_base.dispatch_date
	)
	
, action_date_based AS (
	 SELECT base.shipping_city,
		base.last_mile_hub,
			CASE
				WHEN base.action_bucket = 'pickup' then edd
				WHEN base.action_bucket = 'fresh_attempt' OR base.action_bucket = 'reattempt' THEN base.ndd
				WHEN base.action_bucket = 'ndr_required' THEN base.last_failed_date 
				WHEN base.action_bucket = 'RTO OFD' THEN base.rto_freeze_date 
				WHEN base.action_bucket = 'RTO Delivery' THEN base.rto_ofd_date + interval '1 day'
				when base.action_bucket = 'Lost' then date_trunc('day',base.lost_mark_time) 
				ELSE base.modified_date 
			END AS action_date,
		count(DISTINCT CASE WHEN base.action_bucket = 'fresh_attempt' THEN base.awb ELSE NULL END) AS fresh_pending,
		count(DISTINCT CASE WHEN base.action_bucket = 'reattempt' THEN base.awb ELSE NULL END) AS reattempt_pending,
		count(distinct case when base.action_bucket = 'pickup' then base.awb else null end) as pickup_pending,
		count(distinct case when base.action_bucket = 'lost' then base.awb else null end) as lost_marked
	   FROM base
	  WHERE 1 = 1
	  GROUP BY base.shipping_city, base.last_mile_hub, action_date
	)
, final as (
	select
		concat(to_char(a.datetime,'DD/MM/YYYY'),'-',a.shipping_city,'-',a.last_mile_hub) as key
	,	a.datetime
	,	a.shipping_city
	,	a.last_mile_hub
	,	b.drr
	,	b.heavy_shipments
	,	c.otd
	,	c.ofd
	,	c.delivered
	,	c.efd
	,	c."OTD%"
	,	c."OFD%"
	,	c."DEL%"
	,	c.delivered::double precision / NULLIF(d.mandays, 0) AS prouductivity
	,   c.no_ratings
	,	c.avg_rating
	,	d.prepaid_fresh_delivered
    ,   d.prepaid_fresh_attempted
    ,   d.cod_fresh_delivered
    ,   d.cod_fresh_attempted
	,	d.fresh_delivered
	,	d.fresh_attempted
	,	d.mandays
    ,   d.prepaid_fresh_delivered::double precision / NULLIF(d.prepaid_fresh_attempted, 0) AS prepaid_fasr
    ,   d.cod_fresh_delivered::double precision / NULLIF(d.cod_fresh_attempted, 0) AS cod_fasr
	,	d."FASR%"
	,	d.upi_collection
	,	d.cash_collection
	,	e.fresh_pending
	,	e.reattempt_pending
	,	e.pickup_pending
	,	e.lost_marked
	,	f.missing_shipment as missing_shipment
	
	from all_hubs as a
	left join pickup_based as b on a.datetime = b.datetime AND a.shipping_city = b.shipping_city AND a.last_mile_hub = b.last_mile_hub
	left join edd_based as c on a.datetime = c.edd AND a.shipping_city = c.shipping_city AND a.last_mile_hub = c.last_mile_hub
	left join locus_based as d on a.datetime = d.dispatch_date AND a.shipping_city = d.shipping_city AND a.last_mile_hub = d.last_mile_hub
	left join action_date_based as e ON a.datetime = e.action_date AND a.shipping_city = e.shipping_city AND a.last_mile_hub = e.last_mile_hub
	left join missing as f on a.datetime = f.missing_action_date and a.shipping_city = f.shipping_city and a.last_mile_hub = f.last_mile_hub
)

select * from final