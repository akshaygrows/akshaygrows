WITH rel_shipments AS
	(SELECT
        DISTINCT shipment_id
    FROM
        public.order_tracking
    WHERE
		--  (	
        --     -- ( --to update for a past interval with last four hours included. Comment this post usage and uncomment the regular expression
		-- 		-- (update_time::date >= '2024-01-01'::date OR timestamp::date >='2024-01-01'::date)
		-- 		-- AND 
		-- 		-- (update_time::date<'2024-01-02'::date OR timestamp>='2024-01-02'::date)
		-- 	-- ) 
		-- 	-- OR
		-- 		-- (update_time>=(NOW() - interval '4 hours') OR timestamp>=(NOW() - interval '4 hours'))
		-- )
		(update_time>=(NOW() - interval '4 hours') OR timestamp>=(NOW() - interval '4 hours'))
		-- (date_trunc('day',update_time) >= '2024-06-25')
        AND user_id NOT IN (1, 2, 3, 10, 11, 42)
		),
rel_order_tracking AS 
	(SELECT
	 	*
	 FROM
		(SELECT 
		order_tracking.tracking_id AS tracking_id, 
		order_tracking.shipment_id AS shipment_id, 
		order_tracking.shipper_id AS shipper_id, 
		CASE 
			WHEN order_tracking.shipment_status='Shipment Created' THEN 'Shipment Created'
			WHEN order_tracking.shipment_status='Order Assigned' THEN 'Shipment Created'
			WHEN order_tracking.shipment_status='Awb Generated' THEN 'Shipment Created'
	 		WHEN order_tracking.shipment_status='RTO In-transit' THEN 'RTO In-Transit'
			WHEN order_tracking.shipment_status='Last-Mile' THEN 'Last Mile'
			WHEN order_tracking.shipment_status='Failed Delivery' THEN 'Failed Delivered'
			WHEN order_tracking.shipment_status='Transit Damage' THEN 'Damaged'
			WHEN order_tracking.shipment_status='In-transit' THEN 'In-Transit'
			WHEN order_tracking.shipment_status='Picked Up' THEN 'Picked Up'
			WHEN order_tracking.shipment_status='Out For Delivery' THEN 'Out for Delivery'
			WHEN order_tracking.shipment_status='Lost' THEN 'Lost'
			WHEN order_tracking.shipment_status='Cancelled' THEN 'Cancelled'
			WHEN order_tracking.shipment_status='Reached Nearest Hub' THEN 'Last Mile'
			WHEN order_tracking.shipment_status='RTO Reached Nearest Hub' THEN 'RTO In-Transit'
			WHEN order_tracking.shipment_status='Failed & Action Required' THEN 'Failed Delivered'
			WHEN order_tracking.shipment_status='RTO Delivered' THEN 'RTO Delivered'
			WHEN order_tracking.shipment_status='RTO Initiated' THEN 'RTO Initiated'
			WHEN order_tracking.shipment_status='Failed Delivered' THEN 'Failed Delivered'
			WHEN order_tracking.shipment_status='Out for Pickup' THEN 'Out for Pickup'
			WHEN order_tracking.shipment_status='Reached Destination City' THEN 'Reached Destination City'
			WHEN order_tracking.shipment_status='Picked-up' THEN 'Picked Up'
			WHEN order_tracking.shipment_status='In-Transit' THEN 'In-Transit'
			WHEN order_tracking.shipment_status='Delivered' THEN 'Delivered'
			WHEN order_tracking.shipment_status='Out for Delivery' THEN 'Out for Delivery'
		 	WHEN order_tracking.shipment_status='Pickup Failed' THEN 'Pickup Failed'
		 	WHEN order_tracking.shipment_status='RTO In-Transit' THEN 'RTO In-Transit'
		 	WHEN order_tracking.shipment_status='RTO Mid-Mile' THEN 'RTO Mid-Mile'
		 	WHEN order_tracking.shipment_status='RTO Out for Delivery' THEN 'RTO Out for Delivery'
			WHEN order_tracking.shipment_status='RTO Freeze' THEN 'RTO Freeze'
		 	WHEN order_tracking.shipment_status='Sort to Bin' THEN 'Sort to Bin'
			WHEN order_tracking.shipment_status='NDR Initiated' THEN 'NDR Initiated'
			WHEN order_tracking.shipment_status='Failed Delivered Collected' THEN 'Failed Delivered Collected'
			ELSE order_tracking.shipment_status
			END AS shipment_status, 
		CASE 
			WHEN shipment_status IN ('Shipment Created','Awb Generated','Shipment Failed','Cancelled','Pickup Failed','Out for Pickup','RTO Delivered') THEN 'Fulfillment Centre'
			WHEN shipment_status IN ('Picked from Rack','Packed') THEN 'Mother Hub'
			WHEN shipment_status = 'Picked-up' and (order_tracking.location IS NULL or order_tracking.location = '') THEN 'FM Vehicle'
			WHEN shipment_status IN ('Picked-up','Reached City Hub','Add to bag','Reached Destination City','Reached Nearest Hub') THEN 'Hub'
			WHEN shipment_status = 'Sort to Bin' then 'Sort to Bin'
			-- WHEN shipment_status = 'Failed Delivered Collected' then 'Node Name'
			WHEN shipment_status IN ('NDR Initiated','RTO Initiated','RTO Freeze','Failed Delivered Collected') THEN null
			WHEN shipment_status  = 'Delivered' THEN 'End Customer'
			WHEN shipment_status = 'Lost' THEN 'Lost'
			WHEN shipment_status IN ('Bag-Dispatched from Hub','In-Transit') THEN 'Mid Mile'
			WHEN shipment_status IN ('Out for Delivery','Failed Delivered') THEN 'Rider'
			WHEN shipment_status IN ('RTO Out for Delivery') THEN 'RTO Vehicle'
			else shipment_status 
			end as last_location,
		location,
		(TO_TIMESTAMP(update_time::TEXT, 'YYYY-MM-DD HH24:MI:SS')::timestamp without time zone) + interval '5 hour 30 minutes' AS update_time,
		order_tracking.remark AS remark

	FROM 
		(SELECT
		 	public.order_tracking.*
		 FROM
			rel_shipments
			left JOIN public.order_tracking ON order_tracking.shipment_id=rel_shipments.shipment_id) AS order_tracking
	ORDER by 
		shipment_id, update_time ASC
		) AS tracking_temp
	WHERE
		shipment_status IS NOT NULL),
last_status AS
	(SELECT
	 	DISTINCT ON (shipment_id) shipment_id,
	 	--shipment_status AS last_shipment_status,
		location AS last_scan_location,
	 	remark AS last_remark
	 FROM
	 	rel_order_tracking
	ORDER by shipment_id, update_time DESC
	),
non_duplicates AS 
	(SELECT
		tracking_id, shipment_id, shipment_status, remark, update_time
	FROM
		(SELECT
			rel_order_tracking.*,  
			lag(shipment_status) over(PARTITION BY shipment_id ORDER BY update_time ASC) AS prev_status 
		FROM
			rel_order_tracking
		WHERE
			shipment_status IN ('Failed Delivered', 'Out for Delivery')) AS rel_trackers
	WHERE
		shipment_status<>prev_status
		OR prev_status IS NULL
	),
ofd_time AS 
	(SELECT 
		shipment_id, 
		COUNT(DISTINCT tracking_id) AS no_attempts,
		min(update_time) AS first_ofd, 
		max(update_time) AS last_ofd
	FROM
		non_duplicates 
	WHERE
		shipment_status='Out for Delivery'
	GROUP BY 
		shipment_id),
failed_delivered_time AS 
	(
	SELECT 
		non_duplicates.shipment_id AS shipment_id, 
		min(non_duplicates.update_time) AS first_failed_delivered, 
		max(non_duplicates.update_time) AS last_failed_delivered,
		min(non_duplicates.tracking_id) AS first_failed_delivered_tracking,
		max(non_duplicates.tracking_id) AS last_failed_delivered_tracking
	FROM
		non_duplicates
		RIGHT JOIN ofd_time ON ofd_time.shipment_id=non_duplicates.shipment_id
	WHERE
		non_duplicates.shipment_status='Failed Delivered'
		AND non_duplicates.update_time>ofd_time.first_ofd
	GROUP BY 
		non_duplicates.shipment_id),
pickup_failed_time AS 
	(SELECT 
		shipment_id, 
		COUNT(DISTINCT tracking_id) AS no_failed_pickup_attempts,
		min(update_time) AS first_failed_pickup_time, 
		max(update_time) AS last_failed_pickup_time
	FROM
		rel_order_tracking 
	WHERE
		shipment_status='Pickup Failed'
	GROUP BY 
		shipment_id),
basic_markers AS
	(SELECT
		shipment_id,
		min(CASE WHEN shipment_status='Out for Pickup' THEN update_time END) AS out_for_pickup_time,
		min(CASE WHEN shipment_status='Picked Up' THEN update_time END) AS pickup_time2,
		max(CASE WHEN shipment_status='Awb Generated' THEN update_time END) AS awb_generated_time,
		min(CASE WHEN shipment_status='In-Transit' THEN update_time END) AS first_in_transit_time,
		max(CASE WHEN shipment_status='In-Transit' THEN update_time END) AS last_in_transit_time,
		-- Last Mile is Reached Nearest Hub time
		max(CASE WHEN shipment_status='Last Mile' THEN update_time END) AS last_mile_time2,
		min(CASE WHEN shipment_status='Delivered' THEN update_time END) AS delivered_time,
		max(CASE WHEN shipment_status='RTO Initiated' THEN update_time END) AS rto_initiated_time,
		max(CASE WHEN shipment_status='RTO Out for Delivery' THEN update_time END) AS rto_ofd_time,
		min(CASE WHEN shipment_status='RTO Delivered' THEN update_time END) AS rto_delivered_time,
		max(CASE WHEN shipment_status='NDR Initiated' THEN update_time END) AS ndr_initiated_time,
	 	max(CASE WHEN shipment_status='RTO Freeze' THEN update_time END) AS rto_freeze_time,
	 	max(CASE WHEN shipment_status='Sort to Bin' AND (NOT (lower(rel_order_tracking."remark") like '%ndr%')) AND (NOT (lower(rel_order_tracking."remark") like '%rto%')) THEN update_time END) AS sort_to_bin_time,
		min(case when shipment_status='Picked Up' and lower(rel_order_tracking.remark) like 'picked-up by%' then update_time end) as cfc_pickup_time,
		max(update_time) AS last_update_time,
		max(case when shipment_status = 'Lost' then update_time end) as lost_mark_time,
		min(case when shipment_status = 'Reached City Hub' then update_time end) as hub_in_scan_time,
		min(case when shipment_status = 'Add to bag' then update_time end) as addtobag_time,
		min(case when shipment_status = 'Reached Destination City' then update_time end) as dest_city_time,
		min(CASE WHEN LEFT(SPLIT_PART(location, '>', 1), 3) = LEFT(SPLIT_PART(location, '>', 2), 3) and shipment_status = 'In-Transit' then update_time end) as intracity_in_transit_time,
		min(CASE WHEN LEFT(SPLIT_PART(location, '>', 1), 3) <> LEFT(SPLIT_PART(location, '>', 2), 3) and shipment_status = 'In-Transit' then update_time end) as intercity_in_transit_time
    FROM 
       rel_order_tracking
    GROUP BY 
       shipment_id
    ),
failed_remarks AS (
	SELECT
		DISTINCT ON (failed_delivered_time.shipment_id)
		failed_delivered_time.shipment_id AS shipment_id,
		failed_delivered_time.first_failed_delivered,
		failed_delivered_time.last_failed_delivered,
		(CASE
			WHEN failed_delivered_time.first_failed_delivered IS NULL
				THEN NULL
			ELSE rel_order_tracking_first.remark END) AS first_failed_delivered_remarks,
		(CASE
			WHEN failed_delivered_time.first_failed_delivered IS NULL
				THEN NULL
			ELSE rel_order_tracking_last.remark END) AS last_failed_delivered_remarks
	FROM
		failed_delivered_time
		LEFT JOIN rel_order_tracking AS rel_order_tracking_last
		ON rel_order_tracking_last.tracking_id=failed_delivered_time.last_failed_delivered_tracking
		LEFT JOIN rel_order_tracking AS rel_order_tracking_first
		ON rel_order_tracking_first.tracking_id=failed_delivered_time.first_failed_delivered_tracking
),
last_loc AS
	(SELECT
		shipment_id, 
		update_time,
		last_location,
		tracking_id,
		location,
		case 	
			when last_location = 'FM Vehicle' then remark
			when last_location = 'Rider' then 'Last Rider ID' --putting this so I replace it while merging in ops main
			when last_location = 'Hub' then trim(split_part(location,'/',2)) || ' Hub'
			when last_location = 'Mid Mile' then 'dispatched from ' || trim(split_part(location,'/',2)) || ' Hub'
			when last_location = 'Node Name' then location
			when last_location = 'Sort to Bin' then 
				case 
					when lower(remark) like '%ndr%' then trim(split_part(location,'/',2)) || ' Hub' || ' - NDR Bin'
					when lower(remark) like '%rto initiated%' then trim(split_part(location,'/',2)) || ' Hub' || ' - RTO Initiate Bin'
					when lower(remark) like '%rto freeze%' then trim(split_part(location,'/',2)) || ' Hub' || ' - RTO Freeze Bin'
					when lower(remark) like '%rto out' then trim(split_part(location,'/',2)) || ' Hub' || ' - RTO OFD Bin'
					else trim(split_part(location,'/',2)) || ' Hub' end
			else null
			end as last_location_details,
		remark
	FROM 
		rel_order_tracking
	WHERE 
		last_location is not null
	ORDER by 
		shipment_id
		, tracking_id DESC
	),
shipment_last_location AS
	(SELECT
		DISTINCT ON (shipment_id)
		shipment_id,
		-- LAST_VALUE(last_location) OVER (PARTITION BY shipment_id ORDER BY update_time DESC) AS last_location
		last_location,
		last_location_details,
		location,
		update_time,
		remark
	FROM 
		last_loc)
SELECT 
	shipment_id,
	out_for_pickup_time,
	pickup_time as pickup_time,
	last_mile_time as last_mile_time, --reached_nearest_hub_time
	no_attempts AS no_attempts,
	first_ofd AS first_ofd,
	last_ofd AS last_ofd,
	first_failed_delivered AS first_failed_delivered,
	last_failed_delivered AS last_failed_delivered,
	first_failed_delivered_remarks,
	last_failed_delivered_remarks,
	delivered_time AS delivered_time,
	rto_initiated_time AS rto_initiated_time,
	rto_delivered_time AS rto_delivered_time,
	last_update_time AS last_update_time,
	last_remark, 
	closed AS closed,
	(CASE
		WHEN first_failed_pickup_time is NULL THEN pickup_time
		ELSE first_failed_pickup_time END)::timestamp AS first_pickup_attempt_time,
	(CASE
		WHEN pickup_time is NULL THEN last_failed_pickup_time
		ELSE pickup_time END)::timestamp AS last_pickup_attempt_time,
	(CASE
		WHEN pickup_time is NULL THEN no_failed_pickup_attempts
		ELSE no_failed_pickup_attempts+1 END)::integer AS no_pickup_attempts,
	last_location, --last grouped location
	ndr_initiated_time,
	last_scan_location, --actual last location in tracking
	rto_ofd_time AS rto_ofd_time,
	rto_freeze_time AS rto_freeze_time,
	sort_to_bin_time, --means when last sorted to DISPATCH bin time
	lost_mark_time,
	hub_in_scan_time,
	cfc_pickup_time,
	addtobag_time,
	in_transit_time,
	dest_city_time,
	intracity_in_transit_time,
	intercity_in_transit_time
FROM
	(SELECT
		basic_markers.shipment_id::bigint AS shipment_id, 
		basic_markers.out_for_pickup_time::timestamp AS out_for_pickup_time, 
		(CASE 
			WHEN basic_markers.pickup_time2 IS NOT NULL THEN basic_markers.pickup_time2
			WHEN basic_markers.first_in_transit_time IS NOT NULL THEN basic_markers.first_in_transit_time
			WHEN ofd_time.first_ofd IS NOT NULL THEN least(ofd_time.first_ofd - interval '4 hours', basic_markers.awb_generated_time + interval '1 day')
			ELSE basic_markers.pickup_time2 END)::timestamp as pickup_time,
		basic_markers.last_mile_time2 as last_mile_time,
		ofd_time.no_attempts::integer AS no_attempts,
		(CASE 
			WHEN ofd_time.first_ofd IS NULL THEN basic_markers.delivered_time - interval '4 hours'
			ELSE ofd_time.first_ofd
		END)::timestamp AS first_ofd,
		(CASE 
			WHEN ofd_time.last_ofd IS NULL THEN basic_markers.delivered_time - interval '4 hours'
			ELSE ofd_time.last_ofd
		END)::timestamp AS last_ofd,
	 	(CASE 
		 WHEN pickup_failed_time.no_failed_pickup_attempts IS NOT NULL THEN no_failed_pickup_attempts
		 ELSE 0 END
		)::integer AS no_failed_pickup_attempts,
 		pickup_failed_time.first_failed_pickup_time::timestamp AS first_failed_pickup_time,
 		pickup_failed_time.last_failed_pickup_time::timestamp AS last_failed_pickup_time,
		failed_remarks.first_failed_delivered::timestamp AS first_failed_delivered,
		failed_remarks.last_failed_delivered::timestamp AS last_failed_delivered,
 		failed_remarks.first_failed_delivered_remarks,
		failed_remarks.last_failed_delivered_remarks,
		basic_markers.delivered_time::timestamp AS delivered_time,
		basic_markers.rto_initiated_time::timestamp AS rto_initiated_time,
		basic_markers.rto_ofd_time::timestamp AS rto_ofd_time,
		basic_markers.rto_delivered_time::timestamp AS rto_delivered_time,
	 	basic_markers.rto_freeze_time::timestamp AS rto_freeze_time,
	 	basic_markers.sort_to_bin_time,
		basic_markers.ndr_initiated_time,
		basic_markers.cfc_pickup_time,
		basic_markers.lost_mark_time,
		basic_markers.hub_in_scan_time,
 		--last_status.last_shipment_status AS last_shipment_status,
		(CASE 
			WHEN (rto_delivered_time IS NULL) AND (delivered_time IS NULL)
				THEN 0
			ELSE 1 END)::integer AS closed,
		shipment_last_location.update_time AS last_update_time,
 		shipment_last_location.remark AS last_remark,
	 	shipment_last_location.last_location AS last_location,
		shipment_last_location.location AS last_scan_location,
		basic_markers.addtobag_time,
		basic_markers.first_in_transit_time as in_transit_time,
		basic_markers.dest_city_time,
		intracity_in_transit_time,
		intercity_in_transit_time
	FROM 
		basic_markers
		LEFT JOIN ofd_time ON ofd_time.shipment_id=basic_markers.shipment_id
		LEFT JOIN failed_remarks ON failed_remarks.shipment_id=basic_markers.shipment_id
 		LEFT JOIN last_status ON last_status.shipment_id=basic_markers.shipment_id
 		LEFT JOIN pickup_failed_time on pickup_failed_time.shipment_id=basic_markers.shipment_id
	 	LEFT JOIN shipment_last_location ON shipment_last_location.shipment_id=basic_markers.shipment_id
	) AS simplified
