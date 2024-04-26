with 

transaction as (
    select 
        distinct on (t.trip_id) t.trip_id
    ,   tt.created_at
    from trip as t 
    inner join transaction_logs as tl on tl.trip_id = t.trip_id
    left join transaction_tracker as tt on tt.id = tl.transaction_id
    where t.start_at >= date_trunc('month',now()+interval '5.5 hours') - interval '1 month'
    order by trip_id, created_at desc
)

, final as (
    select
        -- t.location_dest as trip_id,
        t.trip_id,
        t.locus_trip_id,
        t.trip_name as awb,
        t.start_at as dispatch_time,
        t.status,
        t.cod_amount,
        t.failed_delivery_reason as cancellation_remarks,
		t.is_fake_attempt,
        case when t.status = 'COMPLETED' then t.closed_at else null end as completed_at,
        case when t.status = 'CANCELLED' then t.closed_at else null end as cancelled_at,
        t.tour_id,
        n.node_id,
        n.node_name,
        n.node_type,
        n.actual_city_name as shipping_city,
        l.pincode,
        l.address as customer_address,
        n.locus_home_base_id as location_id,
        t.is_collected,
        l.lat as actual_location_lat,
        l.lng as actual_location_lng,
        t.pod,
        r.rider_id as num_rider_id,
        r.locus_rider_id as rider_id,
        r.rider_name,
        r.phone as rider_phone,
        tour.start_time as slot_start,
        tour.end_time as slot_end,
        tour.tour_date,
        t.user_id,
        t.is_realized,
        t.shipment_id,
        t.attempt_count,
		t.collected_by,
        t.reconciled_at

    from trip t
    left join location l on t.location_dest = l.location_id
    left join tour on t.tour_id = tour.tour_id
    left join rider r on tour.rider_id = r.rider_id
    left join node n on tour.node_id = n.node_id
    left join transaction on transaction.trip_id = t.trip_id
	where ( t.start_at >= now()  - interval '1 hour'
                or
            t.closed_at >= now()  - interval '1 hour'
                or
            transaction.created_at >= now() - interval '1 hour'
        )
)
select * from final