with trip as (
    select
        location_dest
    ,   tour_id
    ,   trip_id
    ,   locus_trip_id
    ,   start_at as dispatch_time
    ,   cod_amount
    ,   failed_delivery_reason as cancellation_remarks
    ,   case when status = 'Cancelled' then closed_at else null end as cancelled_at
    ,   case when status = 'Completed' then closed_at else null end as completed_at
    ,   delivery_otp as otp
    ,   is_collected
    ,   pod
    ,   trip_name as awb
    ,   status
    ,   collected_by
    ,   user_id
    ,   trip_rating
    ,   is_realized
    ,   shipment_id
    ,   attempt_count
    ,   is_fake_attempt
    ,   reconciled_at
    from trip
)
,
location as (
    select
        location_id
    ,   address as customer_address
    ,   pincode
    ,   lat as actual_location_lat
    ,   lng as actual_location_lng
    from location
)
,
tour as (
    select
        tour_id
    ,   node_id
    ,   rider_id as num_rider_id
    ,   start_time as slot_start
    ,   end_time as slot_end
    ,   tour_date
    from tour
)
,
rider as (
    select
        rider_id as num_rider_id
    ,   locus_rider_id as rider_id
    ,   rider_name
    ,   phone as rider_phone
    from rider
)
,
node as (
    select
        node_id
    ,   node_name
    ,   node_type
    ,   actual_city_name as shipping_city
    ,   locus_home_base_id as location_id
    from node
)
,
final as (
    select
        a.trip_id
    ,   a.locus_trip_id
    ,   a.dispatch_time
    ,   a.cod_amount
    ,   a.cancellation_remarks
    ,   a.cancelled_at
    ,   a.completed_at
    ,   a.otp
    ,   c.node_id
    ,   e.node_name
    ,   e.node_type
    ,   e.shipping_city
    ,   b.pincode
    ,   b.customer_address
    ,   e.location_id
    ,   a.is_collected
    ,   b.actual_location_lat
    ,   b.actual_location_lng
    ,   a.pod
    ,   c.num_rider_id
    ,   d.rider_id
    ,   d.rider_name
    ,   d.rider_phone
    ,   a.awb
    ,   c.slot_start
    ,   c.slot_end
    ,   c.tour_date
    ,   a.status
    ,   a.tour_id
    ,   a.collected_by
    ,   a.user_id
    ,   a.trip_rating
    ,   a.is_realized
    ,   a.shipment_id
    ,   a.attempt_count
    ,   a.is_fake_attempt
    ,   a.reconciled_at
    from trip as a
    left join location as b on a.location_dest = b.location_id
    left join tour as c on a.tour_id = c.tour_id
    left join rider as d on c.num_rider_id = d.num_rider_id
    left join node as e on c.node_id = e.node_id
)
select * from final