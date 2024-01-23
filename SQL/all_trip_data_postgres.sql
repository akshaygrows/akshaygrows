WITH base AS (
    SELECT ops_main.awb,
    ops_main.pickuptime,
    ops_main.last_mile_hub,
    ops_main.warehouse_city,
    ops_main.shipping_city,
    ops_main.shipping_zone,
    ops_main.shipment_status,
    ops_main.sort_code,
    ops_main.created_date
    FROM ops_main
    WHERE ops_main.shipping_partner = 'Hyperlocal'::text AND ops_main.created_date::date >= '2023-12-01'::date AND ops_main.pickuptime IS NOT NULL
), all_attempts AS (
    SELECT o.awb,
    n.node_name,
    o.shipping_city,
    concat(o.awb, '-zero-attempt') AS task_id,
    o.created_date AS dispatch_time,
    0 AS attempt_number,
        CASE
            WHEN o.warehouse_city = 'Bangalore'::text AND o.shipping_city = 'Hyderabad'::text THEN
            CASE
                WHEN date_part('hour'::text, o.pickuptime) < 22::double precision THEN o.pickuptime::date + '1 day'::interval + '18:00:00'::interval
                ELSE o.pickuptime::date + '2 days'::interval + '18:00:00'::interval
            END
            WHEN o.shipping_zone = 'Within Zone'::text THEN
            CASE
                WHEN date_part('hour'::text, o.pickuptime) < 21::double precision THEN o.pickuptime::date + '1 day'::interval + '12:00:00'::interval
                ELSE o.pickuptime::date + '2 days'::interval + '12:00:00'::interval
            END
            WHEN o.shipping_zone = 'Metro'::text THEN
            CASE
                WHEN date_part('hour'::text, o.pickuptime) < 15::double precision THEN o.pickuptime::date + '1 day'::interval + '12:00:00'::interval
                ELSE o.pickuptime::date + '2 days'::interval + '12:00:00'::interval
            END
            WHEN o.shipping_zone = 'Intracity'::text THEN
            CASE
                WHEN date_part('hour'::text, o.pickuptime) < 3::double precision THEN o.pickuptime::date + '12:00:00'::interval
                WHEN date_part('hour'::text, o.pickuptime) < 11::double precision THEN o.pickuptime::date + '18:00:00'::interval
                ELSE o.pickuptime::date + '1 day'::interval + '12:00:00'::interval
            END
            ELSE o.pickuptime::date + '1 day'::interval + '18:00:00'::interval
        END AS min_reattempt_time,
    NULL::character varying AS status,
    NULL::character varying AS cancellation_remarks,
    o.shipment_status,
    o.sort_code
    FROM base o
        LEFT JOIN application_db.node n ON o.last_mile_hub = btrim(split_part(n.sort_codes::text, '/'::text, 2))
    WHERE n.node_name::text <> 'GSCAN TEST'::text
UNION ALL
    SELECT o.awb,
    n.node_name,
    o.shipping_city,
    l.task_id,
    l.dispatch_time,
    rank() OVER (PARTITION BY o.awb ORDER BY l.dispatch_time) AS attempt_number,
        CASE
            WHEN l.status::text = 'COMPLETED'::text THEN NULL::timestamp without time zone
            WHEN lower(l.cancellation_remarks::text) ~~ '%wrong address%'::text THEN l.dispatch_time::date + '2 days'::interval + '12:00:00'::interval
            WHEN date_part('hour'::text, l.dispatch_time) < 14::double precision THEN l.dispatch_time::date + '1 day'::interval + '12:00:00'::interval
            ELSE l.dispatch_time::date + '1 day'::interval + '18:00:00'::interval
        END AS min_reattempt_time,
    l.status,
    l.cancellation_remarks,
    o.shipment_status,
    l.sort_code
    FROM base o
        JOIN locus_task_brief l ON o.awb = l.awb::text
        LEFT JOIN application_db.node n ON l.location_id::text = n.locus_home_base_id::text
), all_attempts_data AS (
    SELECT ct.awb,
    ct.node_name,
    ct.shipping_city,
    ct.task_id,
    ct.dispatch_time,
    ct.attempt_number,
    ct.min_reattempt_time,
    ct.status,
    ct.cancellation_remarks,
    ct.shipment_status,
    ct.sort_code,
    pt.dispatch_time AS previous_trip_time,
    nt.dispatch_time AS next_trip_time
    FROM all_attempts ct
        LEFT JOIN all_attempts pt ON ct.awb = pt.awb AND (ct.attempt_number - 1) = pt.attempt_number
        LEFT JOIN all_attempts nt ON ct.awb = nt.awb AND (ct.attempt_number + 1) = nt.attempt_number
), ndr_details_data AS (
    SELECT ndr_details.awb,
    ndr_details."timestamp" + '05:30:00'::interval AS "timestamp",
    ndr_details.message,
    ndr_details.remarks,
        CASE
            WHEN ndr_details.message::text = 'Reattempt Requested'::text AND (ndr_details.deferred_date IS NULL OR ndr_details.deferred_date::text = ''::text) THEN ndr_details."timestamp"::date + '1 day'::interval
            ELSE to_date(ndr_details.deferred_date::text, 'YYYY-MM-DD'::text)::timestamp without time zone
        END AS deferred_date,
        CASE
            WHEN ndr_details.re_attempt_slot::text = ANY (ARRAY['Evening'::character varying::text, 'Afternoon'::character varying::text, '4PM - 10PM'::character varying::text]) THEN 'evening'::text
            WHEN ndr_details.re_attempt_slot::text = ANY (ARRAY['9AM - 4PM'::character varying::text, 'Morning'::character varying::text]) THEN 'morning'::text
            ELSE NULL::text
        END AS re_attempt_slot
    FROM application_db.ndr_details
    WHERE ndr_details.shipper_id = 301 AND ndr_details."timestamp"::date >= (date_trunc('week'::text, CURRENT_DATE::timestamp with time zone) - '84 days'::interval)
), final AS (
    SELECT full_data.awb,
    full_data.node_name,
    full_data.shipping_city,
    full_data.sort_code,
    full_data.task_id,
    full_data.dispatch_time,
    full_data.attempt_number,
    full_data.status AS task_status,
    full_data.shipment_status,
    full_data.cancellation_remarks,
    full_data.min_reattempt_time,
    full_data.next_trip_time,
    full_data.deferred_time,
    full_data.re_attempt_slot,
        CASE
            WHEN full_data.attempt_number = 0 AND full_data.deferred_time IS NULL THEN full_data.min_reattempt_time
            WHEN full_data.deferred_time IS NULL THEN NULL::timestamp without time zone
            WHEN full_data.deferred_time >= full_data.min_reattempt_time THEN full_data.deferred_time
            WHEN full_data.deferred_time < full_data.min_reattempt_time AND full_data.re_attempt_slot IS NULL THEN full_data.min_reattempt_time
            WHEN full_data.deferred_time < full_data.min_reattempt_time THEN full_data.deferred_time + date_trunc('day'::text, age(full_data.min_reattempt_time, full_data.deferred_time)) + '1 day'::interval
            ELSE NULL::timestamp without time zone
        END AS ideal_next_attempt_time
    FROM ( SELECT a.awb,
            a.node_name,
            a.shipping_city,
            a.task_id,
            a.dispatch_time,
            a.attempt_number,
            a.min_reattempt_time,
            a.status,
            a.cancellation_remarks,
            a.shipment_status,
            a.sort_code,
            a.previous_trip_time,
            a.next_trip_time,
            ndr.message,
            ndr."timestamp",
                CASE
                    WHEN ndr.deferred_date IS NULL THEN NULL::timestamp without time zone
                    WHEN ndr.re_attempt_slot = 'evening'::text THEN ndr.deferred_date + '18:00:00'::interval
                    ELSE ndr.deferred_date + '12:00:00'::interval
                END AS deferred_time,
            ndr.remarks,
            rank() OVER (PARTITION BY a.task_id ORDER BY ndr."timestamp" DESC) AS ndr_rank,
            ndr.re_attempt_slot
            FROM all_attempts_data a
                LEFT JOIN ndr_details_data ndr ON a.dispatch_time <= ndr."timestamp" AND
                CASE
                    WHEN a.next_trip_time IS NULL THEN true
                    ELSE a.next_trip_time >= ndr."timestamp"
                END AND a.awb = ndr.awb::text) full_data
    WHERE full_data.ndr_rank = 1
), final2 AS (
    SELECT final.awb,
    final.node_name,
    final.shipping_city,
    final.sort_code,
    final.task_id,
    final.dispatch_time,
    final.attempt_number,
    final.task_status,
    final.shipment_status,
    final.cancellation_remarks,
    final.min_reattempt_time,
    final.next_trip_time,
    final.deferred_time,
    final.re_attempt_slot,
    final.ideal_next_attempt_time,
        CASE
            WHEN date_part('hour'::text, final.ideal_next_attempt_time) = 12::double precision THEN 'Morning'::text
            ELSE 'Evening'::text
        END AS ideal_next_attempt_slot,
    final.ideal_next_attempt_time::date AS ideal_next_attempt_date,
        CASE
            WHEN final.shipment_status = 'Cancelled'::text THEN NULL::integer
            WHEN final.next_trip_time <= final.ideal_next_attempt_time THEN 1
            WHEN final.ideal_next_attempt_time IS NULL THEN NULL::integer
            ELSE 0
        END AS ot_ofd,
        CASE
            WHEN final.shipment_status ~~ '%RTO%'::text OR (final.shipment_status = ANY (ARRAY['Cancelled'::text, 'Lost'::text])) THEN 'Done'::text
            WHEN final.ideal_next_attempt_time IS NOT NULL AND final.next_trip_time IS NULL THEN 'Pending'::text
            ELSE 'Done'::text
        END AS pendency,
        CASE
            WHEN final.attempt_number = 0 THEN 'Fresh'::text
            ELSE 'Reattempt'::text
        END AS order_type
    FROM final
)
SELECT final2.awb,
final2.node_name,
final2.shipping_city,
final2.sort_code,
final2.task_id,
final2.dispatch_time,
final2.attempt_number,
final2.task_status,
final2.shipment_status,
final2.cancellation_remarks,
final2.min_reattempt_time,
final2.next_trip_time,
final2.deferred_time,
final2.re_attempt_slot,
final2.ideal_next_attempt_time,
final2.ideal_next_attempt_slot,
final2.ideal_next_attempt_date,
final2.ot_ofd,
final2.pendency,
final2.order_type
FROM final2
ORDER BY final2.shipping_city, final2.ideal_next_attempt_date DESC, final2.ideal_next_attempt_slot, final2.attempt_number
