-- View: public.main_metric_table_2

-- DROP MATERIALIZED VIEW IF EXISTS public.main_metric_table_2;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.main_metric_table_2
TABLESPACE pg_default
AS
 WITH base AS (
         SELECT ops_main.awb,
            ops_main.shipping_city,
            n.node_name AS last_mile_hub,
            ops_main.shipment_status,
            date_trunc('day'::text, ops_main.pickuptime) AS pickup_date,
                CASE
                    WHEN date_trunc('day'::text, ops_main.delivertime) <= ops_main.edd THEN 1
                    ELSE 0
                END AS otd,
                CASE
                    WHEN ops_main.op_owner = 'GS'::text OR ops_main.user_name::text = 'HealthKart'::text THEN ops_main.created_date
                    ELSE ops_main.pickuptime
                END AS datetime,
                CASE
                    WHEN date_trunc('day'::text, ops_main.first_ofd_time) <= ops_main.edd THEN 1
                    ELSE 0
                END AS ofd,
            ops_main.edd,
            ops_main.first_ofd_time,
            ops_main.next_delivery_date AS ndd,
            ops_main.last_failed_date::date AS last_failed_date,
            ops_main.modified_date::date AS modified_date,
            tracking_events.rto_freeze_time::date AS rto_freeze_date,
            tracking_events.rto_ofd_time::date AS rto_ofd_date,
                CASE
                    WHEN (ops_main.shipment_status = ANY (ARRAY['Awb Generated'::text, 'Order Assigned'::text, 'Picked-up'::text, 'Packed'::text])) AND
                    CASE
                        WHEN ops_main.op_owner = 'GS'::text OR ops_main.user_name::text = 'HealthKart'::text THEN ops_main.created_date
                        ELSE ops_main.pickuptime
                    END IS NOT NULL THEN 'pickup'::text
                    WHEN ops_main.shipment_status = 'Reached Nearest Hub'::text THEN 'fresh_attempt'::text
                    WHEN ops_main.shipment_status = 'NDR Initiated'::text THEN 'reattempt'::text
                    WHEN ops_main.shipment_status = 'Failed Delivered'::text THEN 'ndr_required'::text
                    WHEN ops_main.shipment_status = 'Out for Delivery'::text AND tracking_events.last_ofd::date = CURRENT_DATE THEN 'action_taken'::text
                    WHEN ops_main.shipment_status = 'Out for Delivery'::text AND ops_main.delivery_attempts >= 1::double precision THEN 'reattempt'::text
                    WHEN ops_main.shipment_status = 'Out for Delivery'::text THEN 'fresh_attempt'::text
                    WHEN ops_main.shipment_status = 'RTO Freeze'::text THEN 'RTO OFD'::text
                    WHEN ops_main.shipment_status = 'RTO Out for Delivery'::text THEN 'RTO Delivery'::text
                    WHEN ops_main.shipment_status = 'Lost'::text THEN 'Lost'::text
                    ELSE 'action_taken'::text
                END AS action_bucket,
            ops_main.last_scan_time::date AS last_scan_date,
            ops_main.last_scan_time,
            tracking_events.lost_mark_time,
            GREATEST(a.shipment_chargeable_weight, GREATEST((b.length * b.breadth * b.height)::numeric / 5000.0, b.weight::numeric)::double precision) AS chargeable_weight,
            ops_main.closure_status,
            rating_logs.shipper_rating AS rating
           FROM ops_main
             LEFT JOIN tracking_events ON ops_main.shipment_id = tracking_events.shipment_id
             LEFT JOIN shipment_order_details a ON a.shipment_id = ops_main.shipment_id
             LEFT JOIN application_db.shipment_weight b ON ops_main.awb = b.shipment_awb::text
             LEFT JOIN application_db.rating_logs rating_logs ON ops_main.awb = rating_logs.awb::text
             LEFT JOIN application_db.node n ON btrim(split_part(n.sort_codes::text, '/'::text, 2)) = ops_main.last_mile_hub
          WHERE 1 = 1 AND ops_main.shipping_partner = 'Hyperlocal'::text AND ops_main.created_date >= (date_trunc('month'::text, now() + '05:30:00'::interval) - '2 mons'::interval) AND ops_main.shipping_city IS NOT NULL AND n.node_name::text <> 'GSCAN TEST'::text
        ), missing AS (
         SELECT base.shipping_city,
            base.last_mile_hub,
            base.last_scan_date AS missing_action_date,
            count(DISTINCT base.awb) AS missing_shipment
           FROM base
          WHERE 1 = 1 AND base.last_scan_time <= (now() + '05:30:00'::interval - '24:00:00'::interval) AND base.closure_status <> 'closed'::text AND base.pickup_date IS NOT NULL
          GROUP BY base.shipping_city, base.last_mile_hub, base.last_scan_date
        ), locus_base AS (
         SELECT locus_task_brief.task_id,
                CASE
                    WHEN n.city_name::text = 'DEL'::text THEN 'Delhi'::character varying
                    WHEN n.city_name::text = 'BLR'::text THEN 'Bangalore'::character varying
                    WHEN n.city_name::text = 'HYD'::text THEN 'Hyderabad'::character varying
                    WHEN n.city_name::text = 'NCR'::text THEN 'NCR'::character varying
                    WHEN n.city_name::text = 'BOM'::text THEN 'Mumbai'::character varying
                    WHEN n.city_name::text = 'JPR'::text THEN 'Jaipur'::character varying
                    ELSE n.city_name
                END AS shipping_city,
            n.node_name AS last_mile_hub,
            locus_task_brief.customer_pincode AS pincode,
            date_trunc('day'::text, locus_task_brief.dispatch_time) AS dispatch_date,
            rank() OVER (PARTITION BY locus_task_brief.awb ORDER BY locus_task_brief.dispatch_time) AS attempt_number,
            locus_task_brief.status,
            locus_task_brief.rider_id,
                CASE
                    WHEN locus_task_brief.cod_amount > 0::double precision THEN 'COD'::text
                    ELSE 'Prepaid'::text
                END AS mop,
                CASE
                    WHEN NOT lower(t.collected_by) ~~ '%manual%'::text THEN t.cod_amount
                    ELSE 0::double precision
                END AS upi_collection,
                CASE
                    WHEN lower(t.collected_by) ~~ '%manual%'::text THEN t.cod_amount
                    ELSE 0::double precision
                END AS cash_collection
           FROM locus_task_brief
             LEFT JOIN application_db.node n ON n.locus_home_base_id::text = locus_task_brief.location_id::text
             LEFT JOIN application_db.trip t ON locus_task_brief.task_id::text = t.locus_trip_id
          WHERE 1 = 1 AND locus_task_brief.dispatch_time IS NOT NULL AND locus_task_brief.dispatch_time >= (now() - '2 mons 15 days'::interval)
        ), all_hubs AS (
         SELECT date_data.date_time AS datetime,
            hub_list.shipping_city,
            hub_list.last_mile_hub
           FROM ( SELECT dd.dd::date AS date_time
                   FROM generate_series(date_trunc('month'::text, now() + '05:30:00'::interval) - '2 mons'::interval, date_trunc('day'::text, now() + '05:30:00'::interval) + '10 days'::interval, '1 day'::interval) dd(dd)) date_data
             CROSS JOIN ( SELECT base.shipping_city,
                    base.last_mile_hub,
                    count(base.awb) AS count
                   FROM base
                  WHERE base.shipping_city IS NOT NULL AND base.last_mile_hub IS NOT NULL AND base.last_mile_hub::text <> ''::text
                  GROUP BY base.shipping_city, base.last_mile_hub) hub_list
        ), edd_based AS (
         SELECT base.shipping_city,
            base.last_mile_hub,
            base.edd,
            sum(base.otd) AS otd,
            sum(base.ofd) AS ofd,
            sum(
                CASE
                    WHEN base.shipment_status = 'Delivered'::text THEN 1
                    ELSE 0
                END) AS delivered,
            count(DISTINCT base.awb) AS efd,
            sum(base.otd) / NULLIF(count(DISTINCT base.awb), 0) AS "OTD%",
            sum(base.ofd) / NULLIF(count(DISTINCT base.awb), 0) AS "OFD%",
            sum(
                CASE
                    WHEN base.shipment_status = 'Delivered'::text THEN 1
                    ELSE 0
                END) / NULLIF(count(DISTINCT base.awb), 0) AS "DEL%",
            count(base.rating) AS no_ratings,
            avg(base.rating)::double precision AS avg_rating
           FROM base
          WHERE 1 = 1 AND base.pickup_date IS NOT NULL
          GROUP BY base.shipping_city, base.last_mile_hub, base.edd
        ), pickup_based AS (
         SELECT base.shipping_city,
            base.last_mile_hub,
            date_trunc('day'::text, base.datetime) AS datetime,
            count(DISTINCT base.awb) AS drr,
            sum(
                CASE
                    WHEN base.chargeable_weight >= 2000::double precision THEN 1
                    ELSE 0
                END) AS heavy_shipments
           FROM base
          WHERE 1 = 1 AND base.datetime IS NOT NULL
          GROUP BY base.shipping_city, base.last_mile_hub, (date_trunc('day'::text, base.datetime))
        ), locus_based AS (
         SELECT a.shipping_city,
            a.last_mile_hub,
            a.dispatch_date,
            sum(
                CASE
                    WHEN a.attempt_number = 1 AND a.status::text = 'COMPLETED'::text THEN 1
                    ELSE 0
                END) AS fresh_delivered,
            sum(
                CASE
                    WHEN a.attempt_number = 1 THEN 1
                    ELSE 0
                END) AS fresh_attempted,
            sum(
                CASE
                    WHEN a.attempt_number = 1 AND a.mop = 'Prepaid'::text AND a.status::text = 'COMPLETED'::text THEN 1
                    ELSE 0
                END) AS prepaid_fresh_delivered,
            sum(
                CASE
                    WHEN a.attempt_number = 1 AND a.mop = 'Prepaid'::text THEN 1
                    ELSE 0
                END) AS prepaid_fresh_attempted,
            sum(
                CASE
                    WHEN a.attempt_number = 1 AND a.mop = 'COD'::text AND a.status::text = 'COMPLETED'::text THEN 1
                    ELSE 0
                END) AS cod_fresh_delivered,
            sum(
                CASE
                    WHEN a.attempt_number = 1 AND a.mop = 'COD'::text THEN 1
                    ELSE 0
                END) AS cod_fresh_attempted,
            sum(
                CASE
                    WHEN a.attempt_number = 1 AND a.status::text = 'COMPLETED'::text THEN 1
                    ELSE 0
                END)::double precision / NULLIF(sum(
                CASE
                    WHEN a.attempt_number = 1 THEN 1
                    ELSE 0
                END), 0)::double precision AS "FASR%",
            sum(
                CASE
                    WHEN a.status::text = 'COMPLETED'::text THEN 1
                    ELSE 0
                END) AS total_delivered,
            count(DISTINCT a.rider_id) AS mandays,
            sum(a.upi_collection) AS upi_collection,
            count(DISTINCT a.pincode) AS distinct_pincodes,
            sum(a.cash_collection) AS cash_collection
           FROM locus_base a
          WHERE 1 = 1
          GROUP BY a.shipping_city, a.last_mile_hub, a.dispatch_date
        ), action_date_based AS (
         SELECT base.shipping_city,
            base.last_mile_hub,
                CASE
                    WHEN base.action_bucket = 'pickup'::text THEN base.edd
                    WHEN base.action_bucket = 'fresh_attempt'::text OR base.action_bucket = 'reattempt'::text THEN base.ndd
                    WHEN base.action_bucket = 'ndr_required'::text THEN base.last_failed_date::timestamp without time zone
                    WHEN base.action_bucket = 'RTO OFD'::text THEN base.rto_freeze_date::timestamp without time zone
                    WHEN base.action_bucket = 'RTO Delivery'::text THEN base.rto_ofd_date + '1 day'::interval
                    WHEN base.action_bucket = 'Lost'::text THEN date_trunc('day'::text, base.lost_mark_time)
                    ELSE base.modified_date::timestamp without time zone
                END AS action_date,
            count(DISTINCT
                CASE
                    WHEN base.action_bucket = 'fresh_attempt'::text THEN base.awb
                    ELSE NULL::text
                END) AS fresh_pending,
            count(DISTINCT
                CASE
                    WHEN base.action_bucket = 'reattempt'::text THEN base.awb
                    ELSE NULL::text
                END) AS reattempt_pending,
            count(DISTINCT
                CASE
                    WHEN base.action_bucket = 'pickup'::text THEN base.awb
                    ELSE NULL::text
                END) AS pickup_pending,
            count(DISTINCT
                CASE
                    WHEN base.action_bucket = 'lost'::text THEN base.awb
                    ELSE NULL::text
                END) AS lost_marked
           FROM base
          WHERE 1 = 1
          GROUP BY base.shipping_city, base.last_mile_hub, (
                CASE
                    WHEN base.action_bucket = 'pickup'::text THEN base.edd
                    WHEN base.action_bucket = 'fresh_attempt'::text OR base.action_bucket = 'reattempt'::text THEN base.ndd
                    WHEN base.action_bucket = 'ndr_required'::text THEN base.last_failed_date::timestamp without time zone
                    WHEN base.action_bucket = 'RTO OFD'::text THEN base.rto_freeze_date::timestamp without time zone
                    WHEN base.action_bucket = 'RTO Delivery'::text THEN base.rto_ofd_date + '1 day'::interval
                    WHEN base.action_bucket = 'Lost'::text THEN date_trunc('day'::text, base.lost_mark_time)
                    ELSE base.modified_date::timestamp without time zone
                END)
        ), final AS (
         SELECT concat(to_char(a.datetime::timestamp with time zone, 'DD/MM/YYYY'::text), '-', a.shipping_city, '-', a.last_mile_hub) AS key,
            a.datetime,
            a.shipping_city,
            a.last_mile_hub,
            b.drr,
            b.heavy_shipments,
            c.otd,
            c.ofd,
            c.delivered,
            c.efd,
            c."OTD%",
            c."OFD%",
            c."DEL%",
            c.no_ratings,
            c.avg_rating,
            d.prepaid_fresh_delivered,
            d.prepaid_fresh_attempted,
            d.cod_fresh_delivered,
            d.cod_fresh_attempted,
            d.fresh_delivered,
            d.fresh_attempted,
            d.mandays,
            d.prepaid_fresh_delivered::double precision / NULLIF(d.prepaid_fresh_attempted, 0)::double precision AS prepaid_fasr,
            d.cod_fresh_delivered::double precision / NULLIF(d.cod_fresh_attempted, 0)::double precision AS cod_fasr,
            d."FASR%",
            d.upi_collection,
            d.cash_collection,
            d.total_delivered AS locus_delivered,
            d.total_delivered::double precision / NULLIF(d.mandays, 0)::double precision AS productivity,
            d.distinct_pincodes,
            d.total_delivered::double precision / NULLIF(d.distinct_pincodes, 0)::double precision AS load_per_pincode,
            e.fresh_pending,
            e.reattempt_pending,
            e.pickup_pending,
            e.lost_marked,
            f.missing_shipment,
            g.target_productivity,
            d.total_delivered::double precision / NULLIF(d.mandays, 0)::double precision / g.target_productivity::double precision AS cap_util
           FROM all_hubs a
             LEFT JOIN pickup_based b ON a.datetime = b.datetime AND a.shipping_city = b.shipping_city AND a.last_mile_hub::text = b.last_mile_hub::text
             LEFT JOIN edd_based c ON a.datetime = c.edd AND a.shipping_city = c.shipping_city AND a.last_mile_hub::text = c.last_mile_hub::text
             LEFT JOIN locus_based d ON a.datetime = d.dispatch_date AND a.shipping_city = d.shipping_city::text AND a.last_mile_hub::text = d.last_mile_hub::text
             LEFT JOIN action_date_based e ON a.datetime = e.action_date AND a.shipping_city = e.shipping_city AND a.last_mile_hub::text = e.last_mile_hub::text
             LEFT JOIN missing f ON a.datetime = f.missing_action_date AND a.shipping_city = f.shipping_city AND a.last_mile_hub::text = f.last_mile_hub::text
             LEFT JOIN target_productivity g ON a.last_mile_hub::text = g.node_name::text AND a.shipping_city = g.shipping_city::text AND date_trunc('month'::text, a.datetime::timestamp with time zone) = to_date(g.month::text || ' 1, 2024'::text, 'Month DD, YYYY'::text)
        )
 SELECT final.key,
    final.datetime,
    final.shipping_city,
    final.last_mile_hub,
    final.drr,
    final.heavy_shipments,
    final.otd,
    final.ofd,
    final.delivered,
    final.efd,
    final."OTD%",
    final."OFD%",
    final."DEL%",
    final.no_ratings,
    final.avg_rating,
    final.prepaid_fresh_delivered,
    final.prepaid_fresh_attempted,
    final.cod_fresh_delivered,
    final.cod_fresh_attempted,
    final.fresh_delivered,
    final.fresh_attempted,
    final.mandays,
    final.prepaid_fasr,
    final.cod_fasr,
    final."FASR%",
    final.upi_collection,
    final.cash_collection,
    final.locus_delivered,
    final.productivity,
    final.distinct_pincodes,
    final.load_per_pincode,
    final.fresh_pending,
    final.reattempt_pending,
    final.pickup_pending,
    final.lost_marked,
    final.missing_shipment,
    final.target_productivity,
    final.cap_util
   FROM final
WITH DATA;

ALTER TABLE IF EXISTS public.main_metric_table_2
    OWNER TO postgres;