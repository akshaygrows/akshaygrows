-- View: public.locus_task_brief

-- DROP MATERIALIZED VIEW IF EXISTS public.locus_task_brief;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.locus_task_brief
TABLESPACE pg_default
AS
 SELECT DISTINCT ON (task.task_id) task.task_id,
    task.scan_id AS awb,
    task.alternate_id AS shop_order_number,
    task.task_status AS status,
    task.rider_id,
    rider_details.rider_name,
    rider_details.phone AS rider_phone,
    (task.started_at + '05:30:00'::interval)::timestamp without time zone AS task_start_time,
        CASE
            WHEN task.completed_at IS NOT NULL THEN task.completed_at + '05:30:00'::interval
            WHEN task.cancelled_at IS NOT NULL THEN task.cancelled_at + '05:30:00'::interval
            ELSE NULL::timestamp with time zone
        END AS task_end_time,
    task.homebase_address AS homebase,
    task.team_name AS lm_team_name,
    (task.completed_at + '05:30:00'::interval)::timestamp without time zone AS completed_at,
    (task.cancelled_at + '05:30:00'::interval)::timestamp without time zone AS cancelled_at,
    task.cancellation_remarks,
    task.amount_collected::double precision AS cod_amount,
    geodistance(task.planned_location_lat, task.planned_location_lng, task.actual_location_lat, task.actual_location_lng) AS drift_ditance,
        CASE
            WHEN task.drift_cancelled_kms IS NOT NULL OR task.drift_cancelled_kms::text <> ''::text THEN NULLIF(task.drift_cancelled_kms::text, ''::text)::double precision
            ELSE geodistance(task.planned_location_lat, task.planned_location_lng, task.actual_location_lat, task.actual_location_lng)
        END AS drift_cancelled_kms,
        CASE
            WHEN task.drift_completed_kms IS NOT NULL OR task.drift_completed_kms::text <> ''::text THEN NULLIF(task.drift_completed_kms::text, ''::text)::double precision
            ELSE geodistance(task.planned_location_lat, task.planned_location_lng, task.actual_location_lat, task.actual_location_lng)
        END AS drift_completed_kms,
    (task.initial_eta + '05:30:00'::interval)::timestamp without time zone AS original_eta,
    task.completed_at::timestamp without time zone - task.initial_eta::timestamp without time zone AS sla_delay,
    task.customer_name,
    task.customer_address,
    task.pincode AS customer_pincode,
    task.line_items,
    task.location_id,
    task.plan_id,
    task.tour_id,
    (task.accepted_at + '05:30:00'::interval)::timestamp without time zone AS dispatch_time,
    (task.started_at + '05:30:00'::interval)::timestamp without time zone AS started_at,
    (task.slot_end + '05:30:00'::interval)::timestamp without time zone AS slot_end,
    (task.slot_start + '05:30:00'::interval)::timestamp without time zone AS slot_start,
    task.planned_location_lat_long,
    task.actual_location_lat_long,
    task.pod AS pod_url,
    task.track_link AS live_track_link,
    task.signature AS signature_url,
    shipment_order_details.sort_code
   FROM application_db.locus_task task
     LEFT JOIN application_db.rider rider_details ON task.rider_id::text = rider_details.locus_rider_id
     LEFT JOIN ( SELECT DISTINCT ON (shipment_order_details_1.awb) shipment_order_details_1.awb,
            shipment_order_details_1.sort_code
           FROM shipment_order_details shipment_order_details_1) shipment_order_details ON shipment_order_details.awb = task.scan_id::text
WITH DATA;

ALTER TABLE IF EXISTS public.locus_task_brief
    OWNER TO postgres;

GRANT SELECT ON TABLE public.locus_task_brief TO localeuser;
GRANT ALL ON TABLE public.locus_task_brief TO postgres;

CREATE INDEX locus_task_brief_awb_idx
    ON public.locus_task_brief USING btree
    (awb COLLATE pg_catalog."default" DESC)
    TABLESPACE pg_default;
CREATE UNIQUE INDEX task_id
    ON public.locus_task_brief USING btree
    (task_id COLLATE pg_catalog."default")
    TABLESPACE pg_default;