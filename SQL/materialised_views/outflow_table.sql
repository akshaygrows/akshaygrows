-- View: public.outflow_table

-- DROP MATERIALIZED VIEW IF EXISTS public.outflow_table;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.outflow_table
TABLESPACE pg_default
AS
 WITH raw AS (
        ( WITH tracker AS (
                 SELECT outflow_outflowlog.outflow_id,
                    max(
                        CASE
                            WHEN outflow_outflowlog.status::text = ANY (ARRAY['Order Assigned'::character varying::text, 'STO Assigned'::character varying::text]) THEN outflow_outflowlog."timestamp" + '05:30:00'::interval
                            ELSE NULL::timestamp with time zone
                        END) AS assigned_time,
                    max(
                        CASE
                            WHEN outflow_outflowlog.status::text = 'Picked'::text THEN outflow_outflowlog."timestamp" + '05:30:00'::interval
                            ELSE NULL::timestamp with time zone
                        END) AS picked_time,
                    max(
                        CASE
                            WHEN outflow_outflowlog.status::text = 'Packed'::text THEN outflow_outflowlog."timestamp" + '05:30:00'::interval
                            ELSE NULL::timestamp with time zone
                        END) AS packed_time,
                    max(
                        CASE
                            WHEN outflow_outflowlog.status::text = 'Left Warehouse'::text THEN outflow_outflowlog."timestamp" + '05:30:00'::interval
                            ELSE NULL::timestamp with time zone
                        END) AS dispatched_time
                   FROM application_db.outflow_outflowlog
                  GROUP BY outflow_outflowlog.outflow_id
                )
         SELECT outflow_outflow.id::character varying AS id,
            outflow_outflow.suborder_id,
            outflow_outflow.quantity,
            outflow_outflow.status,
            outflow_outflow.warehouse_id,
            shipment_order_details.shipment_id,
            shipment_order_details.awb,
            shipment_order_details.user_id,
            shipment_order_details.shipment_status,
                CASE
                    WHEN warehouses_warehouse.warehouse_city::text = ANY (ARRAY['DELHI'::character varying, 'New Delhi'::character varying, 'Delhi'::character varying, 'North Delhi'::character varying, 'Yamuna Nagar'::character varying, 'North West Delhi'::character varying, 'South Delhi'::character varying, 'Dwarka'::character varying, 'Gurgaon'::character varying, 'Gurugram'::character varying, 'South East Delhi'::character varying, 'Mahendragarh'::character varying, 'South West Delhi'::character varying, 'Rewari'::character varying, 'Jhajjar'::character varying, 'Gurgaon '::character varying]::text[]) THEN 'Delhi'::character varying
                    WHEN warehouses_warehouse.warehouse_city::text = ANY (ARRAY['Bangalore'::character varying, 'Bangalore Rural'::character varying, 'Bengaluru'::character varying, 'Bangalore '::character varying]::text[]) THEN 'Bangalore'::character varying
                    WHEN warehouses_warehouse.warehouse_city::text = ANY (ARRAY['Mumbai'::character varying, 'Thane'::character varying]::text[]) THEN 'Mumbai'::character varying
                    WHEN warehouses_warehouse.warehouse_city::text = 'K.V.Rangareddy'::text THEN 'Hyderabad'::character varying
                    ELSE warehouses_warehouse.warehouse_city
                END AS warehouse_city,
                CASE
                    WHEN date_part('hour'::text, tracker.assigned_time) < 15::double precision THEN date_trunc('day'::text, tracker.assigned_time) + '17:00:00'::interval
                    ELSE date_trunc('day'::text, tracker.assigned_time) + '36:00:00'::interval
                END AS cutoff,
            outflow_outflow.order_id,
            outflow_outflow."shopOrderNumber",
            tracker.assigned_time,
            tracker.picked_time,
            tracker.packed_time,
            tracker.dispatched_time,
            date_part('epoch'::text, tracker.packed_time - tracker.assigned_time) / 3600.0::double precision AS tat,
            date_part('epoch'::text, now() + '05:30:00'::interval -
                CASE
                    WHEN date_part('hour'::text, tracker.assigned_time) < 15::double precision THEN date_trunc('day'::text, tracker.assigned_time) + '17:00:00'::interval
                    ELSE date_trunc('day'::text, tracker.assigned_time) + '36:00:00'::interval
                END) / (24 * 3600)::double precision AS day_since_cutoff
           FROM tracker
             LEFT JOIN application_db.outflow_outflow ON outflow_outflow.id = tracker.outflow_id
             LEFT JOIN application_db.warehouses_warehouse ON warehouses_warehouse.warehouse_int_id = outflow_outflow.warehouse_id
             LEFT JOIN shipment_order_details ON shipment_order_details.awb = outflow_outflow.awb_number::text
          WHERE outflow_outflow.warehouse_id = ANY (ARRAY[182::bigint, 195::bigint, 230::bigint, 359::bigint, 1373::bigint]))
        UNION
         SELECT concat(appserver_suborder.suborder_id::character varying, '-1') AS id,
            appserver_suborder.suborder_id::character varying AS suborder_id,
            appserver_suborder.quantity,
                CASE
                    WHEN shipment_order_details.shipment_status = ANY (ARRAY['Awb Generated'::text, 'Order Assigned'::text, 'Out for Pickup'::text, 'Pickup Failed'::text, 'Shipment Created'::text]) THEN 'Packed'::text
                    ELSE 'Left Warehouse'::text
                END AS status,
            shipment_order_details.warehouse_id,
            shipment_order_details.shipment_id,
            shipment_order_details.awb,
            shipment_order_details.user_id,
            shipment_order_details.shipment_status,
                CASE
                    WHEN shipment_order_details.warehouse_city ~~ '%Delhi%'::text THEN 'Dwarka'::text
                    ELSE shipment_order_details.warehouse_city
                END AS warehouse_city,
                CASE
                    WHEN date_part('hour'::text, shipment_order_details.created_date) < 15::double precision THEN date_trunc('day'::text, shipment_order_details.created_date) + '17:00:00'::interval
                    ELSE date_trunc('day'::text, shipment_order_details.created_date) + '36:00:00'::interval
                END AS cutoff,
            shipment_order_details.order_id::character varying AS order_id,
            shipment_order_details.shop_order_number AS "shopOrderNumber",
            shipment_order_details.created_date AS assigned_time,
            shipment_order_details.created_date AS picked_time,
            shipment_order_details.created_date AS packed_time,
                CASE
                    WHEN tracking_events.pickup_time IS NULL THEN tracking_events.first_ofd
                    ELSE tracking_events.pickup_time
                END AS dispatched_time,
            0 AS tat,
            date_part('epoch'::text, now() + '05:30:00'::interval -
                CASE
                    WHEN date_part('hour'::text, shipment_order_details.created_date) < 15::double precision THEN date_trunc('day'::text, shipment_order_details.created_date) + '17:00:00'::interval
                    ELSE date_trunc('day'::text, shipment_order_details.created_date) + '36:00:00'::interval
                END::timestamp with time zone) / (24 * 3600)::double precision AS day_since_cutoff
           FROM shipment_order_details
             LEFT JOIN application_db.appserver_suborder ON shipment_order_details.order_id = appserver_suborder.order_id_id
             LEFT JOIN tracking_events ON shipment_order_details.shipment_id = tracking_events.shipment_id
          WHERE (shipment_order_details.user_id = ANY (ARRAY[81::bigint, 123::bigint])) AND shipment_order_details.shipment_status <> 'Cancelled'::text AND (shipment_order_details.warehouse_id = ANY (ARRAY[250::bigint, 281::bigint, 370::bigint, 374::bigint, 833::bigint, 1078::bigint]))
        ), final_raw AS (
         SELECT raw.shipment_id,
            raw.awb,
            sum(raw.quantity) AS quantity_sum
           FROM raw
          WHERE raw.shipment_id IS NOT NULL
          GROUP BY raw.shipment_id, raw.awb
        ), final_table AS (
         SELECT raw.shipment_id,
            raw.awb,
            raw.status,
            final_raw.quantity_sum,
            raw.warehouse_id,
            raw.user_id,
            raw.shipment_status,
            raw.warehouse_city,
            raw.cutoff,
            raw.order_id,
            raw."shopOrderNumber",
            raw.assigned_time,
            raw.picked_time,
            raw.packed_time,
            raw.dispatched_time,
            raw.tat,
            raw.day_since_cutoff,
                CASE
                    WHEN raw.status::text = 'Order Assigned'::text THEN 1
                    WHEN raw.status::text = 'Picked'::text THEN 2
                    WHEN raw.status::text = 'Packed'::text THEN 3
                    WHEN raw.status::text = 'Left Warehouse'::text THEN 4
                    ELSE NULL::integer
                END AS seq_id
           FROM raw
             LEFT JOIN final_raw ON final_raw.awb = raw.awb
          WHERE raw.shipment_id IS NOT NULL
        )
 SELECT DISTINCT ON (final_table.shipment_id) final_table.shipment_id,
    final_table.awb,
    final_table.quantity_sum,
    final_table.status,
    final_table.warehouse_id,
    final_table.user_id,
    final_table.shipment_status,
    final_table.warehouse_city,
    final_table.cutoff,
    final_table.order_id,
    final_table."shopOrderNumber",
    final_table.assigned_time,
    final_table.picked_time,
    final_table.packed_time,
    final_table.dispatched_time,
    final_table.tat,
    final_table.day_since_cutoff
   FROM final_table
  ORDER BY final_table.shipment_id, final_table.seq_id DESC
WITH DATA;

ALTER TABLE IF EXISTS public.outflow_table
    OWNER TO postgres;

GRANT SELECT ON TABLE public.outflow_table TO localeuser;
GRANT ALL ON TABLE public.outflow_table TO postgres;

CREATE UNIQUE INDEX shipment_id_idx
    ON public.outflow_table USING btree
    (shipment_id DESC)
    TABLESPACE pg_default;