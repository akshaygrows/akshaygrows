-- View: public.ops_main

-- DROP MATERIALIZED VIEW IF EXISTS public.ops_main;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.ops_main
TABLESPACE pg_default
AS
 WITH last_rider_details AS (
         SELECT DISTINCT ON (locus_task_brief.awb) locus_task_brief.awb,
            (('rider_id: '::text || last_value(locus_task_brief.rider_id) OVER (PARTITION BY locus_task_brief.awb ORDER BY locus_task_brief.task_start_time DESC)::text) || ' -- rider_name: '::text) || last_value(locus_task_brief.rider_name) OVER (PARTITION BY locus_task_brief.awb ORDER BY locus_task_brief.task_start_time DESC)::text AS last_rider
           FROM locus_task_brief
        ), rtd AS (
         SELECT outflow_table.shipment_id,
            max(outflow_table.packed_time) AS ready_to_dispatch_time
           FROM outflow_table
          GROUP BY outflow_table.shipment_id
        ), cod_data AS (
         SELECT finops_with_awb.shipment_id,
            max(finops_with_awb.collection_date) AS cod_collection_date,
            max(finops_with_awb.cashflow_time) AS cod_deposition_date
           FROM finops_with_awb
          WHERE finops_with_awb.reference_party_1::text = 's301'::text AND finops_with_awb.transaction_type::text = 'cod_amount'::text AND finops_with_awb.realized = true AND finops_with_awb.anulled = false
          GROUP BY finops_with_awb.shipment_id
        )
 SELECT a.awb,
    a.shipment_id,
    a.shop_order_number,
    a.shipment_status,
    a.shipment_type,
    a.shipping_zone,
    a.warehouse_name,
    a.warehouse_city,
    a.shipping_city,
        CASE
            WHEN a.shipper_id = 301 THEN btrim(split_part(a.sort_code, '/'::text, 2))
            ELSE 'NA-3PL'::text
        END AS last_mile_hub,
    b.user_name,
    a.payment_type AS mop,
    a.created_date,
    a.modified_date,
        CASE
            WHEN a.shipper_id = 1 THEN 'Shadowfax'::text
            WHEN a.shipper_id = 2 THEN 'Delhivery'::text
            WHEN a.shipper_id = 3 THEN 'Xpressbees'::text
            WHEN a.shipper_id = 4 THEN 'FedEx'::text
            WHEN a.shipper_id = 5 THEN 'Bluedart'::text
            WHEN a.shipper_id = 6 THEN 'Smartr'::text
            WHEN a.shipper_id = 7 THEN 'Ekart'::text
            WHEN a.shipper_id = 8 THEN 'DTDC'::text
            WHEN a.shipper_id = 301 THEN 'Hyperlocal'::text
            WHEN a.shipper_id = 1001 THEN 'B2B'::text
            ELSE concat(a.shipper_id, ', ', 'Unknown')
        END AS shipping_partner,
    d.shipment_edd AS edd,
        CASE
            WHEN a.shipment_status = ANY (ARRAY['Delivered'::text, 'RTO Delivered'::text, 'Lost'::text, 'Cancelled'::text]) THEN 'closed'::text
            ELSE 'open'::text
        END AS closure_status,
        CASE
            WHEN a.delivery_attempts IS NOT NULL THEN a.delivery_attempts::double precision
            WHEN c.no_attempts IS NOT NULL THEN c.no_attempts
            ELSE 0::double precision
        END AS delivery_attempts,
    c.first_failed_delivered AS first_failed_time,
    c.first_failed_delivered_remarks AS first_failed_remark,
    c.last_failed_delivered AS last_failed_date,
    c.last_remark AS latest_remark,
    d.ideal_pickup_time,
    d.ndd AS next_delivery_date,
        CASE
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Shipment Failed'::text THEN 'SF'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Shipment Created'::text THEN 'PP'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Order Assigned'::text THEN 'PP'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Out for Pickup'::text THEN 'PP'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Pickup Failed'::text THEN 'PP'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Picked-up'::text THEN 'IN'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'In-Transit'::text THEN 'IN'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Reached Destination City'::text THEN 'IN'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Reached Nearest Hub'::text THEN 'IN'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Delivery Delay'::text THEN 'ND'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Out for Delivery'::text THEN 'OFD'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Failed Delivered'::text THEN 'ND'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Failed & Action Required'::text THEN 'ND'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Transit Damage'::text THEN 'TD'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Delivered'::text THEN 'DL'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'RTO Initiated'::text THEN 'RTO IN'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'RTO In-Transit'::text THEN 'RTO IN'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'RTO Out for Delivery'::text THEN 'RTO OFD'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'RTO Not Delivered'::text THEN 'RTO ND'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'RTO Delivered'::text THEN 'RTO DL'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Lost'::text THEN 'LT'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Cancelled'::text THEN 'CN'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Status Not Defined'::text THEN 'SND'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Delivered - To be RTO'::text THEN 'SND'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Lost - POD Not Available'::text THEN 'SND'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'NDR Initiated'::text THEN 'NDR'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Damaged'::text THEN 'DAM'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Awb Generated'::text THEN 'PP'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'Packed Warehouse'::text THEN 'PP'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'RTO Delivery Delay'::text THEN 'RTO ND'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'RTO Failed Delivered'::text THEN 'RTO ND'::text
            WHEN a.shipper_id <> 301 AND a.shipment_status = 'RTO Reached Nearest Hub'::text THEN 'RTO IN'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Shipment Failed'::text THEN 'SF'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Awb Generated'::text THEN 'PP'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Shipment Created'::text THEN 'PP'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Order Assigned'::text THEN 'PP'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Out for Pickup'::text THEN 'PP'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Pickup Failed'::text THEN 'PP'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Picked-up'::text THEN 'IN'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'In-Transit'::text THEN 'IN'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Reached Destination City'::text THEN 'IN'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Reached Nearest Hub'::text THEN 'IN'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Delivery Delay'::text THEN 'ND'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Out for Delivery'::text THEN 'OFD'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Failed Delivered'::text THEN 'ND'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Failed & Action Required'::text THEN 'ND'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Transit Damage'::text THEN 'TD'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Delivered'::text THEN 'DL'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'RTO Initiated'::text THEN 'RTO IN'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'RTO In-Transit'::text THEN 'RTO IN'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'RTO Out for Delivery'::text THEN 'RTO OFD'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'RTO Not Delivered'::text THEN 'RTO ND'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'RTO Delivered'::text THEN 'RTO DL'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Lost'::text THEN 'LT'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Cancelled'::text THEN 'CN'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Status Not Defined'::text THEN 'SND'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Delivered - To be RTO'::text THEN 'SND'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'Lost - POD Not Available'::text THEN 'SND'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'NDR Initiated'::text THEN 'NDR'::text
            WHEN a.shipper_id = 301 AND a.shipment_status = 'RTO Freeze'::text THEN 'RTO'::text
            ELSE 'SND'::text
        END AS gs_status,
    c.pickup_time AS pickuptime,
    c.delivered_time AS delivertime,
    c.first_ofd AS first_ofd_time,
    c.last_ofd AS last_ofd_time,
    c.rto_initiated_time,
    c.rto_ofd_time,
    c.rto_delivered_time,
    c.rto_freeze_time,
    c.last_update_time,
        CASE
            WHEN c.pickup_time::date > d.ideal_pickup_time::date THEN 0
            WHEN c.pickup_time::date <= d.ideal_pickup_time::date THEN 1
            ELSE NULL::integer
        END AS on_time_pickup,
        CASE
            WHEN c.first_ofd::date > d.shipment_edd::date THEN 0
            WHEN c.first_ofd::date <= d.shipment_edd::date THEN 1
            ELSE NULL::integer
        END AS on_time_ofd,
        CASE
            WHEN c.delivered_time::date > d.shipment_edd::date THEN 0
            WHEN c.delivered_time::date <= d.shipment_edd::date THEN 1
            ELSE NULL::integer
        END AS on_time_del,
    date_part('epoch'::text, c.pickup_time - c.first_ofd) / 3600::double precision AS pto_h,
    date_part('epoch'::text, d.ideal_pickup_time - c.delivered_time) / 3600::double precision AS ito_h,
        CASE
            WHEN (date_part('epoch'::text, c.delivered_time - d.ideal_pickup_time) / 3600::double precision) > 240::double precision THEN 1
            WHEN (date_part('epoch'::text, c.pickup_time - c.first_ofd) / 3600::double precision) < 0::double precision THEN 1
            WHEN date_part('epoch'::text, c.pickup_time - c.delivered_time) < 0::double precision THEN 1
            WHEN date_part('epoch'::text, a.created_date - c.delivered_time) < 0::double precision THEN 1
            ELSE 0
        END AS outlier_flag,
    c.last_failed_delivered_remarks AS last_failed_remark,
    a.shipping_pincode,
    d.shipment_edd_logops AS shipper_edd,
    c.last_update_time AS last_scan_time,
    a.op_owner,
    a.sort_code,
    d.ideal_packed_time,
        CASE
            WHEN c.last_location::text ~~ 'Sorted to bin RTO%'::text AND a.shipper_id = 301 THEN 'Sorted to bin RTO'::character varying
            WHEN c.last_location::text ~~ 'Sorted to bin NDR%'::text AND a.shipper_id = 301 THEN 'Sorted to bin NDR'::character varying
            WHEN c.last_location::text ~~ 'Sorted to bin%'::text AND a.shipper_id = 301 THEN 'Sorted to bin'::character varying
            WHEN c.last_location::text ~~ 'Prepared Bag%'::text AND a.shipper_id = 301 THEN 'Prepared Bag'::character varying
            ELSE c.last_location
        END AS last_location,
        CASE
            WHEN c.last_location::text = 'Fulfillment Centre'::text THEN (a.warehouse_name || ', '::text) || a.warehouse_city
            WHEN a.shipper_id <> 301 THEN '3PL'::text
            WHEN c.last_location::text = 'Last Mile Hub'::text AND a.shipper_id = 301 THEN c.last_scan_location::text
            WHEN c.last_location::text = 'Rider'::text AND a.shipper_id = 301 THEN last_rider_details.last_rider
            WHEN c.last_location::text ~~ 'Sorted to bin RTO%'::text AND a.shipper_id = 301 THEN c.last_scan_location::text
            WHEN c.last_location::text ~~ 'Sorted to bin NDR%'::text AND a.shipper_id = 301 THEN c.last_scan_location::text
            WHEN c.last_location::text ~~ 'Sorted to bin%'::text AND a.shipper_id = 301 THEN "substring"(c.last_location::text, 14)
            WHEN c.last_location::text ~~ 'Prepared Bag%'::text AND a.shipper_id = 301 THEN "substring"(c.last_location::text, 16)
            ELSE NULL::text
        END AS last_location_details,
    rtd.ready_to_dispatch_time,
        CASE
            WHEN cod_data.cod_collection_date IS NOT NULL THEN cod_data.cod_collection_date::timestamp with time zone
            ELSE cod_data.cod_deposition_date
        END AS cod_collection_date,
    cod_data.cod_deposition_date,
    now() AS data_refresh_time
   FROM shipment_order_details a
     LEFT JOIN company_info b ON a.user_id = b.user_id
     LEFT JOIN tracking_events c ON a.shipment_id = c.shipment_id
     LEFT JOIN shipment_timelines d ON a.shipment_id = d.shipment_id
     LEFT JOIN last_rider_details ON last_rider_details.awb::text = a.awb
     LEFT JOIN rtd ON a.shipment_id = rtd.shipment_id
     LEFT JOIN cod_data ON cod_data.shipment_id = a.shipment_id
WITH DATA;

ALTER TABLE IF EXISTS public.ops_main
    OWNER TO postgres;

GRANT SELECT ON TABLE public.ops_main TO localeuser;
GRANT ALL ON TABLE public.ops_main TO postgres;

CREATE UNIQUE INDEX shipment_id
    ON public.ops_main USING btree
    (shipment_id)
    TABLESPACE pg_default;