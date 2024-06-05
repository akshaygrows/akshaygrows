-- View: public.lost_attribution

-- DROP VIEW public.lost_attribution;

CREATE OR REPLACE VIEW public.lost_attribution
 AS
 WITH base AS (
         SELECT a.awb,
            o.remark,
            o."timestamp"::date AS lost_date,
            b.total_shipment_value AS shipment_value,
            a.shipping_city
           FROM ops_main a
             LEFT JOIN application_db.order_tracking o ON a.awb = o.awb::text
             LEFT JOIN shipment_order_details b ON a.awb = b.awb
          WHERE 1 = 1 AND a.shipment_status = 'Lost'::text AND o.shipment_status::text = 'Lost'::text AND o.update_time::date >= '2023-08-01'::date
        ), attribution AS (
         SELECT base.awb,
            base.remark,
                CASE
                    WHEN base.remark::text ~~ '%Lost on Hub:%'::text THEN 'Hub'::text
                    WHEN base.remark::text ~~ '%Lost on Rider:%'::text THEN 'Rider'::text
                    WHEN base.remark::text ~~ '%Lost on FM:%'::text THEN 'FM'::text
                    ELSE 'other'::text
                END AS attribution,
            base.lost_date,
            base.shipment_value,
            base.shipping_city
           FROM base
        ), details AS (
         SELECT attribution.awb,
            attribution.remark,
                CASE
                    WHEN attribution.attribution = 'Hub'::text THEN "substring"(attribution.remark::text, "position"(attribution.remark::text, ':'::text) + 2, "position"(attribution.remark::text, '.'::text) - "position"(attribution.remark::text, ':'::text) - 2)
                    ELSE NULL::text
                END AS node_id,
                CASE
                    WHEN attribution.attribution = 'Rider'::text THEN "substring"(attribution.remark::text, "position"(attribution.remark::text, ':'::text) + 1, "position"(attribution.remark::text, '.'::text) - "position"(attribution.remark::text, ':'::text) - 1)
                    ELSE NULL::text
                END AS rider_id,
            attribution.attribution,
            attribution.lost_date,
            attribution.shipment_value,
            attribution.shipping_city
           FROM attribution
        ), details_better AS (
         SELECT a.awb,
            a.remark,
            a.attribution,
            b.locus_rider_id AS rider_id,
            c.node_name,
            a.lost_date,
            a.shipment_value,
            a.shipping_city
           FROM details a
             LEFT JOIN application_db.rider b ON a.rider_id::numeric = b.rider_id::numeric
             LEFT JOIN application_db.node c ON a.node_id::numeric = c.node_id::numeric
        )
 SELECT details_better.awb,
    details_better.remark,
    details_better.attribution,
    details_better.rider_id,
    details_better.node_name,
    details_better.lost_date,
    details_better.shipment_value,
    details_better.shipping_city
   FROM details_better
  WHERE details_better.attribution <> 'other'::text;

ALTER TABLE public.lost_attribution
    OWNER TO postgres;

