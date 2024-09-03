-- View: public.cod_base_data

-- DROP MATERIALIZED VIEW IF EXISTS public.cod_base_data;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.cod_base_data
TABLESPACE pg_default
AS
 WITH finops_cashflow AS (
         SELECT finops_cashflow.id,
            finops_cashflow.created_time
           FROM application_db.finops_cashflow
          WHERE finops_cashflow.created_time >= (now() - '3 mons'::interval)
        ), collection AS (
         SELECT date_trunc('day'::text, k.completed_at) AS delivery_date,
            k.num_rider_id AS rider_id,
            k.rider_name,
            k.awb,
            k.cod_amount AS amount,
            k.node_name AS hub_name,
            k.shipping_city,
            0 AS collected,
            0 AS deposited,
            NULL::text AS collected_on,
            NULL::text AS collected_by,
            k.trip_id,
            NULL::date AS deposited_on
           FROM kaptaan k
          WHERE k.status::text = 'COMPLETED'::text AND k.is_collected = false AND k.is_realized = false AND k.cod_amount <> 0 AND k.dispatch_time >= (date_trunc('month'::text, now()) - '3 mons'::interval)
        ), deposition AS (
         SELECT date_trunc('day'::text, k.completed_at) AS delivery_date,
            k.num_rider_id AS rider_id,
            k.rider_name,
            k.awb,
            f.amount,
            k.node_name AS hub_name,
            k.shipping_city,
            1 AS collected,
                CASE
                    WHEN f.incoming_cash_flow_id IS NULL THEN 0
                    ELSE 1
                END AS deposited,
            to_char(k.reconciled_at, 'YYYY-MM-DD'::text) AS collected_on,
            split_part(k.collected_by::text, '__'::text, 4) AS collected_by,
            k.trip_id,
            (fc.created_time + '05:30:00'::interval)::date AS deposited_on
           FROM kaptaan k
             LEFT JOIN ( SELECT ops_main.awb,
                    ops_main.shipment_id,
                    ops_main.shipping_city
                   FROM ops_main) o ON k.awb::text = o.awb
             LEFT JOIN application_db.finops_transaction f ON o.shipment_id::character varying::text = f.reference_id::text
             LEFT JOIN finops_cashflow fc ON f.incoming_cash_flow_id = fc.id
          WHERE k.status::text = 'COMPLETED'::text AND f.reference_type::text = 'cod_amount'::text AND (k.is_collected = true OR k.is_realized = true) AND k.dispatch_time >= (date_trunc('month'::text, now()) - '3 mons'::interval)
        ), final AS (
         SELECT collection.delivery_date,
            collection.rider_id,
            collection.rider_name,
            collection.awb,
            collection.amount,
            collection.hub_name,
            collection.shipping_city,
            collection.collected,
            collection.deposited,
            collection.collected_on,
            collection.collected_by,
            collection.trip_id,
            collection.deposited_on
           FROM collection
        UNION
         SELECT deposition.delivery_date,
            deposition.rider_id,
            deposition.rider_name,
            deposition.awb,
            deposition.amount,
            deposition.hub_name,
            deposition.shipping_city,
            deposition.collected,
            deposition.deposited,
            deposition.collected_on,
            deposition.collected_by,
            deposition.trip_id,
            deposition.deposited_on
           FROM deposition
        )
 SELECT final.trip_id,
    final.delivery_date::date AS delivery_date,
    final.rider_id,
    final.rider_name,
    final.awb,
    final.amount,
    final.hub_name,
    final.shipping_city,
    final.collected,
    final.deposited,
        CASE
            WHEN final.collected_on IS NULL THEN NULL::date
            ELSE to_date(final.collected_on, 'YYYY-MM-DD'::text)
        END AS collected_on,
    final.collected_by,
    final.deposited_on
   FROM final
  WHERE 1 = 1 AND final.delivery_date >= (date_trunc('day'::text, now() + '05:30:00'::interval) - '3 mons'::interval)
WITH DATA;

ALTER TABLE IF EXISTS public.cod_base_data
    OWNER TO postgres;


CREATE UNIQUE INDEX cod_base_data_pkey
    ON public.cod_base_data USING btree
    (trip_id)
    TABLESPACE pg_default;