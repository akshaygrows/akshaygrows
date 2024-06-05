-- View: public.finops_with_awb

-- DROP MATERIALIZED VIEW IF EXISTS public.finops_with_awb;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.finops_with_awb
TABLESPACE pg_default
AS
 SELECT finops_transaction.transaction_id,
    finops_transaction.amount,
    finops_transaction.realized,
    finops_transaction.anulled,
    finops_transaction.reference_type AS transaction_type,
    finops_transaction.created_time + '05:30:00'::interval AS created_time,
    finops_transaction.completion_time + '05:30:00'::interval AS completion_time,
    finops_transaction.incoming_cash_flow_id,
    finops_transaction.outgoing_cash_flow_id,
    finops_transaction.reference_party_1,
    finops_transaction.incoming_ref_id,
        CASE
            WHEN "substring"(finops_transaction.incoming_ref_id::text, 0, 7) = 'MANUAL'::text THEN "substring"(finops_transaction.incoming_ref_id::text, 9, 10)::date
            ELSE NULL::date
        END AS collection_date,
    finops_cashflow.created_time + '05:30:00'::interval AS cashflow_time,
    finops_cashflow.txn_id AS cashflow_ref_id,
    finops_cashflow.verified AS cashflow_verified,
    shipment_order_details.awb,
    shipment_order_details.shipment_id,
    shipment_order_details.shop_order_number,
    company_info.user_name,
        CASE
            WHEN shipment_order_details.shipper_id = 1 THEN 'Shadowfax'::text
            WHEN shipment_order_details.shipper_id = 2 THEN 'Delhivery'::text
            WHEN shipment_order_details.shipper_id = 3 THEN 'Xpressbees'::text
            WHEN shipment_order_details.shipper_id = 4 THEN 'FedEx'::text
            WHEN shipment_order_details.shipper_id = 5 THEN 'Bluedart'::text
            WHEN shipment_order_details.shipper_id = 6 THEN 'Smartr'::text
            WHEN shipment_order_details.shipper_id = 7 THEN 'Ekart'::text
            WHEN shipment_order_details.shipper_id = 8 THEN 'DTDC'::text
            WHEN shipment_order_details.shipper_id = 301 THEN 'Hyperlocal'::text
            ELSE concat(shipment_order_details.shipper_id, ', ', 'Unknown')
        END AS shipping_partner,
    shipment_order_details.shipment_status
   FROM application_db.finops_transaction
     LEFT JOIN shipment_order_details ON finops_transaction.reference_id::text = shipment_order_details.shipment_id::character varying::text
     LEFT JOIN company_info ON shipment_order_details.user_id = company_info.user_id
     LEFT JOIN application_db.finops_cashflow ON finops_transaction.incoming_cash_flow_id = finops_cashflow.id
  WHERE (finops_transaction.wallet_id <> 1 OR finops_transaction.wallet_id IS NULL) AND shipment_order_details.shipment_id IS NOT NULL AND finops_transaction.created_time >= '2022-09-01 00:00:00+00'::timestamp with time zone
WITH DATA;

ALTER TABLE IF EXISTS public.finops_with_awb
    OWNER TO postgres;


CREATE INDEX finops_with_awb_awb_idx
    ON public.finops_with_awb USING btree
    (awb COLLATE pg_catalog."default" DESC)
    TABLESPACE pg_default;
CREATE INDEX finops_with_awb_shipment_id_idx
    ON public.finops_with_awb USING btree
    (shipment_id DESC)
    TABLESPACE pg_default;
CREATE UNIQUE INDEX transaction_id
    ON public.finops_with_awb USING btree
    (transaction_id)
    TABLESPACE pg_default;