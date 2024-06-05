-- View: public.ops_main_brief

-- DROP VIEW public.ops_main_brief;

CREATE OR REPLACE VIEW public.ops_main_brief
 AS
 SELECT ops_main.shipment_id,
    ops_main.awb,
    ops_main.shop_order_number,
    ops_main.user_name,
    ops_main.shipping_partner,
    ops_main.created_date,
    ops_main.shipment_status,
    ops_main.latest_remark,
    shipment_order_details.cod_amount,
    shipment_order_details.total_shipment_value AS order_amount,
    ops_main.edd,
    ops_main.warehouse_name,
    ops_main.shipping_pincode,
    shipment_order_details.shipping_city,
    ops_main.shipping_zone,
    shipment_order_details.shipping_phone AS customer_phone,
    ops_main.pickuptime,
    ops_main.delivertime,
    ops_main.first_ofd_time,
    ops_main.first_failed_time,
    ops_main.closure_status,
    ops_main.delivery_attempts,
    ops_main.data_refresh_time AS last_update_time
   FROM ops_main
     LEFT JOIN shipment_order_details ON ops_main.shipment_id = shipment_order_details.shipment_id;

ALTER TABLE public.ops_main_brief
    OWNER TO postgres;

