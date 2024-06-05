with base as (with base as (
    select 
        a.awb
    ,   o.remark
    -- ,   length(o.remark) as remark_length
    ,   o.timestamp::date as lost_date
    ,   b.total_shipment_value as shipment_value
    ,   a.shipping_city
    from ops_main as a
    left join application_db.order_tracking as o on a.awb = o.awb
    left join public.shipment_order_details as b on a.awb = b.awb
    where 1=1 
    and a.shipment_status = 'Lost'
    and o.shipment_status = 'Lost'
    and o.update_time::date >= '2023-08-01'
    and o.shipper_id = 301
)
, attribution as(
    select 
        awb
    ,   remark
    ,   case when remark like '%Lost on Hub:%' then 'Hub'
            when remark like '%Lost on Rider:%' then 'Rider'
            when remark like '%Lost on FM:%' then 'FM'
            else 'other' end as attribution
    ,   lost_date
    ,   shipment_value
    ,   shipping_city
    from base
)
, details as(
    select 
        awb
    ,   remark
    ,   case when attribution = 'Hub' then SUBSTRING(remark, POSITION(':' IN remark) + 2, POSITION('.' IN remark) - POSITION(':' IN remark) - 2) end as node_id
    ,   case when attribution = 'Rider' then SUBSTRING(remark, POSITION(':' IN remark) + 1, POSITION('.' IN remark) - POSITION(':' IN remark) - 1) end as rider_id
    ,   attribution
    ,   lost_date
    ,   shipment_value
    ,   shipping_city
    from attribution

)
, details_better as (
    select
        awb
    ,   remark
    ,   attribution
    ,   locus_rider_id as rider_id
    ,   node_name
    ,   lost_date
    ,   shipment_value
    ,   shipping_city
    from details as a
    left join application_db.rider as b on a.rider_id::numeric = b.rider_id
    left join application_db.node as c on a.node_id::numeric = c.node_id    
    )

select * from details_better where attribution <> 'other')

select 
   sum(shipment_value)
from base
where attribution = 'Hub'
and date_trunc('month',lost_date) = '2024-04-01'