with pip_data as (
    select
   rider_id
,   case when pip_result = 'failed' then 'failing' else 'passing' end as pip_result
    from rider_pip
    where pip_status = 'running'
)

, base as(     
    select 
        l.task_id, 
        l.awb,
        l.status,
        l.rider_id,
        l.rider_name,
        l.cancellation_remarks,
        l.dispatch_time,
        date_trunc('day',l.dispatch_time) as dispatch_date,
        date_trunc('day',now()+interval '5.5 hours') as today_date,
        -- date_trunc('day',now()+interval '5.5 hours') as today_date,
        case when RANK() OVER ( PARTITION BY l.awb ORDER BY l.dispatch_time) = 1 then 'fresh' else 'reattempt' end as order_type,
        case when l.cod_amount = 0 then 'Prepaid' else 'COD' end as mop,
        city_name as shipping_city,
        node_name as hub,
        tr.is_realized,
        tr.is_collected,
        l.cod_amount,
        tr.is_fake_attempt
        
    from public.locus_task_brief as l
    -- left join ops_main
    -- on locus_task_brief.awb = ops_main.awb
    left join application_db.node as n on n.locus_home_base_id = l.location_id
    left join application_db.trip as tr on l.task_id = tr.locus_trip_id
    where 1=1
    and l.awb is not null
    and l.dispatch_time > now() - interval '30 days'
    and l.location_id <> 'homebase_test'
)
, numbers as (
select
        shipping_city
    ,   hub
    ,   rider_id
    ,   rider_name
    ,   count(case when dispatch_date=today_date then task_id else null end) as load
    ,   count(case when dispatch_date=today_date and status = 'COMPLETED' then task_id else null end) as delivered
    ,   count(case when dispatch_date=today_date and order_type = 'fresh' then task_id else null end) as fresh_attempted
    ,   count(case when dispatch_date=today_date and status = 'COMPLETED' and order_type = 'fresh' then task_id else null end) as fresh_delivered
    ,   count(case when dispatch_date=today_date and order_type = 'fresh' and mop = 'Prepaid' then task_id else null end) as prepaid_fresh_attempted
    ,   count(case when dispatch_date=today_date and status = 'COMPLETED' and order_type = 'fresh' and mop = 'Prepaid' then task_id else null end) as prepaid_fresh_delivered
    ,   count(case when dispatch_date=today_date and order_type = 'fresh' and mop = 'COD' then task_id else null end) as COD_fresh_attempted
    ,   count(case when dispatch_date=today_date and status = 'COMPLETED' and order_type = 'fresh' and mop = 'COD' then task_id else null end) as COD_fresh_delivered
    ,   count(case when dispatch_date <= today_date - interval '1 day' and status = 'CANCELLED' and is_realized = false then task_id else null end) as failed_collection_pendency
    ,   sum(case when dispatch_date <= today_date - interval '1 day' and status = 'COMPLETED' and is_collected = false then cod_amount else 0 end) as COD_collection_pendency
    ,   count(case when is_fake_attempt = 'true' then task_id else null end) as mtd_fake
    from base
    group by 
        shipping_city
    ,   hub
    ,   rider_id
    ,   rider_name)
    
,   final as (
    select 
        shipping_city
    ,   hub
    ,   numbers.rider_id
    ,   rider_name
    ,   load as assigned
    ,   delivered
    ,   delivered::double precision/nullif(load,0) as delivery_per
    ,   to_char((delivered::double precision/nullif(load,0))*100, '99%') as delivery_per_string
    ,   round((fresh_delivered::double precision/nullif(fresh_attempted,0))*100) as fasr
    ,   round((prepaid_fresh_delivered::double precision/nullif(prepaid_fresh_attempted,0))*100) as prepaid_fasr
    ,   round((cod_fresh_delivered::double precision/nullif(cod_fresh_attempted,0))*100) as cod_fasr
    ,   failed_collection_pendency
    ,   round(cod_collection_pendency) as cod_collection_pendency
    ,   case when mtd_fake = 0 then null else mtd_fake end as mtd_fake
    ,   pip_result
    from numbers
    left join pip_data as pip on pip.rider_id = numbers.rider_id    
    where load <> 0
)

select 
    rider_name
,   hub
,   assigned
,   delivered
,   delivery_per
,   delivery_per_string
,   case when delivery_per >= 0.8 then 1 else 0 end as delivery_flag
from 
final order by shipping_city, hub, fasr 