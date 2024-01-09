with locus_task as(     
    select 
        task_id
    ,   locus_task_brief.awb
    ,   status
    ,   rider_id
    ,   task_start_time
    ,   cancellation_remarks
    ,   slot_start
    ,   dispatch_time
    ,   dispatch_time::date as dispatch_date
    ,   RANK() OVER ( PARTITION BY locus_task_brief.awb ORDER BY dispatch_time) as attempt_number
    ,   case when cod_amount = 0 then 'Prepaid' else 'COD' end as mop
    ,   case    
        when extract(hour from dispatch_time) <= 12 then 'Morning'
        else 'Evening' end as slot
    ,   case    
        when extract(hour from dispatch_time) <= 12 then 1
        else 2 end as slot_number
    ,    shipping_city
    from public.locus_task_brief
    left join ops_main
    on locus_task_brief.awb = ops_main.awb
    where 1=1
    and locus_task_brief.awb is not null
    and dispatch_time > now() - interval '3 months'
    and location_id <> 'homebase_test'
),

rank_table as(
    select 
        *
    ,   case when attempt_number=1 then 'Fresh' else 'Reattempt' end as order_type
        from locus_task 
        
        where 1=1
)

, base as(
    select
        dispatch_date
    ,   shipping_city
    ,   slot as slot2
    ,   slot_number
    ,   rider_id

    ,   sum(case when order_type = 'Fresh' then 1 else 0 end) as fresh_attempted
    ,   sum(case when order_type = 'Fresh' and status = 'COMPLETED' then 1 else 0 end) as fresh_delivered

    ,   sum(case when order_type = 'Fresh' and mop = 'Prepaid' then 1 else 0 end) as fresh_prepaid_attempted
    ,   sum(case when order_type = 'Fresh' and status = 'COMPLETED' and mop = 'Prepaid' then 1 else 0 end) as fresh_prepaid_delivered

    ,   sum(case when order_type = 'Fresh' and mop = 'COD' then 1 else 0 end) as fresh_cod_attempted
    ,   sum(case when order_type = 'Fresh' and status = 'COMPLETED' and mop = 'COD' then 1 else 0 end) as fresh_cod_delivered
    
    
    ,   count(task_id) as total_attempted
    ,   sum(case when status = 'COMPLETED' then 1 else 0 end) as total_delivered
    

    ,   count(distinct slot) as trips
        from rank_table
        where 1=1
        group by dispatch_date, shipping_city, slot, slot_number, rider_id
)

, rider_info as (
    select
        locus_rider_id
    ,   rider_name
    ,   node_name
    ,   a.created_at
    ,   vendor_name
    ,   case when lower(node_name) like '%frh%'or lower(node_name) like '%franchise%' then 'franchise' else 'blitz' end as hub_type
    from application_db.rider as a
    left join application_db.node as b
    on a.node_node_id = b.node_id 
    left join application_db.vendor as c
    on c.vendor_id = a.vendor_id

)

, weekly_rider_data as (
select 
        shipping_city
    ,   date_trunc('week',dispatch_date) as dispatch_week
    ,   rider_id
    ,   sum(total_attempted) as "Assigned"
    ,   cast(sum(fresh_delivered) as float)/nullif(sum(fresh_attempted),0) as "FASR%"
    ,   cast(sum(case when slot2 = 'Morning' then fresh_delivered else null end) as float)/nullif(sum(case when slot2 = 'Morning' then fresh_attempted else null end),0) as "Morning FASR%"
    ,   cast(sum(case when slot2 = 'Evening' then fresh_delivered else null end) as float)/nullif(sum(case when slot2 = 'Evening' then fresh_attempted else null end),0) as "Evening FASR%"
    ,   cast(sum(case when slot2 = 'Morning' then fresh_prepaid_delivered else null end) as float)/nullif(sum(case when slot2 = 'Morning' then fresh_prepaid_attempted else null end),0) as "Morning Prepaid FASR%"
    ,   cast(sum(case when slot2 = 'Evening' then fresh_prepaid_delivered else null end) as float)/nullif(sum(case when slot2 = 'Evening' then fresh_prepaid_attempted else null end),0) as "Evening Prepaid FASR%"
    ,   cast(sum(case when slot2 = 'Morning' then fresh_COD_delivered else null end) as float)/nullif(sum(case when slot2 = 'Morning' then fresh_COD_attempted else null end),0) as "Morning COD FASR%"
    ,   cast(sum(case when slot2 = 'Evening' then fresh_COD_delivered else null end) as float)/nullif(sum(case when slot2 = 'Evening' then fresh_COD_attempted else null end),0) as "Evening COD FASR%"
    ,   cast(sum(case when slot2 = 'Morning' then fresh_prepaid_delivered else null end) as float)/nullif(count(distinct(case when slot2='Morning' then dispatch_date else null end)),0) as avg_mn_pre_orders
    ,   cast(sum(case when slot2 = 'Morning' then fresh_COD_delivered else null end) as float)/nullif(count(distinct(case when slot2='Morning' then dispatch_date else null end)),0) as avg_mn_cod_orders
    ,   cast(sum(case when slot2 = 'Evening' then fresh_prepaid_delivered else null end) as float)/nullif(count(distinct(case when slot2='Evening' then dispatch_date else null end)),0) as avg_ev_pre_orders
    ,   cast(sum(case when slot2 = 'Evening' then fresh_COD_delivered else null end) as float)/nullif(count(distinct(case when slot2='Evening' then dispatch_date else null end)),0) as avg_ev_cod_orders
    -- ,   trips

from base

where 1=1
-- and date_trunc('week',dispatch_date)::date = '2023-10-23'
and shipping_city is not null
and total_delivered >=3
group by 
        shipping_city
    ,   date_trunc('week',dispatch_date)
    ,   rider_id
order by shipping_city, dispatch_week, rider_id
)

, weekly_rider_data_flag as(
        select 
                    dispatch_week
                ,   shipping_city
                ,   rider_id
                ,   rider_name
                ,   node_name as hub
                ,   vendor_name
                ,   date_part('day',dispatch_week - created_at) as age
                ,   "FASR%" as fasr
                ,   "Morning Prepaid FASR%"
                ,   "Morning COD FASR%"
                ,   "Evening Prepaid FASR%"
                ,   "Evening COD FASR%"
                ,   avg_mn_pre_orders
                ,   avg_mn_cod_orders
                ,   avg_ev_pre_orders
                ,   avg_ev_cod_orders
                ,   case when "Morning Prepaid FASR%" < 0.85 then 1 else 0 end as MN_pre_flag
                ,   case when "Morning COD FASR%" < 0.6 then 1 else 0 end as MN_cod_flag
                ,   case when "Evening Prepaid FASR%" < 0.8 then 1 else 0 end as EV_pre_flag
                ,   case when "Evening COD FASR%" < 0.5 then 1 else 0 end as EV_cod_flag
                ,   "FASR%"
        from weekly_rider_data
        left join rider_info
        on weekly_rider_data.rider_id = rider_info.locus_rider_id
        where hub_type = 'blitz'
)

,   pip_data as (
    select
        concat(to_char(b.dispatch_week + interval '1 week','YYYYMMDD'),'-',b.rider_id) as key
    ,   b.dispatch_week + interval '1 week' as pip_date
	,   b.shipping_city
    ,   b.rider_id
    ,   b.rider_name	
    ,   b.hub	
    ,   b.vendor_name	
    ,   b.age as rider_age

    ,   concat((case when b.MN_pre_flag = 0 then '' else concat('Morning Prepaid (', (b."Morning Prepaid FASR%"*100)::int::text ,'%) ') end)
			   ,(case when b.MN_COD_flag = 0 then '' else concat('Morning COD(', (b."Morning COD FASR%"*100)::int::text ,'%) ') end)
			   ,(case when b.EV_pre_flag = 0 then '' else concat('Morning Prepaid (', (b."Evening Prepaid FASR%"*100)::int::text ,'%) ') end)
			   ,(case when b.EV_COD_flag = 0 then '' else concat('Morning Prepaid (', (b."Evening COD FASR%"*100)::int::text ,'%) ') end)) as need_improvement
    
    ,   trunc(b.fasr*100) as starting_fasr
	,   trunc(c.fasr*100) as closing_fasr
    ,   'running' as pip_status	
    ,   '' as pip_result
    from weekly_rider_data_flag as b
    left join weekly_rider_data_flag as c on b.dispatch_week + interval '1 week' = c.dispatch_week and b.rider_id = c.rider_id

    where 1=1 
    and b."FASR%" < 0.84
    and (   (b.MN_pre_flag = 1 and b.avg_mn_pre_orders >= 3)
            or
            (b.MN_COD_flag = 1 and b.avg_mn_cod_orders >= 3)
            or
            (b.EV_pre_flag = 1 and b.avg_ev_pre_orders >= 3)
            or
            (b.EV_cod_flag = 1 and b.avg_ev_cod_orders >= 3) )
    and b.age >= 10
    -- and b.dispatch_week + interval '1 week' = date_trunc('week',now()+interval '5.5 hours')
    and b.dispatch_week + interval '1 week' = date_trunc('week',now()+interval '5.5 hours'+interval '1 week')


)

select 
    key,
    to_char(pip_date,'YYYY-MM-DD'),
    rider_id,
	shipping_city,
    rider_name,
    hub,
    vendor_name,
    rider_age::integer,
	need_improvement,
    starting_fasr,
    closing_fasr,
    pip_status,
    pip_result
from pip_data order by shipping_city, hub, starting_fasr