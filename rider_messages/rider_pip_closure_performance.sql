with locus_task as(     
	select 
		task_id
	,   locus_task_brief.awb
	,	tour_id
	,   status
	,   rider_id
	,   task_start_time
	,   cancellation_remarks
	,   slot_start
	,   dispatch_time
	,   dispatch_time::date as dispatch_date
	,	date_trunc('week',dispatch_time) as dispatch_week
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
	and dispatch_time > now() - interval '1 months'
	and date_trunc('day',dispatch_time) <= case when extract(hour from now()+interval '5.5 hours') < 22 then current_date - interval '1 day' else current_date end
	and location_id <> 'homebase_test'
)

, bad_tours as (
    select
        rider_id
    ,	tour_id
    from 
            (select
                rider_id	
            ,	tour_id
            ,	sum(case when status='COMPLETED' then 1 else 0 end) as delivered	
            from locus_task
            group by 1,2) sub_table
    where delivered = 0
)

, rank_table as(
    select 
        *
    ,   case when attempt_number=1 then 'Fresh' else 'Reattempt' end as order_type
        from locus_task


        where 1=1
        and tour_id not in (select tour_id from bad_tours)
)

,   pip_data as (
    select
            pip_date,
        	shipping_city,
            rider_id,
            rider_name,
            hub,
	    	CONCAT(p.time_of_day, '-', p.payment_type) AS performance_type,
			p.threshold

    from rider_pip
	CROSS JOIN
    (VALUES ('Morning', 'Prepaid',0.85), ('Morning', 'COD',0.6), ('Evening', 'Prepaid',0.8), ('Evening', 'COD',0.5)) AS p(time_of_day, payment_type, threshold)

    where 1=1
    and pip_date = date_trunc('week',now()+interval '5.5 hour') - interval '1 week'
)

,   performance_data as (
    select
        a.dispatch_week
	,	a.dispatch_date
    ,   a.shipping_city
    ,   a.rider_id
    ,   concat(slot,'-',mop) as type
    ,   trunc((cast(sum(case when order_type = 'Fresh' and status = 'COMPLETED' then 1 else 0 end) as float)/nullif(sum(case when order_type = 'Fresh' then 1 else 0 end),0))::numeric,2) as fasr
    ,	sum(case when order_type = 'Fresh' and status = 'COMPLETED' then 1 else 0 end) as fresh_delivered
	,	sum(case when order_type = 'Fresh' then 1 else 0 end) as fresh_attempted
	
    from rank_table as a
    group by 1,2,3,4,5
)

,	performance_data_pivot as (
	select
		dispatch_week
	,	shipping_city
	,	rider_id
	,	type
	,	sum(case when dispatch_date = dispatch_week then fasr else null end) as monday
	,	sum(case when dispatch_date = dispatch_week + interval '1 day' then fasr else null end) as tuesday
	,	sum(case when dispatch_date = dispatch_week + interval '2 day' then fasr else null end) as wednesday
	,	sum(case when dispatch_date = dispatch_week + interval '3 day' then fasr else null end) as thursday
	,	sum(case when dispatch_date = dispatch_week + interval '4 day' then fasr else null end) as friday
	,	sum(case when dispatch_date = dispatch_week + interval '5 day' then fasr else null end) as saturday
	,	sum(case when dispatch_date = dispatch_week + interval '6 day' then fasr else null end) as sunday
	,	trunc((cast(sum(fresh_delivered) as float)/nullif(sum(fresh_attempted),0))::numeric,2) as avg_fasr
	from performance_data
	group by 1,2,3,4
)

,   final_performance_data as (
    select 
        a.pip_date
    ,   a.shipping_city
    ,   a.rider_id
    ,   a.rider_name
    ,   a.hub
    ,   a.performance_type
	,	a.threshold
    ,	b.monday   
	,	b.tuesday
	,	b.wednesday
	,	b.thursday
	,	b.friday
	,	b.saturday
	,	b.sunday
    ,	b.avg_fasr
	,	case when b.avg_fasr < a.threshold or avg_fasr is null then 'fail' else 'pass' end as status
    
    from pip_data as a
    left join performance_data_pivot b on a.rider_id = b.rider_id and a.pip_date = b.dispatch_week and a.performance_type = b.type
    
    order by    a.pip_date, a.shipping_city, a.rider_id, a.performance_type desc


)
select * from final_performance_data
order by  pip_date,shipping_city,hub,rider_name,performance_type desc