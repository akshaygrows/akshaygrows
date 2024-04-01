with pip_data as (
    select
   rider_id
,   case when pip_result = 'failed' then 'failing' else 'passing' end as pip_result
    from rider_pip
    where pip_status = 'running'
	and pip_date = date_trunc('week',now()+interval '5.5 hours')
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
		-- change below for date
        -- date_trunc('day',now()+interval '5.5 hours') - interval '1 day' as today_date,
        date_trunc('day',now()+interval '5.5 hours') as today_date,
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
    and date_trunc('month',l.dispatch_time) = date_trunc('month',now() + interval '5.5 hours')
    and l.location_id <> 'homebase_test'
)
, fake_data as (
    SELECT
        rider_id,
        MAX(CASE WHEN rn = 1 THEN awb END) AS fr_1,
        MAX(CASE WHEN rn = 2 THEN awb END) AS fr_2,
        MAX(CASE WHEN rn = 3 THEN awb END) AS fr_3,
        MAX(CASE WHEN rn = 4 THEN awb END) AS fr_4,
        MAX(CASE WHEN rn = 5 THEN awb END) AS fr_5,
        MAX(CASE WHEN rn = 6 THEN awb END) AS fr_6,
        MAX(CASE WHEN rn = 7 THEN awb END) AS fr_7,
        MAX(CASE WHEN rn = 8 THEN awb END) AS fr_8,
        MAX(CASE WHEN rn = 9 THEN awb END) AS fr_9,
        MAX(CASE WHEN rn = 10 THEN awb END) AS fr_10
    FROM
        (
        select
            rider_id
        ,   task_id
        ,   awb
        ,   is_fake_attempt
        ,   ROW_NUMBER() OVER (PARTITION BY rider_id ORDER BY dispatch_time) AS rn
        from base 
        where is_fake_attempt = 'true'
        and date_trunc('day',dispatch_time) = date_trunc('day',now()+interval '5.5 hours')
        ) as fake_numbering
    WHERE rn <= 10
    GROUP BY rider_id
)
, cod_pendency as (
	select 
		rider_id
																						--change below for dates
	,	sum(case when delivery_date = date_trunc('day',now()+interval '5.5 hours') then amount else 0 end) as today_cod
	,	sum(case when delivery_date <= date_trunc('day',now()+interval '5.5 hours'-interval '1 day') then amount else 0 end) as past_cod
	from cod_base_data
	where collected = 0
	group by 1
)

, lost_data as (
	select 
		rider_id
	,	sum(shipment_value) as lost_amount
	,	count(awb) as lost_count
	from lost_attribution
	where date_trunc('month',lost_date) = date_trunc('month',now()+interval '5.5 hours'-interval '1 day')
	group by 1
)

,   payout_data as (
    select 
        r.locus_rider_id as rider_id
    ,   sum(amount) as monthly_earning
    ,   sum(case when trip_date = date_trunc('day',now()+interval '5.5 hours') then amount else 0 end) as today_earning

    from application_db.rider_payout as rp
    left join application_db.rider as r on r.rider_id = rp.rider_id
    where date_trunc('month',trip_date)=date_trunc('month',now()+interval '5.5 hours')
    group by 1
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
    ,   count(case when is_fake_attempt = 'true' then task_id else null end) as mtd_fake
    ,   count(case when is_fake_attempt = 'true' and dispatch_date = date_trunc('day',now()+interval '5.5 hours') then task_id else null end) as today_fake
	,	count(case when is_realized = 'false' and status = 'CANCELLED' and dispatch_date = today_date then task_id else null end) as today_fd_pendency
	,	count(case when is_realized = 'false' and status = 'CANCELLED' and dispatch_date < today_date then task_id else null end) as past_fd_pendency
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
    ,   to_char((delivered::double precision/nullif(load,0))*100, '999%') as delivery_per_string
    
    ,   (fresh_delivered::double precision/nullif(fresh_attempted,0))*100 as fasr
    ,   prepaid_fresh_delivered::double precision/nullif(prepaid_fresh_attempted,0) as prepaid_fasr_per
    ,   cod_fresh_delivered::double precision/nullif(cod_fresh_attempted,0) as cod_fasr_per
    ,   to_char((prepaid_fresh_delivered::double precision/nullif(prepaid_fresh_attempted,0))*100, '999%') as prepaid_fasr_per_string
    ,   to_char((cod_fresh_delivered::double precision/nullif(cod_fresh_attempted,0))*100, '999%') as cod_fasr_per_string
	
    ,	today_fd_pendency
	,	past_fd_pendency
	,	case when today_cod is null then 0 else today_cod end as today_cod
	,	case when past_cod is null then 0 else past_cod end as past_cod
	,	case when today_cod <> 0 then to_char(today_cod,'₹99,99,99,999') else '0' end as today_cod_string
	,	case when past_cod <> 0 then to_char(past_cod,'₹99,99,99,999') else '0' end as past_cod_string
	
    ,	lost_amount
    ,   case when lost_amount is null then '-' else to_char(lost_amount,'₹99,99,999') end as lost_amount_string
	,	case when lost_count is null then 0 else lost_count end as lost_count
    
    ,   case when monthly_earning is null then '-' else to_char(monthly_earning,'₹99,99,999') end as monthly_earning
    ,   case when today_earning is null then '-' else to_char(today_earning,'₹99,99,999') end as today_earning
    
    ,   mtd_fake
    ,   today_fake
    ,   case when fr_1 is null then '' else fr_1 end as fr_1 
    ,   case when fr_2 is null then '' else fr_2 end as fr_2 
    ,   case when fr_3 is null then '' else fr_3 end as fr_3 
    ,   case when fr_4 is null then '' else fr_4 end as fr_4 
    ,   case when fr_5 is null then '' else fr_5 end as fr_5 
    ,   case when fr_6 is null then '' else fr_6 end as fr_6 
    ,   case when fr_7 is null then '' else fr_7 end as fr_7 
    ,   case when fr_8 is null then '' else fr_8 end as fr_8 
    ,   case when fr_9 is null then '' else fr_9 end as fr_9 
    ,   case when fr_10 is null then '' else fr_10 end as fr_10 

    ,   pip_result
	
    from numbers
    left join pip_data as pip on pip.rider_id = numbers.rider_id    
	left join cod_pendency as cod on cod.rider_id = numbers.rider_id
	left join lost_data on lost_data.rider_id = numbers.rider_id
    left join payout_data on payout_data.rider_id = numbers.rider_id
    left join fake_data on fake_data.rider_id = numbers.rider_id
    where load <> 0
)
, final2 as (
    select 
        rider_name
    ,   hub
    ,   assigned
    ,   delivered
    ,   delivery_per
    ,   delivery_per_string
    ,   case when delivery_per >= 0.8 then 1 else 0 end as delivery_flag
    ,   prepaid_fasr_per
    ,   cod_fasr_per
    ,   prepaid_fasr_per_string
    ,   cod_fasr_per_string
    ,   case when prepaid_fasr_per > 0.9 and cod_fasr_per > 0.7 then 1 else 0 end as fasr_flag
    ,	today_cod_string
    ,   today_cod
    ,	past_cod_string
    ,   past_cod
    ,	today_fd_pendency
    ,	past_fd_pendency
    ,   case when past_fd_pendency = 0 and past_cod = 0 then 1 else 0 end as pendency_flag
    -- ,	lost_amount
    ,   lost_amount_string
    ,	lost_count
    ,	case when lost_amount >= 1000 then 0 else 1 end as lost_flag
    ,   today_earning
    ,   monthly_earning
    ,   mtd_fake
    ,   today_fake
    ,   fr_1
    ,   fr_2
    ,   fr_3
    ,   fr_4
    ,   fr_5
    ,   fr_6
    ,   fr_7
    ,   fr_8
    ,   fr_9
    ,   fr_10
    ,   case when today_fake::double precision/nullif(assigned,0) < 0.001 and mtd_fake <= 10 then 1 else 0 end as fake_flag
    ,	case when pip_result is null then 'no_pip' else pip_result end as pip_result

    from 
    final 
    where 1=1
    -- and today_fake >1 
    order by shipping_city, hub, fasr desc
-- limit 3
)
select 
    * 
,	case when delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag =0 then 1 else 0 end as star_score_0
,   case when delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag =1 then 1 else 0 end as star_score_1
,   case when delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag =2 then 1 else 0 end as star_score_2
,   case when delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag =3 then 1 else 0 end as star_score_3
,   case when delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag =4 then 1 else 0 end as star_score_4
,   case when delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag =5 then 1 else 0 end as star_score_5
,	case 
		when delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag = 0 then 
			'Improvement required, focus on all areas.'
		when delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag = 1 then 
			'Below expectations, work on improvement.'
		when delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag = 2 then 
			'Improvement needed, aim for better.'
		when delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag = 3 then 
			'Average performance, keep improving.'
		when delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag = 4 then 
			'Great job, 1 more star to go!'
		when delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag = 5 then 
			'Outstanding performance, top-notch work!!'
		end as star_remark
,	delivery_flag+fasr_flag+pendency_flag+lost_flag+fake_flag as final_rating
from final2