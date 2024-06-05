with pincode_rates_new as (
    Select      pincode
    ,           delivered_rate
    from public.delivery_rate_new2
)

, rider_info as (
	select 
		rider_name
	,	node_name
	,	locus_rider_id
	,	payout_structure
	,	vendor_name
	,   case    when trim(split_part(sort_codes,'/',1)) = 'BLR' then 'Bangalore'
                when trim(split_part(sort_codes,'/',1)) = 'HYD' then 'Hyderabad'
                when trim(split_part(sort_codes,'/',1)) = 'BOM' then 'Mumbai'
                when trim(split_part(sort_codes,'/',1)) = 'DEL' then 'Delhi'
                when trim(split_part(sort_codes,'/',1)) = 'NCR' then 'NCR'
                else 'other' end as shipping_city
	from application_db.rider as a 
	left join application_db.node as b on a.node_node_id = b.node_id
	left join application_db.vendor as c on c.vendor_id = a.vendor_id
)


, locus_data_prep as (
    SELECT                      locus_task_brief.awb,task_id,status, slot_start, locus_task_brief.rider_id,rider_phone,task_start_time,task_end_time,homebase,lm_team_name,completed_at,cancelled_at,cancellation_remarks,locus_task_brief.cod_amount,sla_delay,tour_id,dispatch_time
    ,							rider_info.rider_name
    ,                           rider_info.shipping_city as city
	,							rider_info.node_name as hub
    ,                           RANK() OVER ( PARTITION BY locus_task_brief.awb ORDER BY dispatch_time) as attempt_number
    ,                           case when locus_task_brief.cod_amount = 0 then 'Prepaid' else 'COD' end as mop
    ,                           shipment_order_details.user_id as user_id_id
    ,                           greatest(shipment_order_details.shipment_chargeable_weight, greatest(( "application_db"."shipment_weight"."length"*"application_db"."shipment_weight"."breadth"*"application_db"."shipment_weight"."height"/5000.0), "application_db"."shipment_weight"."weight")) as chargeable_weight
    ,                           shipment_order_details.shipping_pincode as pincode
	,							rider_info.payout_structure
	,							rider_info.vendor_name
    from                        locus_task_brief
    left join                   shipment_order_details
    ON                          locus_task_brief.awb = shipment_order_details.awb
    left join                   "application_db"."shipment_weight" on "public"."locus_task_brief"."awb" = "application_db"."shipment_weight"."shipment_awb"
	left join					rider_info on locus_task_brief.rider_id = rider_info.locus_rider_id
    where                       1=1
    and                         locus_task_brief.awb is not null
    and                         date_trunc('month',dispatch_time)::date = '2023-11-01'
    and                         node_name not like '%FRH%'
    and                         node_name not like '%Franchise%'
    and                         node_name <> 'GSCAN TEST'
)

, locus_task_query as (
    SELECT                      *
    ,                           case when attempt_number=1 then 'Fresh' else 'Reattempt' end as order_type
    
            
    FROM                        locus_data_prep
)

, base as (
    SELECT                      
            date_trunc('day',dispatch_time) as dispatch_date
    ,       city, hub, awb , rider_id, rider_name, status, task_id, payout_structure, vendor_name
    ,       chargeable_weight, delivered_rate, locus_task_query.pincode, pincode_rates_new.pincode
	,		case 	when status = 'COMPLETED' and payout_structure = 'ORDER' and user_id_id in (234,340,339,338,341) then 
        	            (case   when city = 'Delhi' then  25
        	                    when city = 'Mumbai' then 20
                                when city = 'Bangalore' then 22
                                else 0 end)
	                when status='COMPLETED' and payout_structure = 'ORDER' and city = 'Mumbai' then 30 + delivered_rate
	                when status='COMPLETED' and payout_structure = 'ORDER' and city = 'Delhi' then 20 + delivered_rate 
	                when status='COMPLETED' and payout_structure = 'ORDER' and city = 'Bangalore' then 22
	                else 0 end as delivery_incentive
	                
	,       case    when status='COMPLETED' and payout_structure = 'ORDER' and city = 'Bangalore' then (
	                        case    when chargeable_weight > 350 then 8
                                    when chargeable_weight > 600 then 13
                                    when chargeable_weight > 2100 then 28
                                    when chargeable_weight > 5100 then 68
                                    else 0 end)
	                when status='COMPLETED' and payout_structure = 'ORDER' and city = 'Mumbai' then (
							case    when chargeable_weight > 5000 then 60
									when chargeable_weight > 3000 then 30
									else 0 end)
					when status='COMPLETED' and payout_structure = 'ORDER' and city = 'Delhi' then (
							case    when chargeable_weight > 4000 then 25
									else 0 end)
					else 0 end as weight_incentive
    ,       case    when status='CANCELLED' and payout_structure = 'ORDER' and city = 'Mumbai' and LOWER(cancellation_remarks) not like '%operational constraint%' then 10 else 0 end as failed_delivery_incentive
    ,       case 	when date_part('hour',dispatch_time::timestamp)>=15 and payout_structure = 'ORDER' and status='COMPLETED' then 
                        (case   when city = 'Delhi' then 10
                                when city = 'Mumbai' then 15 else 0 end) else 0 end as evening_order_incentive

    FROM    locus_task_query
    LEFT JOIN  pincode_rates_new
    ON      locus_task_query.pincode = pincode_rates_new.pincode
)

, day_rider_view as (
SELECT      dispatch_date
,           city
,           rider_id
,           rider_name
,			hub
,			vendor_name
,			payout_structure
,           count(distinct case when status='COMPLETED' then task_id else null end) as delivered
,           count(distinct task_id) as attempted
,           sum(delivery_incentive) as delivery_pay
,           sum(weight_incentive) as heavy_pay
,           sum(failed_delivery_incentive) as vfd_pay
,           sum(evening_order_incentive) as evening_pay
,           case when payout_structure = 'ORDER' and count(distinct case when status='COMPLETED' then task_id else null end)>2 --no mg if less than 3 orders delivered
                                                                                        then (case when city = 'Mumbai' then 600
                                                                                                else 0 end )
                                                                                        else 0 end as mg

from        base
group by    1,2,3,4,5,6,7

)
, day_rider_view_modified as (
    SELECT  dispatch_date, city, rider_id, rider_name, hub, vendor_name, payout_structure, mg
	,		delivered, attempted, delivery_pay, heavy_pay, vfd_pay, evening_pay
    ,       case when (delivery_pay+vfd_pay+evening_pay+heavy_pay) > mg then 0 else (mg - (delivery_pay+vfd_pay+evening_pay+heavy_pay)) end as mg_payed
	,		case 	when payout_structure = 'FIXED' then (
						case 	when city = 'Mumbai' then (
									case 	when delivered <= 3 then delivered*45
											else 769 end)
								when city = 'Delhi' then (
									case when delivered <= 1 then delivered*30
									else 692 end)
								when city = 'Bangalore' then (
								    case when delivered <=3 then delivered*35
                                        when delivered <=20 then 690
                                        when delivered <=30 then 690 + (delivered-20)*10
                                        when delivered <= 40 then 790 + (delivered-30)*15
                                        when delivered <=50 then 790 + 150 + (delivered-40)*20
                                        else 790 + 350 + (delivered-50)*25 end	)
								when city = 'Hyderabad' then (
								    case when delivered < 1 then 0 
								        when delivered <= 15 then 750
								        else 750 + (delivered-15)*15 end)
						else 0 end )
			else 0 end as fixed_pay
    from    day_rider_view          
)

,	day_view as (
	    SELECT  dispatch_date, city, rider_id, rider_name, hub, vendor_name, payout_structure, mg
	,		delivered, attempted, delivery_pay, heavy_pay, vfd_pay, evening_pay, mg_payed, fixed_pay
	,	(delivery_pay + vfd_pay + evening_pay + mg_payed + fixed_pay + heavy_pay ) as total_pay
	from day_rider_view_modified

)

,	rider_month_view as (
		select 
			date_trunc('month',dispatch_date) as dispatch_month
	,		city, rider_id, rider_name, hub, vendor_name, payout_structure
	,		count(distinct(case when delivered >1 then dispatch_date else null end)) as attendance
	,		sum(delivered) as delivered
	,       sum(delivery_pay) as delivery_pay
	,       sum(heavy_pay) as heavy_pay
	,       sum(vfd_pay) as vfd_pay
	,       sum(evening_pay) as evening_pay
	,       sum(mg_payed) as mg_payed
	,		(sum(delivery_pay) + sum(vfd_pay) + sum(evening_pay) + sum(mg_payed) + sum(heavy_pay)) as total_variable_pay
	,		sum(fixed_pay) as fixed_pay
	,		case when count(distinct(dispatch_date)) >= 26 then 1000 else 0 end as attendance_incentive
	from day_view
	group by 1,2,3,4,5,6,7
)
,	month_view as (
	select
		dispatch_month, city, rider_id, rider_name, hub, vendor_name, payout_structure
	,	attendance , delivered
	,   delivery_pay , heavy_pay, vfd_pay, evening_pay
	,   mg_payed
	,	total_variable_pay
	,	fixed_pay
	,	attendance_incentive
	,	(total_variable_pay + fixed_pay + attendance_incentive) as total_pay
	from rider_month_view
)

, leaves_data as (
    select -- leaves data
        city
    ,   rider_id    
    ,   dates
    from (
            select ---data with all month dates for rider with delivered orders
                a.city
            ,   a.rider_id
            ,   a.dates
            ,   delivered 
            from (
                    select -- data with all month dates for rider
                        city
                    ,   rider_id
                    ,   dates
                    from month_view
                    cross join (select * from generate_series('2023-11-01','2023-11-30',interval '1 day') as dates) as generated_date) as a
            left join day_view as b on a.city = b.city and a.rider_id=b.rider_id and a.dates = b.dispatch_date) as rider_attendance_data
    where delivered is null
)

, incentive_larger as (
    select 
        city
    ,   rider_id
    from leaves_data
    where dates between (case when city='Mumbai' then '2023-11-08'::date
                                when city = 'Bangalore' then '2023-11-06'::date
                                when city = 'Hyderabad' then '2023-11-07'::date
                                else '2023-01-01' end)
                and
                        (case when city='Mumbai' then '2023-11-19'::date
                                when city = 'Bangalore' then '2023-11-19'::date
                                when city = 'Hyderabad' then '2023-11-19'::date
                                else '2023-01-01' end)
    and city <> 'Delhi'
    group by 1,2
)

, incentive_smaller as (
    select 
        city
    ,   rider_id
    from leaves_data
    where dates between (case when city='Mumbai' then '2023-11-08'::date
                                when city = 'Bangalore' then '2023-11-10'::date
                                when city = 'Hyderabad' then '2023-11-10'::date
                                else '2023-01-01' end)
                and
                        (case when city='Mumbai' then '2023-11-16'::date
                                when city = 'Bangalore' then '2023-11-16'::date
                                when city = 'Hyderabad' then '2023-11-16'::date
                                else '2023-01-01' end)
    and city <> 'Delhi'
    group by 1,2
)

, incentives as (
select * from 
(
    select 
        city
    ,   rider_id
    ,   '1000' as amount
    ,   'shorter-consecutive-attendance' as type
    from month_view
    where rider_id not in (select rider_id from incentive_smaller)
    and city <> 'Delhi') as a
union
(
    select 
        city
    ,   rider_id
    ,   '1000' as amount
    ,   'longer-consecutive-attendance' as type
    from month_view
    where rider_id not in (select rider_id from incentive_larger)
    and city <> 'Delhi')
)

-- SELECT                          *

-- -- from                            base
-- -- from                            day_view
-- -- from							month_view
-- -- from                            day_wise_working
-- -- from                            incentives
-- from leaves_data
-- where                           1=1
-- and         rider_id = 'DS_BOM_KRL_22082022_0003'
-- and         dispatch_date = '2023-10-01'
-- and         city = 'Delhi'
-- order by city desc 

select ---data with all month dates for rider with delivered orders
                a.city
            ,   a.rider_id
            ,   a.dates
            ,   delivered 
            from (
                    select -- data with all month dates for rider
                        city
                    ,   rider_id
                    ,   dates
                    from month_view
                    cross join (select * from generate_series('2023-11-01','2023-11-30',interval '1 day') as dates) as generated_date) as a
            left join day_view as b on a.city = b.city and a.rider_id=b.rider_id and a.dates = b.dispatch_date