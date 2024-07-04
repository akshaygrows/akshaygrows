insert into payout_day
with pincode_rates_new as (
    Select      pincode
    ,           delivered_rate
    from public.delivery_rate_new2
)

, rider_info as (
	select 
		a.rider_name
	,   node_name
	,	a.rider_id
	,   b.city_name as city_name
	,   case when d.payout_structure is null then a.payout_structure
	        else d.payout_structure end as payout_structure
    ,   c.vendor_name
	from application_db.rider as a 
	left join application_db.node as b on a.node_node_id = b.node_id
	left join application_db.vendor as c on c.vendor_id = a.vendor_id
	left join public.temp_rider_config_june1 as d on d.rider_id = a.locus_rider_id
)

, shipment_weight as (
    select 
        distinct on (shipment_awb) shipment_awb
    ,   max(length*breadth*height) as dwh_volume
    ,   max(weight) as dwh_weight
    from application_db.shipment_weight 
    group by 1
)

, locus_data_prep as (
    SELECT                      kaptaan.awb,trip_id,status, slot_start, kaptaan.num_rider_id as rider_id,rider_phone,completed_at,cancelled_at,cancellation_remarks,kaptaan.cod_amount,tour_id,dispatch_time
    ,							kaptaan.rider_name
    ,                           kaptaan.shipping_city as city
	,							kaptaan.node_name as hub
    ,                           RANK() OVER ( PARTITION BY kaptaan.awb ORDER BY dispatch_time) as attempt_number
    ,                           case when kaptaan.cod_amount = 0 then 'Prepaid' else 'COD' end as mop
    ,                           shipment_order_details.user_id as user_id_id
    ,                           greatest(shipment_order_details.shipment_chargeable_weight, greatest(( dwh_volume/5000.0),dwh_weight)) as chargeable_weight
    ,                           case when extract(hour from dispatch_time) >= 15 then 'evening' else 'morning' end as slot
	,							rider_info.payout_structure
	,							rider_info.vendor_name
	,                           kaptaan.pincode::int
    from                        kaptaan
    left join                   shipment_order_details
    ON                          kaptaan.awb = shipment_order_details.awb
    left join                   shipment_weight as w on public.kaptaan.awb = w.shipment_awb
	left join					rider_info on kaptaan.num_rider_id = rider_info.rider_id
    where                       1=1
    and                         kaptaan.awb is not null
    and                         date_trunc('month',dispatch_time)::date = '2024-06-01'
    and                         kaptaan.node_name not like '%FRH%'
    and                         kaptaan.node_name not like '%Franchise%'
    and                         kaptaan.node_name not in  ('GSCAN TEST','BLR_OFSC')
	and							kaptaan.shipping_city in ('Bangalore','Hyderabad','Mumbai')
	and							dispatch_time::date <= '2024-06-16'
)

, locus_task_query as (
    SELECT                      *
    ,                           case when attempt_number=1 then 'Fresh' else 'Reattempt' end as order_type
    
            
    FROM                        locus_data_prep
)

, fasr as (
    select 
        rider_id
    ,   sum(case when order_type = 'Fresh' and status = 'COMPLETED' then 1 else 0 end) as fresh_delivered
    ,   sum(case when order_type = 'Fresh' then 1 else 0 end) as fresh_attempted
    from locus_task_query
    group by 1 
)

, base as (
    SELECT                      
            date_trunc('day',dispatch_time) as dispatch_date
    ,       city, hub, awb , rider_id, rider_name, status, trip_id, payout_structure, vendor_name
    ,       chargeable_weight, delivered_rate, locus_task_query.pincode, slot
	,		case 	when status = 'COMPLETED' and payout_structure = 'ORDER'  then 0 else 0 end as org_id_pay
    ,       case    when status = 'COMPLETED' and payout_structure = 'ORDER' then 
                    (case   when city = 'Mumbai' then 26
        	                when city = 'Delhi' then 20 + delivered_rate 
        	                when city = 'Bangalore' then 27
        	                when city = 'Hyderabad' then 30
        	                else 0 end )
        	       else 0 end as delivery_incentive
	,       case    
	                when status='COMPLETED' and payout_structure = 'ORDER' and city = 'Mumbai' then (
							case    
							        when chargeable_weight > 5000 then 60
									when chargeable_weight > 3000 then 30
									else 0 end)
					when status='COMPLETED' and payout_structure = 'ORDER' and city = 'Delhi' then (
							case    when chargeable_weight > 4000 then 30
									else 0 end)
					else 0 end as weight_incentive
    ,       case    when status='CANCELLED' and payout_structure = 'ORDER' and city = 'Mumbai' and LOWER(cancellation_remarks) not like '%operational constraint%' then 0 else 0 end as failed_delivery_incentive
    ,       case 	when date_part('hour',dispatch_time::timestamp)>=15 and payout_structure = 'ORDER' and status='COMPLETED' then 
                        (case   when city = 'Delhi' then 5
                                else 0 end) else 0 end as evening_order_incentive
	,	order_type

    FROM    locus_task_query
    LEFT JOIN  pincode_rates_new
    ON      locus_task_query.pincode = pincode_rates_new.pincode
)

, day_rider_view as (
SELECT      dispatch_date
,           city
,           hub
,           rider_id
,			payout_structure
,           count(distinct case when status='COMPLETED' then trip_id else null end) as delivered
,           count(distinct(case when status='COMPLETED' and slot='morning' then trip_id else null end)) as morning_delivered
,           count(distinct(case when status='COMPLETED' and slot='evening' then trip_id else null end)) as evening_delivered
,			count(distinct(case when status='COMPLETED' and order_type='fresh' then trip_id else null end)) as fresh_delivered
,			count(distinct(case when order_type='fresh' then trip_id else null end)) as fresh_attempted
,           count(distinct trip_id) as attempted
,           sum(org_id_pay) as org_id_pay
,           sum(delivery_incentive) as delivery_pay
,           sum(weight_incentive) as heavy_pay
,           sum(failed_delivery_incentive) as vfd_pay
,           sum(evening_order_incentive) as evening_pay
,           sum(org_id_pay)+sum(delivery_incentive)+sum(weight_incentive)+sum(failed_delivery_incentive)+sum(evening_order_incentive) as earning_pay
from        base
group by    1,2,3,4,5

)
, day_rider_view_modified as (
    SELECT  dispatch_date, city, hub, rider_id, payout_structure
	,		delivered,morning_delivered,evening_delivered, attempted
    ,       delivery_pay , heavy_pay, vfd_pay, evening_pay,  org_id_pay
	,		delivery_pay+heavy_pay+vfd_pay+evening_pay+org_id_pay as earning_pay
	,		fresh_delivered::float/nullif(fresh_attempted,0) as fasr
    ,       case when payout_structure = 'ORDER' then
                    case    --full day mg
                        when city = 'Delhi' and delivered >= 10 then 500  
                        when city = 'Mumbai' and delivered >= 5 and hub = 'DS BOM KDWL' then 750
                        when city = 'Mumbai' and hub <> 'DS BOM KDWL' and delivered >= 5 then 600
                        when city = 'Hyderabad' and delivered >= 3 then 600
                            -- evening mg
                        when city = 'Bangalore' and evening_delivered >= 5 then 500
                            --no other mgs    
                        else 0 end
                else 0 end as order_mg
	,		case 	when payout_structure = 'FIXED' then (
						case 	when city = 'Mumbai' then (
									case 	when delivered <= 3 then delivered*45
											else 642 end)
								when city = 'Delhi' then (
									case when delivered <= 5 then delivered*50
									    when delivered <=20 then 640
                                        when delivered <=30 then 640 + (delivered-20)*10
                                        when delivered <= 40 then 640 + 100 + (delivered-30)*15
                                        when delivered <=50 then 640 + 100 + 150 + (delivered-40)*20
                                        else 640 + 100 + 150 + 200 end)
								when city = 'Bangalore' then (
								    case when delivered <=5 then delivered*35
                                        when delivered <=20 then 690
                                        when delivered <=30 then 690 + (delivered-20)*5
                                        when delivered <= 40 then 740 + (delivered-30)*10
                                        when delivered <=50 then 740 + 100 + (delivered-40)*15
                                        else 740 + 250 + (delivered-50)*20 end	)
								when city = 'Hyderabad' then (
								    case when delivered <= 3 then delivered*45 
								        when delivered <= 15 then 750
								        else 750 + (delivered-15)*15 end)
								when city = 'Jaipur' then (
								    case when delivered <=2 then delivered*30
								        when delivered <=20 then 540
                                        when delivered <=30 then 540 + (delivered-20)*10
                                        when delivered <= 40 then 540 + 100 + (delivered-30)*15
                                        when delivered <=50 then 540 + 100 + 150 + (delivered-40)*20
                                        else 540 + 100 + 150 + 200 end)
						else 0 end )
			else 0 end as fixed_pay
    from    day_rider_view          
)

,	day_view as (
	    SELECT  dispatch_date, city,hub,rider_id, payout_structure
	,		delivered,morning_delivered,evening_delivered, attempted
	, org_id_pay, delivery_pay, heavy_pay, vfd_pay, evening_pay, fasr
	, earning_pay as order_earning , order_mg
	, case when earning_pay < order_mg then order_mg - earning_pay else 0 end as mg_payed
	, fixed_pay
	, earning_pay + (case when earning_pay < order_mg then order_mg - earning_pay end) + fixed_pay as total_pay
	from day_rider_view_modified

)

,	rider_month_view as (
		select 
			date_trunc('month',dispatch_date) as dispatch_month
	,		city, rider_id, hub, payout_structure
	,		count(distinct(case when delivered >= 3 then dispatch_date else null end)) as attendance
	,		sum(delivered) as delivered
	,       sum(org_id_pay) as org_id_pay
	,       sum(delivery_pay) as delivery_pay
	,       sum(heavy_pay) as heavy_pay
	,       sum(vfd_pay) as vfd_pay
	,       sum(evening_pay) as evening_pay
	,       sum(mg_payed) as mg_payed
	,		(sum(org_id_pay) + sum(delivery_pay) + sum(vfd_pay) + sum(evening_pay) + sum(mg_payed) + sum(heavy_pay)) as total_variable_pay
	,		sum(fixed_pay) as fixed_pay
	,		case when count(distinct(case when delivered > 3 then dispatch_date else null end)) >= (case when city in ('Mumbai','Bangalore') then 28 else 26 end)
	            then 1000 else 0 end as attendance_incentive
	from day_view
	group by 1,2,3,4,5
)
,	month_view as (
	select
		dispatch_month, city, a.rider_id, rider_name, hub, vendor_name, a.payout_structure
	,	attendance , delivered
	,   org_id_pay, delivery_pay , heavy_pay, vfd_pay, evening_pay
	,   mg_payed
	,	total_variable_pay
	,	fixed_pay
	,	attendance_incentive
	,	(total_variable_pay + fixed_pay + attendance_incentive) as total_pay
	,   fresh_delivered::double precision / nullif(fresh_attempted,0) as fasr
	from rider_month_view as a
	left join rider_info as r on r.rider_id = a.rider_id
	left join fasr as f on a.rider_id = f.rider_id
)
, hub_view as (
    select 
        dispatch_month
        , city
        , hub
        ,   sum(attendance) as mandays
        ,   sum(delivered) as delivered
        ,   sum(org_id_pay) as org_id_pay
        ,   sum(delivery_pay) as delivery_pay
        ,   sum(heavy_pay) as heavy_pay 
        ,   sum(vfd_pay) as vfd_pay 
        ,   sum(evening_pay) as evening_pay 
	    ,   sum(mg_payed) as mg_payed 
	    ,	sum(total_variable_pay) as total_variable_pay 
	    ,	sum(fixed_pay) as fixed_pay 
	    ,	sum(attendance_incentive) as attendance_incentive 
	    ,	sum(total_pay) as total_pay
	    ,   SUM(total_pay) / nullif(sum(delivered),0) as cpo
	from month_view
	group by dispatch_month, city, hub

)

select 
    city as shipping_city,
    hub as node_name,
    rider_id as num_rider_id,
    payout_structure,
    dispatch_date,
    attempted as assigned,
    delivered,
    fasr,
    delivery_pay as order_base_0_pay,
    heavy_pay as order_base_3000_pay,
    0 as order_base_5000_pay,
    0 as order_slot_0_12_pay,
    0 as order_slot_12_15_pay,
    evening_pay as order_slot_15_24_pay,
    0 as pincode_rate_pay,
    case when fasr > 0.9 then 2*delivered
		when fasr > 0.85 then 1*delivered
		else 0 end as
		fasr_pay,
    mg_payed,
    fixed_pay,
    0 as fixed_milestone_pay,
    delivery_pay+heavy_pay+evening_pay+
	case when fasr > 0.9 then 2*delivered
		when fasr > 0.85 then 1*delivered
		else 0 end
	+ mg_payed as order_subtotal,
    fixed_pay as fixed_subtotal,
    delivery_pay+heavy_pay+evening_pay+
	case when fasr > 0.9 then 2*delivered
		when fasr > 0.85 then 1*delivered
		else 0 end
	+ mg_payed + fixed_pay as total_pay,
    CONCAT(rider_id,'-',to_char(dispatch_date,'YYYY-MM-DD'),'-',hub) AS key

from day_view

-- SELECT                          *

-- -- from                            base
-- from                            day_view
-- -- from							month_view
-- -- from            hub_view
-- where                           1=1
-- -- and         rider_id = 'M_9606961772_07'
-- -- and         dispatch_date = '2024-02-15'
-- -- and         city = 'Delhi'
-- order by city desc
