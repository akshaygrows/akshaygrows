with pincode_rates_new as (
    Select      pincode
    ,           delivered_rate
    from public.delivery_rate_new2
)

, rider_info as (
	select 
		a.rider_name
	,   node_name
	,	locus_rider_id
	,   b.city_name as city_name
    ,   case when d.payout_structure is null then a.payout_structure else d.payout_structure end as payout_structure
    ,   case when d.vendor_name is null then c.vendor_name else d.vendor_name end as vendor_name
	from application_db.rider as a 
	left join application_db.node as b on a.node_node_id = b.node_id
	left join application_db.vendor as c on c.vendor_id = a.vendor_id
	left join public.temp_rider_config as d on d.rider_id = a.locus_rider_id
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
    SELECT                      locus_task_brief.awb,task_id,locus_task_brief.status, slot_start, locus_task_brief.rider_id,task_start_time,task_end_time,homebase,completed_at,cancelled_at,cancellation_remarks,locus_task_brief.cod_amount,locus_task_brief.dispatch_time
    ,							rider_info.rider_name
    ,                           rider_info.city_name as city
	,							rider_info.node_name as hub
	,							warehouse_city
    ,                           RANK() OVER ( PARTITION BY locus_task_brief.awb ORDER BY locus_task_brief.dispatch_time) as attempt_number
    ,                           case when locus_task_brief.cod_amount = 0 then 'Prepaid' else 'COD' end as mop
    ,                           shipment_order_details.user_id as user_id_id
    ,                           greatest(shipment_order_details.shipment_chargeable_weight, greatest(( dwh_volume/5000.0),dwh_weight)) as chargeable_weight
    ,                           shipment_order_details.shipping_pincode as pincode
    ,                           case when extract(hour from locus_task_brief.dispatch_time) >= 15 then 'evening' else 'morning' end as slot
	,							rider_info.payout_structure
	,							rider_info.vendor_name
	,							(case when  r.fuel_payout_automation = 'true' then tf.fuel_payout else 0 end)/nullif(completed_tasks,0) as fuel_amount
    from                        locus_task_brief
    left join                   shipment_order_details
    ON                          locus_task_brief.awb = shipment_order_details.awb
    left join                   shipment_weight as w on public.locus_task_brief.awb = w.shipment_awb
	left join					rider_info on locus_task_brief.rider_id = rider_info.locus_rider_id
	left join 					application_db.trip as t on t.locus_trip_id = locus_task_brief.task_id
	left join 					application_db.tour_fuel as tf on t.tour_id = tf.tour_id
	left join 					application_db.rider as r on locus_task_brief.rider_id = r.locus_rider_id
	left join 					application_db.tour as tr on tr.tour_id = t.tour_id
	left join 					locus_tour_brief as lt on lt.tour_id = tr.locus_tour_id
-- 	left join                   application_db.node on node.locus_home_base_id = locus_task_brief.location_id 
    where                       1=1
    and                         locus_task_brief.awb is not null
    and                         date_trunc('month',locus_task_brief.dispatch_time)::date = '2024-04-01'
    and                         rider_info.node_name not like '%FRH%'
    and                         rider_info.node_name not like '%Franchise%'
    and                         rider_info.node_name <> 'GSCAN TEST'
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
    ,       city, hub, awb , rider_id, rider_name, status, task_id, payout_structure, vendor_name, warehouse_city
    ,       chargeable_weight, delivered_rate, locus_task_query.pincode, pincode_rates_new.pincode , slot
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
	, fuel_amount

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
,           count(distinct case when status='COMPLETED' then task_id else null end) as delivered
,           count(distinct(case when status='COMPLETED' and slot='morning' then task_id else null end)) as morning_delivered
,           count(distinct(case when status='COMPLETED' and slot='evening' then task_id else null end)) as evening_delivered
,           count(distinct task_id) as attempted
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
    ,       case when payout_structure = 'ORDER' then
                    case    --full day mg
                        when city = 'Delhi' and delivered >= 10 then 500  
                        when city = 'Mumbai' and delivered >= 15 and hub = 'DS BOM KDWL' then 750
                        when city = 'Mumbai' and delivered >= 5 then 600
                        when city = 'Hyderabad' and delivered >= 3 then 600
                            -- evening mg
                        when city = 'Bangalore' and evening_delivered >= 15 then 500
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
	, org_id_pay, delivery_pay, heavy_pay, vfd_pay, evening_pay
	, earning_pay as order_earning , order_mg
	, case when earning_pay < order_mg then order_mg - earning_pay else 0 end as mg_payed
	, fixed_pay
	, earning_pay + (case when earning_pay < order_mg then order_mg - earning_pay end) + fixed_pay as total_pay
	from day_rider_view_modified

)

, locus_task_query_franchise as (
    SELECT                      locus_task_brief.awb,task_id,status, slot_start, locus_task_brief.rider_id,rider_phone,task_start_time,task_end_time,homebase,lm_team_name,completed_at,cancelled_at,cancellation_remarks,locus_task_brief.cod_amount,tour_id,dispatch_time
    ,							rider_info.rider_name
	,                           rider_info.city_name as city
	,							warehouse_city
	,							rider_info.node_name as hub
    ,                           RANK() OVER ( PARTITION BY locus_task_brief.awb ORDER BY dispatch_time) as attempt_number
    ,                           case when locus_task_brief.cod_amount = 0 then 'Prepaid' else 'COD' end as mop
    ,                           shipment_order_details.user_id as user_id_id
    ,                           greatest(shipment_order_details.shipment_chargeable_weight, greatest(( dwh_volume/5000.0), dwh_weight)) as chargeable_weight
    ,                           shipment_order_details.shipping_pincode as pincode
	,							rider_info.payout_structure
	,							rider_info.vendor_name
    from                        locus_task_brief
    left join                   shipment_order_details
    ON                          locus_task_brief.awb = shipment_order_details.awb
    left join                  shipment_weight on public.locus_task_brief.awb = shipment_weight.shipment_awb
	left join					rider_info on locus_task_brief.rider_id = rider_info.locus_rider_id
	left join                   application_db.node on node.locus_home_base_id = locus_task_brief.location_id
    where                       1=1
    and                         (lower(rider_info.node_name) like '%franchise%' or lower(rider_info.node_name) like '%frh%')
    and                         locus_task_brief.awb is not null
    and                         date_trunc('month',dispatch_time)::date = '2024-04-01'
)

, base_franchise as (
    SELECT                      
            date_trunc('day',dispatch_time) as dispatch_date
    ,       city, hub, awb , rider_id, rider_name, status, task_id, warehouse_city
    ,       chargeable_weight, locus_task_query_franchise.pincode, payout_structure
	,		case 	
					when status='COMPLETED' and city = 'Bangalore' then (
					        case    when hub in ('BLR FRH ECTY','BLDR-Franchise') then 35
					                when hub in ('YLHK-Franchise','KPHB-Franchise','CMRJ-Franchise','STNG-Franchise') then 38
					                else 38 end)
					when status='COMPLETED' and city = 'Delhi' then 
					        (case    when hub in ('GGKC-Franchise' , 'SNSR-Franchise') then 30
					                when hub in ('RHNP-Franchise', 'ONFC-Franchise','FBD-Franchise','FABD-Franchise') then 35
                                    -- when hub in ('del_frh_gokp','Noida-Franchise','ALPR-Franchise') then 33 
                                    when hub in ('APAN-Franchise') then 32
                                    when hub in ('APAN-Franchise') then 33
                                    else 33 end)
                    when status='COMPLETED' and city = 'Mumbai' and hub = 'GRGN-Franchise' then 40 
                    when status='COMPLETED' and city = 'Hyderabad' then 38
					else 0 end as delivery_incentive
	,       case
					when status='COMPLETED' and city = 'Delhi' then 
					       ( case    when chargeable_weight > 4000 then  30
					                when chargeable_weight > 7000 then 60
					                else 0 end)
					when status='COMPLETED' and city = 'Bangalore' then (
					        case    when hub in ('BLR FRH ECTY','BLDR-Franchise') and chargeable_weight > 3000 then 45
					                when chargeable_weight > 3000 then 40
					                else 0 end)
					when status='COMPLETED' and city in ('Mumbai','Hyderabad') then (
					        case    when chargeable_weight > 3000 then 40
					                else 0 end)
					  
					else 0 end as weight_incentive
    FROM    locus_task_query_franchise
)
,   awb_view_franchise as (
		select awb
	,	task_id
	,	b.dispatch_date
	,	b.city
	,	b.warehouse_city
	,	b.hub
	,	b.rider_id	
	,	b.delivery_incentive+b.weight_incentive as 	awb_pay
	,	status
	,	b.payout_structure
	from base_franchise as b
	where status = 'COMPLETED'
) 

,   awb_view as (
    select
        awb
    ,   task_id
    ,   base.dispatch_date
    ,   base.city
	,   base.warehouse_city
    ,   base.hub
    ,   base.rider_id
    ,   base.org_id_pay+base.delivery_incentive+base.weight_incentive+base.failed_delivery_incentive+base.evening_order_incentive+base.fuel_amount+coalesce((mg_payed/nullif(delivered,0)),0)+ coalesce((fixed_pay/nullif(delivered,0)),0) as awb_pay
    ,   status
    ,   base.payout_structure
    from base
    left join day_view as dv on dv.rider_id = base.rider_id and dv.dispatch_date = base.dispatch_date
	where status = 'COMPLETED'
)

, final_awb_view as (
select * from awb_view
UNION
select * from awb_view_franchise
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
	left join rider_info as r on r.locus_rider_id = a.rider_id
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

SELECT                          *

-- from                            base
-- from                            day_view
-- from							month_view
from            final_awb_view
where                           1=1
-- and         rider_id = 'M_9606961772_07'
-- and         dispatch_date = '2024-02-15'
-- and         city = 'Delhi'
-- and awb = 'GS1945828791'
order by city desc
