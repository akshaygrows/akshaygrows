with base as (select 
	awb
,	pickuptime
,	shipment_status
,	user_name
,	('2024-05-31'::date - pickuptime::date) as age_interval
 	
from ops_main
where pickuptime::date <= '2024-05-01'
and shipment_status not in ('Cancelled','Lost','Delivered','RTO Delivered','Damaged')
and (delivertime::date >= '2024-05-31' or delivertime is null)
and (rto_delivered_time >= '2024-05-31' or rto_delivered_time is null)
)
select
	user_name
,	shipment_status
,	age_interval as ageing
,	count(distinct(awb)) as orders
from base
group by 1,2,3




