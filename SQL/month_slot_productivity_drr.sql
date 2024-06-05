with locus_base as (
	select
		rider_id
	,	status
	,	case when extract(hour from dispatch_time)>=12 then 'Afternoon' else 'Morning' end as slot
	,	dispatch_time::date as dispatch_date
	,	task_id
	,	case    when split_part(sort_codes,'/',1) = 'BLR' then 'Bangalore'
				when split_part(sort_codes,'/',1) = 'HYD' then 'Hyderabad'
				when split_part(sort_codes,'/',1) = 'BOM' then 'Mumbai'
				when split_part(sort_codes,'/',1) = 'DEL' then 'Delhi'
				when split_part(sort_codes,'/',1) = 'NCR' then 'NCR'
				else 'other' end as shipping_city
	,	greatest(d.shipment_chargeable_weight, greatest(( w.length*w.breadth*w.height/5000.0), w.weight)) as chargeable_weight


	from locus_task_brief as t
	left join application_db.node as n on n.locus_home_base_id = t.location_id
	left join application_db.shipment_weight as w on t.awb = w.shipment_awb
	left join shipment_order_details as d on d.awb = t.awb
	where (lower(n.node_name) not like '%frh%' and lower(n.node_name) not like '%franchise%')
	and dispatch_time::date >= '2023-07-01'
)

, ops_main_base as (
	select 
		shipping_city
	,	edd
	,	count(awb) as drr
	from ops_main
	left join application_db.node on lower(ops_main.last_mile_hub) = trim(split_part(lower(application_db.node.sort_codes),'/',2))
	where edd >= '2023-07-01'
	and shipping_partner = 'Hyperlocal'
	and (lower(application_db.node.node_name) not like '%frh%' and lower(application_db.node.node_name) not like '%franchise%')
	group by 1,2
)

, grouped_by_slot as (
	select 
		shipping_city
	,	dispatch_date
	,	slot
	,	count(case when chargeable_weight >= 1800 then task_id else null end) as heavy_attempted
	,	count(task_id) as attempted
	,	count(case when status = 'COMPLETED' then task_id else null end) as delivered
	,	count(distinct(rider_id)) as mandays
	,	trunc(sum(chargeable_weight)) as weight_load
	from locus_base
	group by 1,2,3
)

, final_locus_data as (
	select
		shipping_city
	,	date_trunc('month',dispatch_date) as dispatch_month
	,	sum(weight_load)::numeric/nullif(sum(attempted),0) as avg_weight
	,	sum(case when slot='Morning' then attempted else null end)::numeric/nullif(sum(case when slot = 'Morning' then mandays else null end),0) as mn_att_prod
	,	sum(case when slot='Morning' then delivered else null end)::numeric/nullif(sum(case when slot = 'Morning' then mandays else null end),0) as mn_del_prod
	,	sum(case when slot='Afternoon' then attempted else null end)::numeric/nullif(sum(case when slot = 'Afternoon' then mandays else null end),0) as af_att_prod
	,	sum(case when slot='Afternoon' then delivered else null end)::numeric/nullif(sum(case when slot = 'Afternoon' then mandays else null end),0) as af_del_prod
	from grouped_by_slot
	group by 1,2
)

, grouped_by_month_ops_main as (
	select
		shipping_city
	,	date_trunc('month',edd) as dispatch_month
	,	avg(drr) as drr
	from ops_main_base
	group by 1,2
)

, final as (
	select
		a.shipping_city
	,	a.dispatch_month
	,	drr
	,	avg_weight
	,	mn_att_prod
	,	mn_del_prod
	,	af_att_prod
	,	af_del_prod
	from grouped_by_month_ops_main as a
	left join final_locus_data as b on a.shipping_city = b.shipping_city and a.dispatch_month = b.dispatch_month
	order by a.shipping_city , a.dispatch_month
)

-- select * from grouped_by_day order by shipping_city, dispatch_date
-- select * from ops_main_base
-- select * from grouped_by_month_ops_main where shipping_city is not null order by shipping_city , dispatch_month
-- select * from final_locus_data order by shipping_city , dispatch_month
select * from final where shipping_city is not null