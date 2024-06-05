with base as (
    SELECT  
            shipping_city 
        ,   sum(case when date_trunc('day',closure_date) = date_trunc('day',now()+interval '5.5 hours'- interval '1 day') then final_total_charges else 0 end) as revenue_drr
        ,   cast(sum(case when date_trunc('day',closure_date) = date_trunc('day',now()+interval '5.5 hours'- interval '1 day') then final_total_charges else 0 end) as float)/nullif(count(awb),0) as rpo_drr
        ,   count(distinct(case when date_trunc('day',closure_date) = date_trunc('day',now()+interval '5.5 hours'- interval '1 day') then awb else null end)) as order_drr
        ,   sum(final_total_charges) as mtd_revenue
        ,   count(awb) as mtd_orders
        ,   cast(sum(final_total_charges) as float)/nullif(count(awb),0) as mtd_rpo

    FROM public.revenue_mis
    
    WHERE 
          date_trunc('month',closure_date) = date_trunc('month',now() + interval '5.5 hours')
        and  closure_date::date < date_trunc('day',now()+interval '5.5 hours')
        AND  shipping_partner = 'Hyperlocal'
        AND  shipment_status = 'Delivered'
        AND  shipping_city is not null
        
    GROUP BY shipping_city
    
    order by revenue_drr
)

, cost as(
    select 
        a.rider_id
    ,   a.trip_date
    ,   sort_codes
    -- ,   l.node_name
    ,   sum(amount) as pay_amount
        
    from application_db.rider_payout as a
    left join application_db.rider as b on a.rider_id = b.rider_id 
    left join application_db.node as l on l.node_id = b.node_node_id
--     where a.trip_date = date_trunc('day', now()+interval '5.5 hours')
    group by 1,2,3
)

, fuel_pay as(
    select
            c.rider_id
        ,   b.tour_date
        -- ,   c.node_node_id as node_id
        ,   sort_codes
        ,   sum(fuel_payout) as fuel_payout
    from application_db.tour_fuel as a
    left join application_db.tour as b
    on a.tour_id = b.tour_id
    left join application_db.rider as c
    on c.rider_id = b.rider_id
    left join application_db.node as d
    on d.node_id = c.node_node_id
    left join public.locus_tour_brief as e
    on e.tour_id = b.locus_tour_id
    
    where 1=1

    group by 
            c.rider_id
        -- ,   c.locus_rider_id
        ,   b.tour_date
        -- ,   c.node_node_id
        ,   sort_codes
        
)

,   mm_cost as (
    select
        *
    from excel.mm_cost 
    where month = '2023-09-01'

)

-- , loss as (
-- select 
--     shipping_city
-- ,   lost_date
-- ,   0 as amount
-- from {{#2366-lost-with-attribution}} as lost_data
-- where attribution = 'Hub'
-- group by 1,2
-- )


,   total_pay  as (
	select
        a.trip_date
    ,   a.rider_id
    , case  when trim(split_part(l.sort_codes,'/',1)) = 'BLR' then 'Bangalore'
            when trim(split_part(l.sort_codes,'/',1)) = 'DEL' then 'Delhi'
			when trim(split_part(l.sort_codes,'/',1)) = 'NCR' then 'NCR'
            when trim(split_part(l.sort_codes,'/',1)) = 'HYD' then 'Hyderabad'
            when trim(split_part(l.sort_codes,'/',1)) = 'BOM' then 'Mumbai' else 'other' end as shipping_city
    ,   pay_amount
    ,   fuel_payout
    ,   (pay_amount*vendor_charge_percent*0.01) + (fuel_payout*vendor_charge_fuel_percent*0.01) as vendor_charge    
    ,   case  when trim(split_part(l.sort_codes,'/',1)) = 'BLR' then (replace((select blr from mm_cost), ',','')::numeric)/30
			when trim(split_part(l.sort_codes,'/',1)) = 'DEL' then (replace((select del_ncr from mm_cost), ',','')::numeric)/30
            when trim(split_part(l.sort_codes,'/',1)) = 'NCR' then (replace((select del_ncr from mm_cost), ',','')::numeric)/30
            when trim(split_part(l.sort_codes,'/',1)) = 'HYD' then (replace((select hyd from mm_cost), ',','')::numeric)/30
            when trim(split_part(l.sort_codes,'/',1)) = 'BOM' then (replace((select mum from mm_cost), ',','')::numeric)/30  else null end as mm_cost
    
    from cost as a
    left join fuel_pay on a.rider_id = fuel_pay.rider_id and a.trip_date=fuel_pay.tour_date and a.sort_codes = fuel_pay.sort_codes
    left join application_db.rider as b on b.rider_id = a.rider_id
    left join application_db.vendor as c on c.vendor_id = b.vendor_id
	left join application_db.node as l on l.node_id = b.node_node_id

)

, total_pay_final as (
    select 
        shipping_city
    ,   trip_date
    ,   sum(pay_amount) as pay_amount
    ,   sum(fuel_payout) as fuel_payout
    ,   sum(vendor_charge) as vendor_charge
    ,   max(mm_cost) as mm_cost
    ,   sum(pay_amount) + sum(fuel_payout) + sum(vendor_charge) + max(mm_cost) as total_cost
    
    from total_pay
-- 	where trip_date = date_trunc('day',now() +interval '5.5 hours') - interval '1 day'
    group by 1,2
)

, total_pay_final_temp as (
    select 
        a.shipping_city
    ,   trip_date
    ,   pay_amount
    ,   fuel_payout
    ,   vendor_charge
    ,   mm_cost
    ,   total_cost
    -- ,   amount as lost_amount
    , 0 as lost_amount
    
    from total_pay_final as a 
    -- left join loss on a.shipping_city = loss.shipping_city and a.trip_date = loss.lost_date
    order by trip_date desc, shipping_city desc
)

, total_pay_final2 as (
	select
		shipping_city
	,	sum(case when trip_date = date_trunc('day',now()+interval '5.5 hours'-interval '1 day') then total_cost else null end) as cost_drr
	,	sum(total_cost) as mtd_cost
	,   sum(lost_amount) as lost_cost
	
	from total_pay_final_temp
	where date_trunc('month',trip_date) = date_trunc('month',now()+interval '5.5 hours')
	and date_trunc('day',trip_date) < date_trunc('day',now()+interval '5.5 hours')
	group by 1
)

, final as (
	select * from(
	(
		select 
			base.shipping_city as city
			
		,   revenue_drr
		,	cost_drr
		,   revenue_drr-cost_drr as profit_drr
		,   order_drr 
		
		,   mtd_revenue
		,	mtd_cost
		,   mtd_revenue-mtd_cost as mtd_profit
		,   mtd_orders
		
		,   mtd_rpo
		, 	cast(mtd_cost as float) / nullif(mtd_orders,0) as mtd_cpo
		,   cast(mtd_revenue-mtd_cost as float) / nullif(mtd_orders,0) as mtd_ppo
		
		from base
		left join total_pay_final2 as b on base.shipping_city=b.shipping_city
		order by base.revenue_drr desc
	)
	UNION
	(
		select
			'Total' as city
		,   sum(revenue_drr)
		,	sum(cost_drr)
		,   sum(revenue_drr-cost_drr) as profit_drr
		,   sum(order_drr)
		,   sum(mtd_revenue)
		,	sum(mtd_cost)
		,   sum(mtd_revenue-mtd_cost) as mtd_profit
		,   sum(mtd_orders)
		,   cast(sum(mtd_revenue) as float)/nullif(sum(mtd_orders),0) as mtd_rpo
		,	cast(sum(mtd_cost) as float)/nullif(sum(mtd_orders),0) as mtd_cpo
		,   cast(sum(mtd_revenue-mtd_cost) as float) / nullif(sum(mtd_orders),0) as mtd_ppo

		from base
		left join total_pay_final2 as b on base.shipping_city=b.shipping_city
	)
	)as revenue_table
	order by case when city = 'Total' then '0001' else city end)

select * from final
-- select * from total_pay_final2
-- select * from total_pay_final_temp
-- select * from loss
-- select * from base