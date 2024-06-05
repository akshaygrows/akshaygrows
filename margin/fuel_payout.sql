with base as(
    select      d.node_name
        ,       b.tour_date
        ,       c.locus_rider_id
        ,       c.rider_name
        ,       c.fuel_payout_automation
        -- ,       coalesce(a.fuel_rate,0) as fuel_rate
        ,       sum(fuel_payout) as fuel_payout
        ,       sum(total_tasks) as assigned_orders
        ,       sum(completed_tasks) as delivered_orders
        ,       sum(covered_distance) as covered_distance
        ,       sum(planned_distance) as planned_distance
        ,       sum(actual_distance) as actual_distance
        ,       count(e.tour_id) as tours 
    
    from        application_db.tour as b
    left join   application_db.rider as c
    on          c.rider_id = b.rider_id
    left join   application_db.tour_fuel as a
    on          a.tour_id = b.tour_id
    left join   public.locus_tour_brief as e
    on          e.tour_id = b.locus_tour_id
    left join   application_db.node as d
    on          d.node_id = c.node_node_id
    
    where       1=1
    and         date_trunc('month',b.tour_date) = '2024-04-01'

    group by    1,2,3,4,5
)

, cod_data as (
    select      rider_id
    ,           sum(amount) as cod_pending

    from        public.cod_base_data
    
    where       1=1
    and         collected = 0
    and         delivery_date < date_trunc('day',now()+interval '5.5 hours') - interval '1 days'
    
    group by    1
)

, second_base as (
    select          base.* 
    ,               covered_distance as final_distance 
    ,               cod_pending
    
    from            base
    left join       cod_data
    on              base.locus_rider_id = cod_data.rider_id
    WHERE
        base.fuel_payout_automation = 'true'
)

, third_base_cleanup as (
    select          second_base.*
    ,               fuel_payout as final_fuel_payout
    
    from            second_base
) ,


xx as 
(
select          node_name
,               tour_date
,               locus_rider_id
,               fuel_payout_automation
,               rider_name
-- ,               fuel_rate
,               final_distance
,               final_fuel_payout
,               tours
,               assigned_orders
,               delivered_orders
,               cod_pending
,               case when assigned_orders !=0 then (delivered_orders*1.0/assigned_orders) else 0 end as del_percent

from            third_base_cleanup
order by        1,2,3,4
) ,

-----Removing riders which have aggregated delivery percent less than 40%------ 
yy as
(
select locus_rider_id, sum(assigned_orders) as assigned_orders, sum(delivered_orders) as delivered_orders, 
case when sum(assigned_orders) !=0 then (sum(delivered_orders)*1.0/sum(assigned_orders)) else 0 end as del_percent
from xx 
group by locus_rider_id
), 

rider_with_default as 
(
select * from yy 
where del_percent < 0.4
)

select *
from xx 
left join application_db.node as n on xx.node_name = n.node_name
where locus_rider_id not in (select locus_rider_id from rider_with_default)
and node_type <> 'franchise_hub'