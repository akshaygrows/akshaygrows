with revenue_mis as (
select * from revenue_mis
where attributed_month = '2023-12-01'
and shipping_partner = 'Hyperlocal'
and shipment_status <> 'lost'
)
, calc_data as (
    select 
        user_id
    ,   city    
    ,   shipper
    ,   forward_rpo
    ,   rto_multiplier
    ,   case when cod_charge = 'Yes' then 1 else 0 end as cod_flag
    from client_level_city_rpo_temp
)
,   final_values as (
    select 
        awb
        ,   revenue_mis.user_name
        ,   shipping_city
        ,   shipment_status
        ,   closure_date
        ,   case    when shipment_status = 'Delivered' then (forward_rpo + cod_flag*finops_cod_charge)
                    when shipment_status = 'RTO Delivered' then (forward_rpo + forward_rpo*rto_multiplier)
                    else 0 end as actual_charges
        ,   final_total_charges
        from revenue_mis
        inner join calc_data 
        on revenue_mis.user_id = calc_data.user_id 
        and (case when shipping_city = 'NCR' then 'Delhi' else shipping_city end) = calc_data.city
)

-- select 
--     user_name
-- ,   shipping_city
-- ,   sum(actual_charges) as actual_charges
-- ,   sum(final_total_charges) as final_total_charges 
-- ,   sum(actual_charges - final_total_charges) as diff_charges
-- from final_values 
-- group by 1,2

update revenue_mis as r
set final_total_charges = f.actual_charges
from final_values as f
where 1=1
-- and r.final_total_charges < f.actual_charges
and r.awb = f.awb