update payout_day as pd
set mg_payed = b.mg_payed
from (
	select 
		key
	,	case when shipping_city = 'Mumbai' and delivered >= 5 then
			case when node_name ='DS BOM KDWL' and order_subtotal <= 750 then mg_payed + (750 - order_subtotal)
				 when node_name in ('DS BOM SWRI','DS BOM KRL','DS BOM THAN') and order_subtotal <= 600 then mg_payed + (600 - order_subtotal)
			else mg_payed end
		else mg_payed
		end as mg_payed
from payout_day
where shipping_city in ('Mumbai','Bangalore')
and dispatch_date <= '2024-06-16') as b
where pd.key = b.key