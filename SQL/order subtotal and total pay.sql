update payout_day as pd
set order_subtotal = order_base_0_pay + order_base_3000_pay + order_base_5000_pay + order_slot_0_12_pay + order_slot_12_15_pay + order_slot_15_24_pay + pincode_rate_pay + fasr_pay + mg_payed
,	total_pay = order_base_0_pay + order_base_3000_pay + order_base_5000_pay + order_slot_0_12_pay + order_slot_12_15_pay + order_slot_15_24_pay + pincode_rate_pay + fasr_pay + mg_payed + fixed_pay + fixed_milestone_pay
