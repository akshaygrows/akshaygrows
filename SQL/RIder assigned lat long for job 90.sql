select 
	rs.rider_id
,	ns.lat
,	ns.lng
from optima_nodestats as ns
left join optima_riderstats as rs on ns.assigned_rider_id = rs.id
where rs.job_id = 90