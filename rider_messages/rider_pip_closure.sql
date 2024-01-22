UPDATE rider_pip AS rp
SET pip_status = 'closed',
    update_time = now() + interval '5.5 hours'
WHERE pip_date = date_trunc('week', now() + interval '5.5 hours') - interval '1 week'
RETURNING rp.*;
