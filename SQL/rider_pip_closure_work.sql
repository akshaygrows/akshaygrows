
select * from rider_pip where pip_date = '2023-11-20' and rider_id in ('Pavan_Kumar_R_2','MOHAMMED_GOUSE_K_M_7','Prashanth_G_7','Syed_Nadeem_7')

UPDATE rider_pip
SET pip_result = 'passed'
WHERE pip_date = '2023-11-20'
  AND rider_id IN ('Ajay_VLS_1_5', 'DS_NCT_DWRK_0326202306_60', 'Kamal_Pathak_5');
  
UPDATE rider_pip
set pip_status = 'final'
where pip_date = '2023-11-20'

select * from rider_pip 
where pip_date = '2023-12-04' 
and pip_status = 'final' 
and pip_result = 'failed'

select * from rider_pip 

delete FROM rider_pip
WHERE pip_date = '2023-11-27'
  AND rider_id IN (
    SELECT rider_id
    FROM rider_pip
    WHERE pip_result = 'failed'
  );