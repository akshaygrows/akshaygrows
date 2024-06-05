ALTER TABLE rider_pip RENAME COLUMN remark TO need_improvement

ALTER TABLE rider_pip
ALTER COLUMN need_improvememt TYPE VARCHAR(250);

delete from rider_pip

drop table rider_pip
drop table your_table_name

CREATE TABLE rider_pip (
    key VARCHAR(50),
    pip_date DATE,
    shipping_city VARCHAR(50),
    rider_id VARCHAR(50),
    rider_name VARCHAR(50),
    hub VARCHAR(50),
    vendor_name VARCHAR(50),
    rider_age INTEGER,
    need_improvement VARCHAR(250),
    starting_fasr FLOAT,
    closing_fasr FLOAT,
    pip_status VARCHAR(50),
    pip_result VARCHAR(50)
);

-- Step 1: Create a new table with the desired order
CREATE TABLE rider_pip_temp AS
SELECT 
    key,
    pip_date,
    rider_id,
    shipping_city,
    rider_name,
    hub,
    vendor_name,
    rider_age,
	need_improvement,
    starting_fasr,
    closing_fasr,
    pip_status,
    pip_result
FROM rider_pip
ORDER BY pip_date, shipping_city, hub, starting_fasr;

-- Step 2: Rename the new table to replace the original one
DROP TABLE rider_pip;
ALTER TABLE rider_pip_temp RENAME TO rider_pip;

UPDATE rider_pip
SET pip_status = 'closed'
WHERE pip_date = '2023-11-27';


update rider_pip
set pip_result = 'passed'
where rider_id in (
'Anil_nayak_2',
'lakshmi_9972726538_02',
'Abdul_Jameel_7_1701435033',
'manirul_8972635746_07',
'Sachain_Anand_Lodhi_7',
'DS_NCT_DWRK_0313202307_2',
'Atul_Jps_7976431735_023',
'Ghanshyam_Ps_5_1701415871',
'Rahul_9582575453_021',
'Vikas_Kumar_PS_5_1701580647',
'vikram_singh_Ps_5',
'Bala_8185059013_04',
'E_6301469279_04',
'_9701430190_04',
'Bandi_Prashanth__19',
'b_6302395530_019',
'G_9381324098_019',
'md_6303548708_019',
'naresh_9059141215_019',
'Yesudas_jannu_4'
)

update rider_pip
set pip_status = 'final'
where pip_date = '2024-01-08'

select * from rider_pip where pip_date = '2024-01-08'