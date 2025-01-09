delete from plotly_test 


select DISTINCT TASK_ID from kpi_for_qcms212448 where STS_NO='103' and KEYTIME>='2025-01-02 10:00:00' and KEYTIME<'2025-01-02 11:00:00'

select DISTINCT TASK_ID from kpi_for_qcms212448 where STS_NO='103' and KEYTIME>='2025-01-02 10:00:00' and KEYTIME<'2025-01-02 11:00:00'
select * from QC_CONTAINER_TRANSFER where TRANS_CHAIN_ID='{transChainId}' order by CREATE_TIME asc

select * from qc_container_transfer where CREATE_TIME>='2025-01-02 09:59:16' and QC_ID=103 and INSTR_TYPE='Ground'


CREATE TABLE kpi_for_container_transfer (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                QC_ID INTEGER,
                TRANS_CHAIN_ID TEXT UNIQUE,
                TASK_ID INTEGER,
                PICKUP_LOCATION TEXT,
                GROUND_LOCATION TEXT,
                PICKUP_TIME TEXT,
                GROUND_TIME TEXT,
                OPERATE_MODE TEXT);
                
               
--delete from qc_trolley_task where START_TIME<='2024-12-25 00:00:10' 
--delete from qc_trolley_instruction  where START_TIME<='2024-12-25 00:00:10' 
--delete from qc_tp_interaction_his  where TRIG_CREATED<='2024-12-25 00:00:10' 
--delete from qc_tos_task_his  where TRIG_CREATED<='2024-12-25 00:00:10' 
--delete from qc_tos_task  where RESPONSE_TIME<='2024-12-25 00:00:10'
--delete from qc_gantry_instruction  where START_TIME<='2024-12-25 00:00:10'
--delete from qc_container_transfer  where CREATE_TIME<='2024-12-25 00:00:10'
--delete from kpi_mt_step_log  where created_at<='2024-12-25 00:00:10'


CREATE TABLE qcms_kpi_for_container_transfer (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                QC_ID INTEGER,
                TRANS_CHAIN_ID TEXT UNIQUE,
                TASK_ID INTEGER,
                TASK_TYPE TEXT,
                VBT_ID INTEGER,
                PICKUP_LOCATION TEXT,
                GROUND_LOCATION TEXT,
                PICKUP_TIME TEXT,
                GROUND_TIME TEXT,
                Pickup_OPERATE_MODE TEXT,
                Ground_OPERATE_MODE TEXT,
                SPREADER_SIZE TEXT);
               
               
select DISTINCT TRANS_CHAIN_ID from qc_container_transfer qct where CREATE_TIME>''

select * from qc_tos_task  where STS_NO=103 and VBT_ID=212448 order by RESPONSE_TIME desc
  

select DISTINCT TRANS_CHAIN_ID from qc_container_transfer where QC_ID=103 and CREATE_TIME>='2025-01-02 11:06:55' and CREATE_TIME<='2025-01-02 15:46:52' order by CREATE_TIME asc


select count(*) from qc_tos_task  where VBT_ID=212407 order by RESPONSE_TIME asc

delete from qcms_kpi_for_container_transfer where GROUND_TIME is NULL

delete from qc_tos_task where 










