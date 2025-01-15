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


CREATE TABLE kpi_for_qcmstest (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                STS_NO TEXT,
                TASK_ID INTEGER,
                VBT_ID INTEGER,
                TASK_TYPE TEXT,
                TASK_STATUS TEXT,
                ORIG_WSLOC TEXT,
                DEST_WS_LOC TEXT,
                KEYTIME TEXT,
                DATA_FROM TEXT,
                DATA_FROM_TYPE TEXT,
                NOTES TEXT);
               
   CREATE TABLE MhAboveSafeHeight (
    ID INTEGER PRIMARY KEY AUTOINCREMENT);
               
select * from kpi_for_qcms212427 kfq where DATA_FROM like '%GANTRY%'       


alter table MtWorkMode add column notes TEXT
alter table MtWorkMode add column QC_ID TEXT



update MtWorkMode set notes='Local Mode' where Value=3;
update MtWorkMode set notes='Maintenance Mode' where Value=2;
update MtWorkMode set notes='Normal Mode' where Value=1;



alter table MtInstructionStatus  add column notes TEXT;
alter table MtInstructionStatus add column QC_ID TEXT;
update MtInstructionStatus set notes='Ready for ECS' where Value=1;
update MtInstructionStatus set notes='Not ready for ECS' where Value=2;





alter table MhAboveSafeHeight  add column notes TEXT;
alter table MhAboveSafeHeight add column QC_ID TEXT;
update MhAboveSafeHeight set notes='Below' where Value=2;
update MhAboveSafeHeight set notes='Above' where Value=1;



delete from MtWorkMode where StatusCode='BadCommunicationError';
delete from MtInstructionStatus where StatusCode='BadCommunicationError';
delete from MhAboveSafeHeight where StatusCode='BadCommunicationError';
select * from qc_tos_task_his where VBT_ID=212427

select * from QC_TOS_TASK where (LOCK_TIME>'2025-01-08 12:57:15' and  LOCK_TIME<'2025-01-09 20:17:41') or (UNLOCK_TIME>'2025-01-08 12:57:15' and  UNLOCK_TIME<'2025-01-09 20:17:41') order by TASK_ID asc

CREATE TABLE kpi_for_qcms (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                STS_NO TEXT,
                TASK_ID INTEGER,
                VBT_ID INTEGER,
                TASK_TYPE TEXT,
                TASK_STATUS TEXT,
                ORIG_WSLOC TEXT,
                DEST_WS_LOC TEXT,
                KEYTIME TEXT,
                DATA_FROM TEXT,
                DATA_FROM_TYPE TEXT,
                NOTES TEXT);
               
               
select * from qc_tos_task_his where TASK_ID=55392;    
delete from kpi_for_qcms  where DATA_FROM='QCMSDB.QC_TOS_TASK_HIS.TRIGGER_ACTION(UPDATE).TRIG_CREATED'

alter table kpi_for_qcms add column TASK_REF_ID INTEGER;
alter table kpi_for_qcms add column TASK_ID_FOR_GANTRY TEXT;
alter table kpi_for_qcms add column INSTR_ID_FOR_MT_TROLLEY TEXT;
alter table kpi_for_qcms add column TASK_REF_ID_FOR_MT_TROLLEY INTEGER;
alter table kpi_for_qcms add column TASK_ID_FOR_MT_TROLLEY TEXT;

alter table kpi_for_qcms add column TRANS_CHAIN_ID  TEXT;
alter table kpi_for_qcms add column OPERATE_MODE_FOR_CTNTRANS  TEXT;
alter table kpi_for_qcms add column SPREADER_SIZE_FOR_CTNTRANS  TEXT;
alter table kpi_for_qcms add column WORK_LOCATION_FOR_CTNTRANS  TEXT;

TASK_REF_ID


select * from QC_GANTRY_INSTRUCTION where (START_TIME>'2025-01-08 12:57:15' and START_TIME<'2025-01-09 20:17:41') or (END_TIME>'2025-01-08 12:57:15' and END_TIME<'2025-01-09 20:17:41') order by START_TIME asc

delete from kpi_for_qcms 

alter table kpi_for_qcms add column PAIRED_VALUE  INTEGER;

paired value

alter table kpi_for_qcms add column TRANS_CHAIN_ID  TEXT;

insert into kpi_for_qcms(                        STS_NO,                        KEYTIME,                        DATA_FROM,                        DATA_FROM_TYPE,                        NOTES) VALUES (
                '103',                '2024-12-16 22:03:01',                'QCMSDB.QC_TP_INTERACTION_HIS.103.1.dict_values(['C', '504', 'LOAD', 'ARRIVED'])',                'QCMS',                "交互状态{'FMS_JOB_POS': 'C', 'FMS_AHT_ID': '504', 'FMS_MOVE_KIND': 'LOAD', 'FMS_AHT_STATUS': 'ARRIVED'}")








select * from kpi_for_qcms where DATA_FROM_TYPE='OPCUA'
delete from kpi_for_qcms where DATA_FROM_TYPE='OPCUA'














