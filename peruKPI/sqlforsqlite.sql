delete from kpi_mt_step_log;
delete from MhAboveSafeHeight;
delete from MtInstructionStatus;
delete from MtWorkMode;
delete from qc_container_transfer;
delete from qc_event_recorder_his ;
delete from qc_gantry_instruction;
delete from qc_tos_task;
delete from qc_tos_task_his;
delete from qc_tp_interaction_his;
delete from qc_trolley_instruction;
delete from qc_trolley_task;
DELETE FROM kpi_for_qcms;
delete from qcms_kpi_for_container_transfer;


select DISTINCT VBT_ID from qc_tos_task_his order by TRIG_CREATED desc;
212528
--212574
--212572
--212571
--212568
--212548
--212427

select * from qc_tos_task_his where VBT_ID=212571 and LOCK_TIME!='' order by LOCK_TIME asc;2025-01-16 09:42:00
select * from qc_tos_task_his where VBT_ID=212571 and LOCK_TIME!='' order by LOCK_TIME desc;2025-01-16 15:44:20

select * from qc_tos_task_his where VBT_ID=212571 and UNLOCK_TIME!='' order by UNLOCK_TIME asc;2025-01-16 09:43:54
select * from qc_tos_task_his where VBT_ID=212571 and UNLOCK_TIME!='' order by UNLOCK_TIME desc;2025-01-16 15:45:12

select * from qc_tos_task_his where VBT_ID=212571 order by TRIG_CREATED asc;2025-01-16 09:37:13
select * from qc_tos_task_his where VBT_ID=212571 order by TRIG_CREATED desc;2025-01-16 15:45:21



select * from qc_tos_task_his where VBT_ID=212568 and LOCK_TIME!='' order by LOCK_TIME asc;2025-01-15 10:07:18
select * from qc_tos_task_his where VBT_ID=212568 and LOCK_TIME!='' order by LOCK_TIME desc;2025-01-15 16:19:13

select * from qc_tos_task_his where VBT_ID=212568 and UNLOCK_TIME!='' order by UNLOCK_TIME asc;2025-01-15 10:08:44
select * from qc_tos_task_his where VBT_ID=212568 and UNLOCK_TIME!='' order by UNLOCK_TIME desc;2025-01-15 16:22:43-------

select * from qc_tos_task_his where VBT_ID=212572 order by TRIG_CREATED asc;2025-01-15 09:59:01--------
select * from qc_tos_task_his where VBT_ID=212572 order by TRIG_CREATED desc;2025-01-15 16:22:52


alter table MtWorkModeCSV  add column notes text;
alter table kpi_for_qcms  add column EVENT_CODE INTEGER;
EVENT_CODE
select * from qc_tos_task  where STS_NO=104 and VBT_ID=212548 and RESPONSE_TIME!='' order by RESPONSE_TIME asc

CREATE TABLE MtWorkModeCSV1 (
ID INTEGER PRIMARY KEY AUTOINCREMENT,
	PortName VARCHAR(50),
	MachineryName INTEGER,
	ItemName VARCHAR(50),
	Quality BOOLEAN,
	DataType VARCHAR(50),
	IsArray BOOLEAN,
	Value INTEGER,
	"Timestamp" VARCHAR(50),
	Labels VARCHAR(50)
);
CREATE TABLE MhAboveSafeHeightCSV1 (
ID INTEGER PRIMARY KEY AUTOINCREMENT,
	PortName VARCHAR(50),
	MachineryName INTEGER,
	ItemName VARCHAR(50),
	Quality BOOLEAN,
	DataType VARCHAR(50),
	IsArray BOOLEAN,
	Value INTEGER,
	"Timestamp" VARCHAR(50),
	Labels VARCHAR(50)
, notes text);


insert into kpi_for_qcms(                STS_NO,                KEYTIME,                DATA_FROM,                DATA_FROM_TYPE,                NOTES) VALUES (
        '103',        '2025-01-08 13:45:40.310000'
        'QCMSDB.MhAboveSafeHeightCSV.1',        'OPCUA',        'Above')
        
        
        
CREATE TABLE MtInstructionStatusCSV (
ID INTEGER PRIMARY KEY AUTOINCREMENT,
	PortName VARCHAR(50),
	MachineryName INTEGER,
	ItemName VARCHAR(50),
	Quality BOOLEAN,
	DataType VARCHAR(50),
	IsArray BOOLEAN,
	Value INTEGER,
	"Timestamp" VARCHAR(50),
	Labels VARCHAR(50), notes text
);  

CREATE TABLE kpi_mt_state_log (
	id INTEGER,
	state_id INTEGER,
	start_time VARCHAR(50),
	end_time VARCHAR(50),
	duration INTEGER,
	metadata VARCHAR(50),
	twistlock INTEGER,
	task_id_high INTEGER,
	task_id_low INTEGER,
	task_id VARCHAR(50),
	sequence_id INTEGER,
	cycle_id INTEGER,
	crane_id INTEGER,
	created_at VARCHAR(50)
);


DELETE from MtWorkModeCSV

DELETE FROM kpi_for_qcms where DATA_FROM like '%CSV%'


delete from kpi_for_qcms where DATA_FROM like '%kpi_mt_state%'
delete from kpi_for_qcms where DATA_FROM like '%qc_event_recorder_his%'


select * from kpi_for_qcms where DATA_FROM like '%mtworkmode%'

select * from qc_tos_task where RESPONSE_TIME>'2025-01-19 12:11:09' order by RESPONSE_TIME asc;
select * from qc_tos_task_his where TRIG_CREATED>'2025-01-19 12:11:09' order by TRIG_CREATED asc;
select * from qc_gantry_instruction where START_TIME>'2025-01-09 03:06:04' order by START_TIME asc;
select * from qc_trolley_instruction where START_TIME>'2025-01-09 05:08:44' order by START_TIME asc;
select * from qc_trolley_task where START_TIME>'2025-01-19 12:09:42' order by START_TIME asc;
select * from qc_container_transfer where CREATE_TIME>'2025-01-20 19:24:14' order by CREATE_TIME asc;
select * from qc_tp_interaction_his where TRIG_CREATED>'2025-01-20 07:46:41' order by TRIG_CREATED asc;
select * from qc_event_recorder_his where CREATE_TIME>'2025-01-20 19:24:14' order by CREATE_TIME asc;
select * from kpi_mt_step_log where start_time>'2025-01-21 00:24:17.676' order by start_time asc;

select DISTINCT SPREADER_SIZE from qc_trolley_task

select count(*) from kpi_mt_state_log where duration>10000;

select count(*) from kpi_for_qcms where DATA_FROM like '%.qc_event_recorder_his%';

CREATE TABLE "kpi_for_qcms1" (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                PAIRED_VALUE  INTEGER, 
                STS_NO TEXT,
                TASK_ID INTEGER,
                TASK_ID_FOR_GANTRY TEXT,
                TASK_ID_FOR_MT_TROLLEY TEXT,
                TASK_ID_FOR_KPI_MT INTEGER,
                TASK_REF_ID_FOR_MT_TROLLEY INTEGER,
                INSTR_ID_FOR_MT_TROLLEY TEXT,
                TASK_REF_ID_FOR_GANTRY INTEGER,
				TRANS_CHAIN_ID  TEXT,                
                VBT_ID INTEGER,
                TASK_TYPE TEXT,
                TASK_STATUS TEXT,
                ORIG_WSLOC TEXT,
                DEST_WS_LOC TEXT,
                KEYTIME TEXT,
                DATA_FROM TEXT,
                DATA_FROM_TYPE TEXT,
                NOTES TEXT,                
				OPERATE_MODE_FOR_CTNTRANS  TEXT, 
        SPREADER_SIZE_FOR_CTNTRANS  TEXT, 
       WORK_LOCATION_FOR_CTNTRANS  TEXT);
      
      insert into kpi_for_qcms1            STS_NO,            TASK_ID_FOR_KPI_MT,            KEYTIME,            DATA_FROM,            DATA_FROM_TYPE,            NOTES,            PAIRED_VALUE) VALUES (
        '103',        0,        '2025-01-22 00:33:01.945000',        'KPIDB.kpi_mt_state_log.21.start_time',        'KPI',        'KPI记录的state=21备用 对应的start_time',        1882304886268366848
        )
delete from kpi_for_qcms1 

