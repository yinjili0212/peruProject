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


select DISTINCT VBT_ID from qc_tos_task_his order by TRIG_CREATED desc;

--212427
--212507
--4415117113263614774
--212448
--212467
--212407
--212447
--212347
--212367
select * from qc_tos_task_his where VBT_ID=212427 and LOCK_TIME!='' order by LOCK_TIME asc;2025-01-08 13:31:26
select * from qc_tos_task_his where VBT_ID=212427 and LOCK_TIME!='' order by LOCK_TIME desc;2025-01-09 08:05:51

select * from qc_tos_task_his where VBT_ID=212427 and UNLOCK_TIME!='' order by UNLOCK_TIME asc;2025-01-08 13:45:31
select * from qc_tos_task_his where VBT_ID=212427 and UNLOCK_TIME!='' order by UNLOCK_TIME desc;2025-01-09 08:06:44

select * from qc_tos_task_his where VBT_ID=212427 order by TRIG_CREATED asc;2025-01-08 12:57:15
select * from qc_tos_task_his where VBT_ID=212427 order by TRIG_CREATED desc;2025-01-09 20:17:41



