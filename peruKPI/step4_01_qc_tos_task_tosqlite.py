import sqliteHandle
import datetime
from basicFunctionDefine import *
import time
#########
'根据船舶靠泊的VBT_ID查询这个船舶作业的开始时间和结束时间，以及在期间内QCMS拉取过的任务，以及每个任务中指令的情况' \
'1.处理任务创建时间，使用的是QC_TOS_TASK_HIS中TRIGGER_ACTION="INSERT"的数据作为任务开始时间'
'2.处理任务闭锁时间，使用的是QC_TOS_TASK中LOCK_TIME和UNLOCK_TIME字段的数据作为任务开始时间'
'3.处理任务完成时间（删除时间，因为QCMS拉取TOS任务时可能存在重复的任务），使用的是QC_TOS_TASK_HIS中TRIGGER_ACTION="DELETE"的数据作为任务开始时间'
'4.处理大车运行的指令，使用的是QC_GANTRY_INSTRUCTION中的数据'
'5.处理QC_TROLLEY_INSTRUCRION的指令'
'6.处理QC_CONTAINER_TRANSFER的指令'
'7.处理交互表QC_TP_INTERACTION_HIS的数据'
'''8.处理ACCS的kpi_mt_step_log的数据'''
#########


startTime='2025-01-08 12:57:15'
endTime='2025-01-09 20:17:41'
# endTime='2025-01-08 16:17:41'

# startTime='2024-01-08 12:57:15'
# endTime='2025-01-15 20:17:41'
# # endTime='2025-01-08 16:17:41'


startTimeforKpi='2025-01-08 17:57:15'
endTimeforKpi='2025-01-10 01:17:41'
# endTimeforKpi='2025-01-08 18:17:41'


QC_TOS_TASK='QC_TOS_TASK'
QC_TOS_TASK_HIS='QC_TOS_TASK_HIS'
tablename_for_kpi='kpi_for_qcms'
QC_GANTRY_INSTRUCTION='QC_GANTRY_INSTRUCTION'
QC_TROLLEY_INSTRUCTION='QC_TROLLEY_INSTRUCTION'
QC_CONTAINER_TRANSFER='QC_CONTAINER_TRANSFER'
kpi_mt_step_log='kpi_mt_step_log'
qc_tp_interaction_his = 'qc_tp_interaction_his'
MtWorkMode='MtWorkMode'
MtInstructionStatus='MtInstructionStatus'
MhAboveSafeHeight='MhAboveSafeHeight'
#连接数据库
o=sqliteHandle.sqliteHandler(r'./kpiforQcms20250109.db')

# #######################处理QC_TOS_TASK和QC_TOS_TASK_HIS中的数据，将时间内的变化节点都打入数据中(有配对值)
# #查询QC_TOS_TASK在规定的时间区间内到底有多少条任务
# qcTosTaskQuerySql = f'''select * from {QC_TOS_TASK} where (LOCK_TIME>'{startTime}' and  LOCK_TIME<'{endTime}') or (UNLOCK_TIME>'{startTime}' and  UNLOCK_TIME<'{endTime}') order by TASK_ID asc'''
# qcTosTaskQueryResults = o.query(qcTosTaskQuerySql,t='df')
# if isinstance(qcTosTaskQueryResults,pd.DataFrame):#能查询出来任务
#     for index,qcTosTaskQueryResult in qcTosTaskQueryResults.iterrows():#遍历每一条查询出来的数据
#
#         ####配对值
#         paired_value = snowFlakeId()
#
#
#         #####QC_TOS_TASK的LOCK_TIME处理记录###########
#         #遍历任务中的LOCK_TIME和UNLOCK_TIME记录到数据库
#         DATA_FROM='QCMSDB.QC_TOS_TASK.LOCK_TIME'#需要改的部分
#         DATA_FROM_TYPE='QCMS'
#         NOTES= '吊具闭锁时间'#需要改的部分
#         insertsql = f'''insert into {tablename_for_kpi}(\
#         STS_NO,\
#         TASK_ID,\
#         VBT_ID,\
#         TASK_TYPE,\
#         TASK_STATUS,\
#         ORIG_WSLOC,\
#         DEST_WS_LOC,\
#         KEYTIME,\
#         DATA_FROM,\
#         DATA_FROM_TYPE,\
#         NOTES,\
#         PAIRED_VALUE) VALUES (
# '{qcTosTaskQueryResult['STS_NO']}',\
# {qcTosTaskQueryResult['TASK_ID']},\
# {qcTosTaskQueryResult['VBT_ID']},\
# '{qcTosTaskQueryResult['TASK_TYPE']}',\
# '{qcTosTaskQueryResult['TASK_STATUS']}',\
# '{qcTosTaskQueryResult['ORIG_WSLOC']}',\
# '{qcTosTaskQueryResult['DEST_WS_LOC']}',\
# '{qcTosTaskQueryResult['LOCK_TIME']}',\
# '{DATA_FROM}',\
# '{DATA_FROM_TYPE}',\
# '{NOTES}',\
# {paired_value}
# )'''
#         o.executesql(insertsql)
#         #####QC_TOS_TASK的LOCK_TIME处理记录###########
#
#         #####QC_TOS_TASK的UNLOCK_TIME处理记录###########
#         #遍历任务中UNLOCK_TIME记录到数据库
#         DATA_FROM='QCMSDB.QC_TOS_TASK.UNLOCK_TIME'#需要改的部分
#         DATA_FROM_TYPE='QCMS'
#         NOTES= '吊具开锁时间'#需要改的部分
#         insertsql = f'''insert into {tablename_for_kpi}(\
#         STS_NO,\
#         TASK_ID,\
#         VBT_ID,\
#         TASK_TYPE,\
#         TASK_STATUS,\
#         ORIG_WSLOC,\
#         DEST_WS_LOC,\
#         KEYTIME,\
#         DATA_FROM,\
#         DATA_FROM_TYPE,\
#         NOTES,\
#         PAIRED_VALUE) VALUES (
# '{qcTosTaskQueryResult['STS_NO']}',\
# {qcTosTaskQueryResult['TASK_ID']},\
# {qcTosTaskQueryResult['VBT_ID']},\
# '{qcTosTaskQueryResult['TASK_TYPE']}',\
# '{qcTosTaskQueryResult['TASK_STATUS']}',\
# '{qcTosTaskQueryResult['ORIG_WSLOC']}',\
# '{qcTosTaskQueryResult['DEST_WS_LOC']}',\
# '{qcTosTaskQueryResult['UNLOCK_TIME']}',\
# '{DATA_FROM}',\
# '{DATA_FROM_TYPE}',\
# '{NOTES}',\
# {paired_value}
# )'''
#         o.executesql(insertsql)
#         #####QC_TOS_TASK的UNLOCK_TIME处理记录###########
#
#         ###配对值
#         paired_value = snowFlakeId()
#         #####INSERT过的数据处理插入###########
#         #同一条任务可能QCMS拉取过多次，因此，需要将QC_TOS_TASK_HIS的数据也拉出来看看，到底INSERT过几次
#         qcTosTaskHisQuerySql=f"""select * from {QC_TOS_TASK_HIS} where TRIGGER_ACTION='INSERT' and TASK_ID={qcTosTaskQueryResult['TASK_ID']} order by TRIG_CREATED asc"""
#         qcTosTaskHisQueryResults = o.query(qcTosTaskHisQuerySql,t='df')
#         if isinstance(qcTosTaskHisQueryResults,pd.DataFrame):#查询出来数据不为空，将所有插入过的数据记录到对应的表中
#             for qcTosTaskHisQueryResult in qcTosTaskHisQueryResults.iterrows():#遍历每一条插入过的数据
#                 DATA_FROM = 'QCMSDB.QC_TOS_TASK_HIS.TRIG_CREATED.INSERT'
#                 DATA_FROM_TYPE='QCMS'
#                 NOTES= '任务创建时间'
#                 insertsql = f'''insert into {tablename_for_kpi}(\
#                 STS_NO,\
#                 TASK_ID,\
#                 VBT_ID,\
#                 TASK_TYPE,\
#                 TASK_STATUS,\
#                 ORIG_WSLOC,\
#                 DEST_WS_LOC,\
#                 KEYTIME,\
#                 DATA_FROM,\
#                 DATA_FROM_TYPE,\
#                 NOTES,\
#                 PAIRED_VALUE) VALUES (
# '{qcTosTaskHisQueryResult[1]['STS_NO']}',\
# {qcTosTaskHisQueryResult[1]['TASK_ID']},\
# {qcTosTaskHisQueryResult[1]['VBT_ID']},\
# '{qcTosTaskHisQueryResult[1]['TASK_TYPE']}',\
# '{qcTosTaskHisQueryResult[1]['TASK_STATUS']}',\
# '{qcTosTaskHisQueryResult[1]['ORIG_WSLOC']}',\
# '{qcTosTaskHisQueryResult[1]['DEST_WS_LOC']}',\
# '{qcTosTaskHisQueryResult[1]['TRIG_CREATED']}',\
# '{DATA_FROM}',\
# '{DATA_FROM_TYPE}',\
# '{NOTES}',\
# {paired_value}
# )'''
#                 o.executesql(insertsql)
#         #####INSERT过的数据处理插入###########
#
#         #####DELETE过的数据处理插入###########
#         #同一条任务可能QCMS拉取过多次，因此，QCMS可能删除过多次，需要将QC_TOS_TASK_HIS的数据也拉出来看看，到底DELETE过几次
#         qcTosTaskHisQuerySql=f"""select * from {QC_TOS_TASK_HIS} where TRIGGER_ACTION='DELETE' and TASK_ID={qcTosTaskQueryResult['TASK_ID']} order by TRIG_CREATED asc"""
#         qcTosTaskHisQueryResults = o.query(qcTosTaskHisQuerySql,t='df')
#         if isinstance(qcTosTaskHisQueryResults,pd.DataFrame):#查询出来数据不为空，将所有插入过的数据记录到对应的表中
#             for qcTosTaskHisQueryResult in qcTosTaskHisQueryResults.iterrows():#遍历每一条插入过的数据
#                 DATA_FROM = 'QCMSDB.QC_TOS_TASK_HIS.TRIGGER_ACTION(DELETE).TRIG_CREATED'
#                 DATA_FROM_TYPE='QCMS'
#                 NOTES= 'QCMS删除任务信息时间'
#                 querydeletesql = f'''insert into {tablename_for_kpi}(\
#                 STS_NO,\
#                 TASK_ID,\
#                 VBT_ID,\
#                 TASK_TYPE,\
#                 TASK_STATUS,\
#                 ORIG_WSLOC,\
#                 DEST_WS_LOC,\
#                 KEYTIME,\
#                 DATA_FROM,\
#                 DATA_FROM_TYPE,\
#                 NOTES,\
#                 PAIRED_VALUE) VALUES (
# '{qcTosTaskHisQueryResult[1]['STS_NO']}',\
# {qcTosTaskHisQueryResult[1]['TASK_ID']},\
# {qcTosTaskHisQueryResult[1]['VBT_ID']},\
# '{qcTosTaskHisQueryResult[1]['TASK_TYPE']}',\
# '{qcTosTaskHisQueryResult[1]['TASK_STATUS']}',\
# '{qcTosTaskHisQueryResult[1]['ORIG_WSLOC']}',\
# '{qcTosTaskHisQueryResult[1]['DEST_WS_LOC']}',\
# '{qcTosTaskHisQueryResult[1]['TRIG_CREATED']}',\
# '{DATA_FROM}',\
# '{DATA_FROM_TYPE}',\
# '{NOTES}',\
# {paired_value}
# )'''
#                 o.executesql(querydeletesql)
# #####INSERT过的数据处理插入###################################
#
#
#
# ##############################################################################QC_GANTRY_INSTRUCTION表的计算(有配对值)
# querysqlforQcGantryInstruction= f"""select * from {QC_GANTRY_INSTRUCTION} where (START_TIME>'{startTime}' and START_TIME<'{endTime}') or (END_TIME>'{startTime}' and END_TIME<'{endTime}') order by START_TIME asc"""
# qcGantryInstructionQueryResults = o.query(querysqlforQcGantryInstruction,t='df')
# if isinstance(qcGantryInstructionQueryResults,pd.DataFrame):#遍历大车移动过的数据
#     for qcGantryInstructionQueryResult in qcGantryInstructionQueryResults.iterrows():
#
#         #配对值
#         paired_value = snowFlakeId()
#
#         DATA_FROM = 'QCMSDB.QC_GANTRY_INSTRUCTION.START_TIME'  # 需要改的部分
#         DATA_FROM_TYPE='QCMS'
#         NOTES= f"""大车指令开始时间，指令状态{qcGantryInstructionQueryResult[1]['INSTR_STATE']}"""#需要改的部分
#         insertsql = f'''insert into {tablename_for_kpi}(\
#         STS_NO,\
#         TASK_REF_ID_FOR_GANTRY,\
#         TASK_ID_FOR_GANTRY,\
#         KEYTIME,\
#         DATA_FROM,\
#         DATA_FROM_TYPE,\
#         NOTES,\
#         PAIRED_VALUE) VALUES (
#         '{qcGantryInstructionQueryResult[1]['QC_ID']}',\
#         {qcGantryInstructionQueryResult[1]['TASK_REF_ID']},\
#         '{qcGantryInstructionQueryResult[1]['TASK_ID']}',\
#         '{qcGantryInstructionQueryResult[1]['START_TIME']}',\
#         '{DATA_FROM}',\
#         '{DATA_FROM_TYPE}',\
#         '{NOTES}',\
#         {paired_value}
#         )'''
#         # print(insertsql)
#         o.executesql(insertsql)
#
#
#         DATA_FROM = 'QCMSDB.QC_GANTRY_INSTRUCTION.END_TIME'  # 需要改的部分
#         DATA_FROM_TYPE='QCMS'
#         NOTES= f"""大车指令结束时间，指令状态{qcGantryInstructionQueryResult[1]['INSTR_STATE']}"""#需要改的部分
#         insertsql = f'''insert into {tablename_for_kpi}(\
#         STS_NO,\
#         TASK_REF_ID_FOR_GANTRY,\
#         TASK_ID_FOR_GANTRY,\
#         KEYTIME,\
#         DATA_FROM,\
#         DATA_FROM_TYPE,\
#         NOTES,\
#         PAIRED_VALUE) VALUES (
#         '{qcGantryInstructionQueryResult[1]['QC_ID']}',\
#         {qcGantryInstructionQueryResult[1]['TASK_REF_ID']},\
#         '{qcGantryInstructionQueryResult[1]['TASK_ID']}',\
#         '{qcGantryInstructionQueryResult[1]['END_TIME']}',\
#         '{DATA_FROM}',\
#         '{DATA_FROM_TYPE}',\
#         '{NOTES}',\
#         {paired_value}
#         )'''
#         # print(insertsql)
#         o.executesql(insertsql)
# ##############################################################################
#
#
#
#
#
#
# ##############################################################################QC_TROLLEY_INSTRUCTION表的计算（有配对值）
# QcTrolleyInstruction= f"""select * from {QC_TROLLEY_INSTRUCTION} where (START_TIME>'{startTime}' and START_TIME<'{endTime}') or (END_TIME>'{startTime}' and END_TIME<'{endTime}') order by START_TIME asc"""
# QcTrolleyInstructionQueryResults = o.query(QcTrolleyInstruction,t='df')
# if isinstance(QcTrolleyInstructionQueryResults,pd.DataFrame):#遍历小车执行过的数据
#     for QcTrolleyInstructionQueryResult in QcTrolleyInstructionQueryResults.iterrows():
#         ##设置配对值
#         paired_value = snowFlakeId()
#
#         DATA_FROM =f'''QCMSDB.QC_TROLLEY_INSTRUCTION.{QcTrolleyInstructionQueryResult[1]['INSTR_TYPE']}.START_TIME'''  # 需要改的部分
#         DATA_FROM_TYPE='QCMS'
#         NOTES= f"""小车指令开始时间，指令类型{QcTrolleyInstructionQueryResult[1]['INSTR_TYPE']}，指令状态{QcTrolleyInstructionQueryResult[1]['INSTR_STATE']}"""#需要改的部分
#         insertsql = f'''insert into {tablename_for_kpi}(\
#         STS_NO,\
#         INSTR_ID_FOR_MT_TROLLEY,\
#         TASK_REF_ID_FOR_MT_TROLLEY,\
#         TASK_ID_FOR_MT_TROLLEY,\
#         KEYTIME,\
#         DATA_FROM,\
#         DATA_FROM_TYPE,\
#         NOTES,\
#         PAIRED_VALUE) VALUES (
#         '{QcTrolleyInstructionQueryResult[1]['QC_ID']}',\
#         '{QcTrolleyInstructionQueryResult[1]['INSTR_ID']}',\
#         {QcTrolleyInstructionQueryResult[1]['TASK_REF_ID']},\
#         '{QcTrolleyInstructionQueryResult[1]['TASK_ID']}',\
#         '{QcTrolleyInstructionQueryResult[1]['START_TIME']}',\
#         '{DATA_FROM}',\
#         '{DATA_FROM_TYPE}',\
#         '{NOTES}',\
#         {paired_value}
#         )'''
#         # print(insertsql)
#         o.executesql(insertsql)
#
#
#         DATA_FROM =f'''QCMSDB.QC_TROLLEY_INSTRUCTION.{QcTrolleyInstructionQueryResult[1]['INSTR_TYPE']}.END_TIME'''  # 需要改的部分
#         DATA_FROM_TYPE='QCMS'
#         NOTES= f"""小车指令结束时间，指令类型{QcTrolleyInstructionQueryResult[1]['INSTR_TYPE']}，指令状态{QcTrolleyInstructionQueryResult[1]['INSTR_STATE']}"""#需要改的部分
#         insertsql = f'''insert into {tablename_for_kpi}(\
#         STS_NO,\
#         INSTR_ID_FOR_MT_TROLLEY,\
#         TASK_REF_ID_FOR_MT_TROLLEY,\
#         TASK_ID_FOR_MT_TROLLEY,\
#         KEYTIME,\
#         DATA_FROM,\
#         DATA_FROM_TYPE,\
#         NOTES,\
#         PAIRED_VALUE) VALUES (
#         '{QcTrolleyInstructionQueryResult[1]['QC_ID']}',\
#         '{QcTrolleyInstructionQueryResult[1]['INSTR_ID']}',\
#         {QcTrolleyInstructionQueryResult[1]['TASK_REF_ID']},\
#         '{QcTrolleyInstructionQueryResult[1]['TASK_ID']}',\
#         '{QcTrolleyInstructionQueryResult[1]['END_TIME']}',\
#         '{DATA_FROM}',\
#         '{DATA_FROM_TYPE}',\
#         '{NOTES}',\
#         {paired_value}
#         )'''
#         # print(insertsql)
#         o.executesql(insertsql)
# ##############################################################################
#
#
#
#
# ##############################################################################QC_CONTAINER_TRANSFER表的计算(无配对值)
# QcContainerTransferQuerySql= f"""select * from {QC_CONTAINER_TRANSFER} where (CREATE_TIME>'{startTime}' and CREATE_TIME<'{endTime}') and GANTRY_POSITION!=0 and TROLLEY_POSITION!=0 and HOIST_POSITION!=0 order by CREATE_TIME asc"""
# QcContainerTransferQueryResults = o.query(QcContainerTransferQuerySql,t='df')
# if isinstance(QcContainerTransferQueryResults,pd.DataFrame):#遍历
#     for QcContainerTransferQueryResult in QcContainerTransferQueryResults.iterrows():
#         DATA_FROM = f'''QCMSDB.QC_CONTAINER_TRANSFER.{QcContainerTransferQueryResult[1]['INSTR_TYPE']}.CREATE_TIME'''  # 需要改的部分
#         DATA_FROM_TYPE='QCMS'
#         NOTES= f"""抓放箱记录指令类型{QcContainerTransferQueryResult[1]['INSTR_TYPE']}，吊具尺寸{QcContainerTransferQueryResult[1]['SPREADER_SIZE']}"""#需要改的部分
#         insertsql = f'''insert into {tablename_for_kpi}(\
#         STS_NO,\
#         TRANS_CHAIN_ID,\
#         OPERATE_MODE_FOR_CTNTRANS,\
#         SPREADER_SIZE_FOR_CTNTRANS,\
#         WORK_LOCATION_FOR_CTNTRANS,\
#         KEYTIME,\
#         DATA_FROM,\
#         DATA_FROM_TYPE,\
#         NOTES) VALUES (
#         '{QcContainerTransferQueryResult[1]['QC_ID']}',\
#         '{QcContainerTransferQueryResult[1]['TRANS_CHAIN_ID']}',\
#         '{QcContainerTransferQueryResult[1]['OPERATE_MODE']}',\
#         '{QcContainerTransferQueryResult[1]['SPREADER_SIZE']}',\
#         '{QcContainerTransferQueryResult[1]['WORK_LOCATION']}',\
#         '{QcContainerTransferQueryResult[1]['CREATE_TIME']}',\
#         '{DATA_FROM}',\
#         '{DATA_FROM_TYPE}',\
#         '{NOTES}'\
#         )'''
#         # print(insertsql)
#         o.executesql(insertsql)
# ##############################################################################
#
#
# ################################################################################单机 kpi_mt_step_log（有配对值）
# kpiMtStepLogQuerySql = f"select * from kpi_mt_step_log where (start_time>'{startTimeforKpi}' and start_time<'{endTimeforKpi}') and (end_time>'{startTimeforKpi}' and end_time<'{endTimeforKpi}') order by start_time asc"
# kpiMtStepLogQueryResults = o.query(kpiMtStepLogQuerySql,t='df')
# # 查询出来不为空才进行下一步
# if isinstance(kpiMtStepLogQueryResults,pd.DataFrame):
#     # 遍历每条数据，并且每条数据都有start_time和end_time,应该进行for插入{tablename_for_kpi}
#     for kpiMtStepLogQueryResult in kpiMtStepLogQueryResults.iterrows():
#
#         ##插入数据前生成唯一的值用来放到数据库配对
#         paired_value = snowFlakeId()
#
#         DATA_FROM = f"""KPIDB.kpi_mt_step_log.{kpiMtStepLogQueryResult[1]['step_id']}.start_time"""  # 需要改的部分
#         DATA_FROM_TYPE = 'KPI'
#         NOTES = f"""KPI记录的step={kpiMtStepLogQueryResult[1]['step_id']}{stepTransToLanguage(kpiMtStepLogQueryResult[1]['step_id'])} 对应的start_time"""  # 需要改的部分
#         insertsql = f'''insert into {tablename_for_kpi}(\
#         STS_NO,\
#         TASK_ID,\
#         KEYTIME,\
#         DATA_FROM,\
#         DATA_FROM_TYPE,\
#         NOTES,\
#         PAIRED_VALUE) VALUES (
#     '{kpiMtStepLogQueryResult[1]['crane_id']}',\
#     {kpiMtStepLogQueryResult[1]['task_id_low']},\
#     '{convertUtc_5(strChangeTime(kpiMtStepLogQueryResult[1]['start_time']))}',\
#     '{DATA_FROM}',\
#     '{DATA_FROM_TYPE}',\
#     '{NOTES}',\
#     {paired_value}
#     )'''
#         # print(insertsql)
#         o.executesql(insertsql)
#
#
#
#         DATA_FROM = f"""KPIDB.kpi_mt_step_log.{kpiMtStepLogQueryResult[1]['step_id']}.end_time"""  # 需要改的部分
#         DATA_FROM_TYPE = 'KPI'
#         NOTES = f"""KPI记录的step={kpiMtStepLogQueryResult[1]['step_id']}{stepTransToLanguage(kpiMtStepLogQueryResult[1]['step_id'])} 对应的end_time"""  # 需要改的部分
#         insertsql = f'''insert into {tablename_for_kpi}(\
#         STS_NO,\
#         TASK_ID,\
#         KEYTIME,\
#         DATA_FROM,\
#         DATA_FROM_TYPE,\
#         NOTES,\
#         PAIRED_VALUE) VALUES (
#     '{kpiMtStepLogQueryResult[1]['crane_id']}',\
#     {kpiMtStepLogQueryResult[1]['task_id_low']},\
#     '{convertUtc_5(strChangeTime(kpiMtStepLogQueryResult[1]['end_time']))}',\
#     '{DATA_FROM}',\
#     '{DATA_FROM_TYPE}',\
#     '{NOTES}',\
#     {paired_value}
#     )'''
#         # print(insertsql)
#         o.executesql(insertsql)
# ################################################################################kpi_mt_step_log
#
#
#
# #############################################################################对QC_TP_INTERACTION_HIS的处理
# qcnos = [103,104,105,106,107,108]#岸桥编号
# lanenos = [1,2,3,4,5,6,7]#岸桥下车道编号
# for qcno in qcnos:
#     for laneno in lanenos:
#         #查询时间段内交互表的变化
#         querySqlForQcTpInteractionHis = f'''select * from {qc_tp_interaction_his} where QC_ID={qcno} and LANE_ID={laneno} and (TRIG_CREATED>'{startTime}' and  TRIG_CREATED<'{endTime}') ORDER BY TRIG_CREATED asc'''
#         qcTpInteractionHisQuryResults = o.query(querySqlForQcTpInteractionHis,t='df')
#         if isinstance(qcTpInteractionHisQuryResults,pd.DataFrame):
#             # 遍历行并比较相邻行的字段值
#             for indexForQcTp,qcTpInteractionHisQuryResult in qcTpInteractionHisQuryResults.iterrows():#####遍历每一行数据
#                 ############从第2行数据开始
#                 if indexForQcTp >= 1:  # 表示从第2行开始计算数据
#                     last_row = qcTpInteractionHisQuryResults.iloc[indexForQcTp-1]#上一行数据
#                     current_row = qcTpInteractionHisQuryResults.iloc[indexForQcTp]#当前行数据
#                     # 遍历上一行的每个字段和对应的值
#                     changedata = {}
#                     for column in ['QC_ID','LANE_ID','FMS_JOB_POS','FMS_AHT_ID','FMS_MOVE_KIND','FMS_AHT_STATUS','QC_REF1','QC_REF2','QC_STATUS']:#遍历这些字段乳沟有变化记录
#                         last_val = last_row[column]
#                         current_val = current_row[column]
#                         if last_val!=current_val:#如果上一行字段跟下一行的值有变化则需要添加到字典中
#                             changedata[column]=current_val
#
#                     if changedata!={}:#如果当前行的有变化值
#                         #将字典的值转换为逗号分隔的字符串
#                         changedatavalues = '.'.join(str(value) for value in changedata.values())#将变化的值存一下，后面需要打入数据库中
#
#                         DATA_FROM = f"""QCMSDB.QC_TP_INTERACTION_HIS.{current_row['QC_ID']}.{current_row['LANE_ID']}.{changedatavalues}"""  # 需要改的部分
#                         DATA_FROM_TYPE = 'QCMS'
#                         NOTES = f'''{changedata}'''  # 需要改的部分
#                         insertsql = f'''insert into {tablename_for_kpi}(\
#                             STS_NO,\
#                             KEYTIME,\
#                             DATA_FROM,\
#                             DATA_FROM_TYPE,\
#                             NOTES) VALUES (
#                     '{qcTpInteractionHisQuryResult['QC_ID']}',\
#                     '{qcTpInteractionHisQuryResult['TRIG_CREATED']}',\
#                     '{DATA_FROM}',\
#                     '{DATA_FROM_TYPE}',\
#                     "{NOTES}")'''
#                     o.executesql(insertsql)
# #########################################################对QC_TP_INTERACTION_HIS的处理



# # #########################################################对OPCUA数据处理：MtWorkMode的处理
# qcnos = ['103','104','105','106','107','108']#岸桥编号
# ####第0步骤，删除错误数据
# deletesql=f"""delete from {MtWorkMode} where StatusCode='BadCommunicationError'"""
# o.executesql(deletesql)
#
# ###第一步先将数据库中字段带SourceTime''的时间去掉
# querySqlForMtWorkMode = f'''select * from {MtWorkMode}'''
# mtWorkModeQuryResults = o.query(querySqlForMtWorkMode,t='df')
# if isinstance(mtWorkModeQuryResults,pd.DataFrame):
#     for indexForMtWorkMode, mtWorkModeQuryResult in mtWorkModeQuryResults.iterrows():  #####遍历每一行数据
#         newtimestr = mtWorkModeQuryResult['SourceTime'].replace("'","")
#         updatesql =f"""update {MtWorkMode} set SourceTime='{newtimestr}' where ID={mtWorkModeQuryResult['ID']}"""
#         o.executesql(updatesql)
#
#
# #第2步将重复的数据去掉
# for qcno in qcnos:
#     # 将字符串转换为datetime对象
#     querySqlForMtWorkMode = f'''select * from {MtWorkMode} where QC_ID='{qcno}' and (SourceTime>'{startTime}' and  SourceTime<'{endTime}') ORDER BY SourceTime asc'''
#     mtWorkModeQuryResults = o.query(querySqlForMtWorkMode,t='df')
#     if isinstance(mtWorkModeQuryResults, pd.DataFrame):
#         # 遍历行并比较相邻行的字段值
#         for indexForMtWorkMode,mtWorkModeQuryResult in mtWorkModeQuryResults.iterrows():#####遍历每一行数据
#             ############从第2行数据开始
#             if indexForMtWorkMode >= 1:  # 表示从第2行开始计算数据
#                 last_row = mtWorkModeQuryResults.iloc[indexForMtWorkMode-1]#上一行数据
#                 current_row = mtWorkModeQuryResults.iloc[indexForMtWorkMode]#当前行数据
#                 if last_row['Value']==current_row['Value']:#检测到上一行数据的Value值和下一行一致，删除上一行数据
#                     deletesql=f"""delete from {MtWorkMode} where ID={last_row['ID']}"""
#                     o.executesql(deletesql)
#
# #第3步将需要的数据插入到目标数据库表中
# for qcno in qcnos:
#     querySqlForMtWorkMode = f'''select * from {MtWorkMode} where QC_ID='{qcno}' and (SourceTime>'{startTime}' and  SourceTime<'{endTime}') ORDER BY SourceTime asc'''
#     mtWorkModeQuryResults = o.query(querySqlForMtWorkMode,t='df')
#     if isinstance(mtWorkModeQuryResults,pd.DataFrame):
#         for indexForMtWorkMode,mtWorkModeQuryResult in mtWorkModeQuryResults.iterrows():#遍历每一行数据
#             DATA_FROM = f"""QCMSDB.{MtWorkMode}.{mtWorkModeQuryResult['Value']}"""  # 需要改的部分
#             DATA_FROM_TYPE = 'OPCUA'
#             NOTES = f'''{mtWorkModeQuryResult['notes']}'''  # 需要改的部分
#             insertsql = f'''insert into {tablename_for_kpi}(\
#                 STS_NO,\
#                 KEYTIME,\
#                 DATA_FROM,\
#                 DATA_FROM_TYPE,\
#                 NOTES) VALUES (
#         '{mtWorkModeQuryResult['QC_ID']}',\
#         '{mtWorkModeQuryResult['SourceTime']}',\
#         '{DATA_FROM}',\
#         '{DATA_FROM_TYPE}',\
#         "{NOTES}")'''
#             o.executesql(insertsql)
# #########################################################对OPCUA数据处理：MtWorkMode的处理



# # #########################################################对OPCUA数据处理：MtInstructionStatus的处理
# qcnos = ['103','104','105','106','107','108']#岸桥编号
# ####第0步骤，删除错误数据
# deletesql=f"""delete from {MtInstructionStatus} where StatusCode='BadCommunicationError'"""
# o.executesql(deletesql)
#
# ###第一步先将数据库中字段带SourceTime''的时间去掉
# querySqlForMtInstructionStatus = f'''select * from {MtInstructionStatus}'''
# mtInstructionStatusQuryResults = o.query(querySqlForMtInstructionStatus,t='df')
# if isinstance(mtInstructionStatusQuryResults,pd.DataFrame):
#     for indexForMtInstructionStatus, mtInstructionStatusQuryResult in mtInstructionStatusQuryResults.iterrows():  #####遍历每一行数据
#         newtimestr = mtInstructionStatusQuryResult['SourceTime'].replace("'","")
#         updatesql =f"""update {MtInstructionStatus} set SourceTime='{newtimestr}' where ID={mtInstructionStatusQuryResult['ID']}"""
#         o.executesql(updatesql)
#
#
# #第2步将重复的数据去掉
# for qcno in qcnos:
#     # 将字符串转换为datetime对象
#     querySqlForMtInstructionStatus = f'''select * from {MtInstructionStatus} where QC_ID='{qcno}' and (SourceTime>'{startTime}' and  SourceTime<'{endTime}') ORDER BY SourceTime asc'''
#     mtInstructionStatusQuryResults = o.query(querySqlForMtInstructionStatus,t='df')
#     if isinstance(mtInstructionStatusQuryResults, pd.DataFrame):
#         # 遍历行并比较相邻行的字段值
#         for indexForMtInstructionStatus,mtInstructionStatusQuryResult in mtInstructionStatusQuryResults.iterrows():#####遍历每一行数据
#             ############从第2行数据开始
#             if indexForMtInstructionStatus >= 1:  # 表示从第2行开始计算数据
#                 last_row = mtInstructionStatusQuryResults.iloc[indexForMtInstructionStatus-1]#上一行数据
#                 current_row = mtInstructionStatusQuryResults.iloc[indexForMtInstructionStatus]#当前行数据
#                 if last_row['Value']==current_row['Value']:#检测到上一行数据的Value值和下一行一致，删除上一行数据
#                     deletesql=f"""delete from {MtInstructionStatus} where ID={last_row['ID']}"""
#                     o.executesql(deletesql)
#
# #第3步将需要的数据插入到目标数据库表中
# for qcno in qcnos:
#     querySqlForMtInstructionStatus = f'''select * from {MtInstructionStatus} where QC_ID='{qcno}' and (SourceTime>'{startTime}' and  SourceTime<'{endTime}') ORDER BY SourceTime asc'''
#     mtInstructionStatusQuryResults = o.query(querySqlForMtInstructionStatus,t='df')
#     if isinstance(mtInstructionStatusQuryResults,pd.DataFrame):
#         for indexForMtInstructionStatus,mtInstructionStatusQuryResult in mtInstructionStatusQuryResults.iterrows():#遍历每一行数据
#             DATA_FROM = f"""QCMSDB.{MtInstructionStatus}.{mtInstructionStatusQuryResult['Value']}"""  # 需要改的部分
#             DATA_FROM_TYPE = 'OPCUA'
#             NOTES = f'''{mtInstructionStatusQuryResult['notes']}'''  # 需要改的部分
#             insertsql = f'''insert into {tablename_for_kpi}(\
#                 STS_NO,\
#                 KEYTIME,\
#                 DATA_FROM,\
#                 DATA_FROM_TYPE,\
#                 NOTES) VALUES (
#         '{mtInstructionStatusQuryResult['QC_ID']}',\
#         '{mtInstructionStatusQuryResult['SourceTime']}',\
#         '{DATA_FROM}',\
#         '{DATA_FROM_TYPE}',\
#         "{NOTES}")'''
#             o.executesql(insertsql)
# #########################################################对OPCUA数据处理：MtInstructionStatus的处理



# #########################################################对OPCUA数据处理：MhAboveSafeHeight的处理
qcnos = ['103','104','105','106','107','108']#岸桥编号
# ####第0步骤，删除错误数据
# deletesql=f"""delete from {MhAboveSafeHeight} where StatusCode='BadCommunicationError'"""
# o.executesql(deletesql)

# ###第一步先将数据库中字段带SourceTime''的时间去掉
# querySqlForMhAboveSafeHeight = f'''select * from {MhAboveSafeHeight}'''
# mhAboveSafeHeightQuryResults = o.query(querySqlForMhAboveSafeHeight,t='df')
# if isinstance(mhAboveSafeHeightQuryResults,pd.DataFrame):
#     for indexForMhAboveSafeHeight, mhAboveSafeHeightQuryResult in mhAboveSafeHeightQuryResults.iterrows():  #####遍历每一行数据
#         newtimestr = mhAboveSafeHeightQuryResult['SourceTime'].replace("'","")
#         updatesql =f"""update {MhAboveSafeHeight} set SourceTime='{newtimestr}' where ID={mhAboveSafeHeightQuryResult['ID']}"""
#         o.executesql(updatesql)


# #第2步将重复的数据去掉
# for qcno in qcnos:
#     # 将字符串转换为datetime对象
#     querySqlForMhAboveSafeHeight = f'''select * from {MhAboveSafeHeight} where QC_ID='{qcno}' and (SourceTime>'{startTime}' and  SourceTime<'{endTime}') ORDER BY SourceTime asc'''
#     mhAboveSafeHeightQuryResults = o.query(querySqlForMhAboveSafeHeight,t='df')
#     if isinstance(mhAboveSafeHeightQuryResults, pd.DataFrame):
#         # 遍历行并比较相邻行的字段值
#         for indexForMhAboveSafeHeight,mhAboveSafeHeightQuryResult in mhAboveSafeHeightQuryResults.iterrows():#####遍历每一行数据
#             ############从第2行数据开始
#             if indexForMhAboveSafeHeight >= 1:  # 表示从第2行开始计算数据
#                 last_row = mhAboveSafeHeightQuryResults.iloc[indexForMhAboveSafeHeight-1]#上一行数据
#                 current_row = mhAboveSafeHeightQuryResults.iloc[indexForMhAboveSafeHeight]#当前行数据
#                 if last_row['Value']==current_row['Value']:#检测到上一行数据的Value值和下一行一致，删除上一行数据
#                     deletesql=f"""delete from {MhAboveSafeHeight} where ID={last_row['ID']}"""
#                     o.executesql(deletesql)

# #第3步将需要的数据插入到目标数据库表中
# for qcno in qcnos:
#     querySqlForMhAboveSafeHeight = f'''select * from {MhAboveSafeHeight} where QC_ID='{qcno}' and (SourceTime>'{startTime}' and  SourceTime<'{endTime}') ORDER BY SourceTime asc'''
#     mhAboveSafeHeightQuryResults = o.query(querySqlForMhAboveSafeHeight,t='df')
#     if isinstance(mhAboveSafeHeightQuryResults,pd.DataFrame):
#         for indexForMhAboveSafeHeight,mhAboveSafeHeightQuryResult in mhAboveSafeHeightQuryResults.iterrows():#遍历每一行数据
#             DATA_FROM = f"""QCMSDB.{MhAboveSafeHeight}.{mhAboveSafeHeightQuryResult['Value']}"""  # 需要改的部分
#             DATA_FROM_TYPE = 'OPCUA'
#             NOTES = f'''{mhAboveSafeHeightQuryResult['notes']}'''  # 需要改的部分
#             insertsql = f'''insert into {tablename_for_kpi}(\
#                 STS_NO,\
#                 KEYTIME,\
#                 DATA_FROM,\
#                 DATA_FROM_TYPE,\
#                 NOTES) VALUES (
#         '{mhAboveSafeHeightQuryResult['QC_ID']}',\
#         '{mhAboveSafeHeightQuryResult['SourceTime']}',\
#         '{DATA_FROM}',\
#         '{DATA_FROM_TYPE}',\
#         "{NOTES}")'''
#             o.executesql(insertsql)
# #########################################################对OPCUA数据处理：MtInstructionStatus的处理