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
QC_TOS_TASK='QC_TOS_TASK'
QC_TOS_TASK_HIS='QC_TOS_TASK_HIS'
tablename_for_kpi='kpi_for_qcms'
QC_GANTRY_INSTRUCTION='QC_GANTRY_INSTRUCTION'
#连接数据库
o=sqliteHandle.sqliteHandler(r'./kpiforQcms20250109.db')


########################处理QC_TOS_TASK和QC_TOS_TASK_HIS中的数据，将时间内的变化节点都打入数据中
# #查询QC_TOS_TASK在规定的时间区间内到底有多少条任务
# qcTosTaskQuerySql = f'''select * from {QC_TOS_TASK} where (LOCK_TIME>'{startTime}' and  LOCK_TIME<'{endTime}') or (UNLOCK_TIME>'{startTime}' and  UNLOCK_TIME<'{endTime}') order by TASK_ID asc'''
# qcTosTaskQueryResults = o.query(qcTosTaskQuerySql,t='df')
# if isinstance(qcTosTaskQueryResults,pd.DataFrame):#能查询出来任务
#     for index,qcTosTaskQueryResult in qcTosTaskQueryResults.iterrows():#遍历每一条查询出来的数据
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
#         NOTES) VALUES (
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
# '{NOTES}'\
# )'''
#         o.executesql(insertsql)
#         #####QC_TOS_TASK的LOCK_TIME处理记录###########

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
#         NOTES) VALUES (
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
# '{NOTES}'\
# )'''
#         o.executesql(insertsql)
#         #####QC_TOS_TASK的UNLOCK_TIME处理记录###########




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
#                 NOTES) VALUES (
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
# '{NOTES}'\
# )'''
#                 o.executesql(insertsql)
#         #####INSERT过的数据处理插入###########

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
#                 NOTES) VALUES (
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
# '{NOTES}'\
# )'''
#                 o.executesql(querydeletesql)
#         #####INSERT过的数据处理插入###########



# ##############################################################################QC_GANTRY_INSTRUCTION表的计算
# querysqlforQcGantryInstruction= f"""select * from {QC_GANTRY_INSTRUCTION} where (START_TIME>'{startTime}' and START_TIME<'{endTime}') or (END_TIME>'{startTime}' and END_TIME<'{endTime}') order by START_TIME asc"""
# qcGantryInstructionQueryResults = o.query(querysqlforQcGantryInstruction,t='df')
# print(querysqlforQcGantryInstruction)
#
# print(qcGantryInstructionQueryResults)
# if isinstance(qcGantryInstructionQueryResults,pd.DataFrame):#遍历大车移动过的数据
#     for qcGantryInstructionQueryResult in qcGantryInstructionQueryResults.iterrows():
#         DATA_FROM = 'QCMSDB.QC_GANTRY_INSTRUCTION.START_TIME'  # 需要改的部分
#         DATA_FROM_TYPE='QCMS'
#         NOTES= f"""大车指令开始时间，指令状态{qcGantryInstructionQueryResult[1]['INSTR_STATE']}"""#需要改的部分
#         insertsql = f'''insert into {tablename_for_kpi}(\
#         STS_NO,\
#         TASK_REF_ID,\
#         TASK_ID_FOR_GANTRY,\
#         KEYTIME,\
#         DATA_FROM,\
#         DATA_FROM_TYPE,\
#         NOTES) VALUES (
#         '{qcGantryInstructionQueryResult[1]['QC_ID']}',\
#         {qcGantryInstructionQueryResult[1]['TASK_REF_ID']},\
#         '{qcGantryInstructionQueryResult[1]['TASK_ID']}',\
#         '{qcGantryInstructionQueryResult[1]['START_TIME']}',\
#         '{DATA_FROM}',\
#         '{DATA_FROM_TYPE}',\
#         '{NOTES}'\
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
#         TASK_REF_ID,\
#         TASK_ID_FOR_GANTRY,\
#         KEYTIME,\
#         DATA_FROM,\
#         DATA_FROM_TYPE,\
#         NOTES) VALUES (
#         '{qcGantryInstructionQueryResult[1]['QC_ID']}',\
#         {qcGantryInstructionQueryResult[1]['TASK_REF_ID']},\
#         '{qcGantryInstructionQueryResult[1]['TASK_ID']}',\
#         '{qcGantryInstructionQueryResult[1]['END_TIME']}',\
#         '{DATA_FROM}',\
#         '{DATA_FROM_TYPE}',\
#         '{NOTES}'\
#         )'''
#         # print(insertsql)
#         o.executesql(insertsql)
# ##############################################################################





# querysqlforgantry= f'''select * from qc_trolley_task where TASK_REF_ID={qc_tos_tasks_queryresult['TASK_ID']} order by START_TIME desc'''#先从qc_trolley_task查询出来对应的TASK_ID
#                 #查询QC_trolley_task中的QC_trolley_task.TASK_ID=QC_GANTRY_INSTRUCTION.TASK_ID
#                 qctrolleytaskqueryresults = o.query(querysqlforgantry)
#                 if len(qctrolleytaskqueryresults)!=0:#查询出来不为空才可以进行下一步
#                     QC_trolley_task_TASK_ID=qctrolleytaskqueryresults[0]['TASK_ID']#查询出来第一个结果的字段
#
#                     #开始查找QC_GANTRY_INSTRUCTION中有无涉及到大车移动的数据
#                     qcGantryInstructionquerysql = f"""select * from QC_GANTRY_INSTRUCTION where TASK_ID='{QC_trolley_task_TASK_ID}' order by START_TIME asc"""
#                     qcGantryInstructionqueryresults = o.query(qcGantryInstructionquerysql)
#                     if len(qcGantryInstructionqueryresults)!=0:#查出来数据不为空，能在大车指令中查到数据，将数据记录到数据表中
#                         for qcGantryInstructionqueryresult in qcGantryInstructionqueryresults:#遍历数据,因为需要记录开始时间和结束时间，因次for2次
#                             for index_gantry_single in range(2):#
#                                 if index_gantry_single==0:#插入指令START_TIME时间
#                                     DATA_FROM='QCMSDB.QC_GANTRY_INSTRUCTION.START_TIME'#需要改的部分
#                                     DATA_FROM_TYPE='QCMS'
#                                     NOTES= f"""大车指令开始时间，指令状态{qcGantryInstructionqueryresult['INSTR_STATE']}"""#需要改的部分
#                                     insertsql = f'''insert into {tablename_for_kpi}(\
#                                     STS_NO,\
#                                     TASK_ID,\
#                                     VBT_ID,\
#                                     TASK_TYPE,\
#                                     TASK_STATUS,\
#                                     ORIG_WSLOC,\
#                                     DEST_WS_LOC,\
#                                     KEYTIME,\
#                                     DATA_FROM,\
#                                     DATA_FROM_TYPE,\
#                                     NOTES) VALUES (
#                     '{qc_tos_tasks_queryresult['STS_NO']}',\
#                     {qc_tos_tasks_queryresult['TASK_ID']},\
#                     {qc_tos_tasks_queryresult['VBT_ID']},\
#                     '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                     '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                     '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                     '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                     '{qcGantryInstructionqueryresult['START_TIME']}',\
#                     '{DATA_FROM}',\
#                     '{DATA_FROM_TYPE}',\
#                     '{NOTES}'\
#                     )'''
#                                     # print(insertsql)
#                                     o.executesql(insertsql)
#                                 if index_gantry_single==1:#插入指令END_TIME时间
#                                     DATA_FROM='QCMSDB.QC_GANTRY_INSTRUCTION.END_TIME'#需要改的部分
#                                     DATA_FROM_TYPE='QCMS'
#                                     NOTES= f"""大车指令结束时间，指令状态{qcGantryInstructionqueryresult['INSTR_STATE']}"""#需要改的部分
#                                     insertsql = f'''insert into {tablename_for_kpi}(\
#                                     STS_NO,\
#                                     TASK_ID,\
#                                     VBT_ID,\
#                                     TASK_TYPE,\
#                                     TASK_STATUS,\
#                                     ORIG_WSLOC,\
#                                     DEST_WS_LOC,\
#                                     KEYTIME,\
#                                     DATA_FROM,\
#                                     DATA_FROM_TYPE,\
#                                     NOTES) VALUES (
#                     '{qc_tos_tasks_queryresult['STS_NO']}',\
#                     {qc_tos_tasks_queryresult['TASK_ID']},\
#                     {qc_tos_tasks_queryresult['VBT_ID']},\
#                     '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                     '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                     '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                     '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                     '{qcGantryInstructionqueryresult['END_TIME']}',\
#                     '{DATA_FROM}',\
#                     '{DATA_FROM_TYPE}',\
#                     '{NOTES}'\
#                     )'''
#                                     # print(insertsql)
#                                     o.executesql(insertsql)







##########################

#
#
# #根据船舶ID查询QCMS收到的TOS任务
# qc_tos_tasks_queryresults = o.query("select * from qc_tos_task where VBT_ID=212427 order by TASK_ID asc")
#
#
#
# if qc_tos_tasks_queryresults!=[]:#能查询出来数据
#
#     # 设置需要写入目标表的表名字
#     tablename_for_kpi = f"""kpi_for_qcms{qc_tos_tasks_queryresults[0]['VBT_ID']}"""
#
#     #如果表不存在则创建表
#     o.createKpiForQcmsTable(tablename_for_kpi)
#
#     o.executesql(f'delete from {tablename_for_kpi}')
#     time.sleep(0.5)
#     for taskidindex,qc_tos_tasks_queryresult in enumerate(qc_tos_tasks_queryresults):#遍历每个数据,将数据插入到{tablename_for_kpi}表中
#         #想打印的时间有多少就得插入几条OPERATE_TIME/LOCK_TIME/UNLOCK_TIME/RESPONSE_TIME
#         for i in range(9):#设置的数量涵盖了表
#             '''
#             i==0处理任务创建时间
#             i==1处理任务闭锁时间
#             i==2处理任务开锁时间
#             i==3处理任务完成时间
#             i==4时处理QC_GANTRY_INSTRUCTION表的数据
#             i==5时处理QC_TROLLEY_INSTRUCRION的数据
#             i==6时处理QC_CONTAINER_TRANSFER表的数据
#             i==7时处理QC_TP_INTERACTION_HIS表的数据
#             i==8时处理ACCS的'kpi_mt_step_log'的数据
#             '''
#             if i==0:#作为创建时间的判断
#                 querysqlforcreates = o.query(f"select * from qc_tos_task_his where TRIGGER_ACTION='INSERT' and TASK_ID={qc_tos_tasks_queryresult['TASK_ID']} order by TRIG_CREATED desc")#查询创建时间
#                 if len(querysqlforcreates)!=0:#能查询出来数据，将第一条数据对应的时间插入到KPI
#                     for querysqlforcreate in querysqlforcreates:#遍历历史表中INSERT的语句，插入几次就是几次
#                         DATA_FROM='QCMSDB.QC_TOS_TASK_HIS.TRIG_CREATED.INSERT'
#                         DATA_FROM_TYPE='QCMS'
#                         NOTES= '任务创建时间'
#                         insertsql = f'''insert into {tablename_for_kpi}(\
#                         STS_NO,\
#                         TASK_ID,\
#                         VBT_ID,\
#                         TASK_TYPE,\
#                         TASK_STATUS,\
#                         ORIG_WSLOC,\
#                         DEST_WS_LOC,\
#                         KEYTIME,\
#                         DATA_FROM,\
#                         DATA_FROM_TYPE,\
#                         NOTES) VALUES (
#         '{querysqlforcreate['STS_NO']}',\
#         {querysqlforcreate['TASK_ID']},\
#         {querysqlforcreate['VBT_ID']},\
#         '{querysqlforcreate['TASK_TYPE']}',\
#         '{querysqlforcreate['TASK_STATUS']}',\
#         '{querysqlforcreate['ORIG_WSLOC']}',\
#         '{querysqlforcreate['DEST_WS_LOC']}',\
#         '{querysqlforcreate['TRIG_CREATED']}',\
#         '{DATA_FROM}',\
#         '{DATA_FROM_TYPE}',\
#         '{NOTES}'\
#         )'''
#                         o.executesql(insertsql)
#             if i==1:#作为LOCK_TIME创建时间的判断
#
#
#                 DATA_FROM='QCMSDB.QC_TOS_TASK.LOCK_TIME'#需要改的部分
#                 DATA_FROM_TYPE='QCMS'
#                 NOTES= '吊具闭锁时间'#需要改的部分
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
#                 NOTES) VALUES (
# '{qc_tos_tasks_queryresult['STS_NO']}',\
# {qc_tos_tasks_queryresult['TASK_ID']},\
# {qc_tos_tasks_queryresult['VBT_ID']},\
# '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
# '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
# '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
# '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
# '{qc_tos_tasks_queryresult['LOCK_TIME']}',\
# '{DATA_FROM}',\
# '{DATA_FROM_TYPE}',\
# '{NOTES}'\
# )'''
#                 # print(insertsql)
#                 o.executesql(insertsql)
#             if i==2:#作为UNLOCK_TIME创建时间的判断
#                 DATA_FROM='QCMSDB.QC_TOS_TASK.UNLOCK_TIME'#需要改的部分
#                 DATA_FROM_TYPE='QCMS'
#                 NOTES= '吊具开锁时间'#需要改的部分
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
#                 NOTES) VALUES (
# '{qc_tos_tasks_queryresult['STS_NO']}',\
# {qc_tos_tasks_queryresult['TASK_ID']},\
# {qc_tos_tasks_queryresult['VBT_ID']},\
# '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
# '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
# '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
# '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
# '{qc_tos_tasks_queryresult['UNLOCK_TIME']}',\
# '{DATA_FROM}',\
# '{DATA_FROM_TYPE}',\
# '{NOTES}'\
# )'''
#                 # print(insertsql)
#                 o.executesql(insertsql)
#             if i==3:#历史表中存在删除时间的记录
#                 querysqlfordeletes = o.query(f"select * from qc_tos_task_his where TRIGGER_ACTION='DELETE' and TASK_ID={qc_tos_tasks_queryresult['TASK_ID']} order by TRIG_CREATED desc")#查询创建时间
#                 if len(querysqlfordeletes)!=0:#能查询出来数据，将第一条数据对应的时间插入到KPI
#                     for querysqlfordelete in querysqlfordeletes:#遍历历史表中delete过的任务
#                         DATA_FROM = 'QCMSDB.QC_TOS_TASK_HIS.TRIGGER_ACTION(DELETE).TRIG_CREATED'  # 需要改的部分,历史表中删除过的任务信息
#                         DATA_FROM_TYPE = 'QCMS'
#                         NOTES = 'QCMS删除任务信息时间'  # 需要改的部分
#                         insertsql = f'''insert into {tablename_for_kpi}(\
#                                         STS_NO,\
#                                         TASK_ID,\
#                                         VBT_ID,\
#                                         TASK_TYPE,\
#                                         TASK_STATUS,\
#                                         ORIG_WSLOC,\
#                                         DEST_WS_LOC,\
#                                         KEYTIME,\
#                                         DATA_FROM,\
#                                         DATA_FROM_TYPE,\
#                                         NOTES) VALUES (
#                         '{querysqlfordelete['STS_NO']}',\
#                         {querysqlfordelete['TASK_ID']},\
#                         {querysqlfordelete['VBT_ID']},\
#                         '{querysqlfordelete['TASK_TYPE']}',\
#                         '{querysqlfordelete['TASK_STATUS']}',\
#                         '{querysqlfordelete['ORIG_WSLOC']}',\
#                         '{querysqlfordelete['DEST_WS_LOC']}',\
#                         '{querysqlfordelete['TRIG_CREATED']}',\
#                         '{DATA_FROM}',\
#                         '{DATA_FROM_TYPE}',\
#                         '{NOTES}'\
#                         )'''
#                         # print(insertsql)
#                         o.executesql(insertsql)
#             if i==4:#处理QC_GANTRY_INSTRUCTION发过的指令
#                 querysqlforgantry= f'''select * from qc_trolley_task where TASK_REF_ID={qc_tos_tasks_queryresult['TASK_ID']} order by START_TIME desc'''#先从qc_trolley_task查询出来对应的TASK_ID
#                 #查询QC_trolley_task中的QC_trolley_task.TASK_ID=QC_GANTRY_INSTRUCTION.TASK_ID
#                 qctrolleytaskqueryresults = o.query(querysqlforgantry)
#                 if len(qctrolleytaskqueryresults)!=0:#查询出来不为空才可以进行下一步
#                     QC_trolley_task_TASK_ID=qctrolleytaskqueryresults[0]['TASK_ID']#查询出来第一个结果的字段
#
#                     #开始查找QC_GANTRY_INSTRUCTION中有无涉及到大车移动的数据
#                     qcGantryInstructionquerysql = f"""select * from QC_GANTRY_INSTRUCTION where TASK_ID='{QC_trolley_task_TASK_ID}' order by START_TIME asc"""
#                     qcGantryInstructionqueryresults = o.query(qcGantryInstructionquerysql)
#                     if len(qcGantryInstructionqueryresults)!=0:#查出来数据不为空，能在大车指令中查到数据，将数据记录到数据表中
#                         for qcGantryInstructionqueryresult in qcGantryInstructionqueryresults:#遍历数据,因为需要记录开始时间和结束时间，因次for2次
#                             for index_gantry_single in range(2):#
#                                 if index_gantry_single==0:#插入指令START_TIME时间
#                                     DATA_FROM='QCMSDB.QC_GANTRY_INSTRUCTION.START_TIME'#需要改的部分
#                                     DATA_FROM_TYPE='QCMS'
#                                     NOTES= f"""大车指令开始时间，指令状态{qcGantryInstructionqueryresult['INSTR_STATE']}"""#需要改的部分
#                                     insertsql = f'''insert into {tablename_for_kpi}(\
#                                     STS_NO,\
#                                     TASK_ID,\
#                                     VBT_ID,\
#                                     TASK_TYPE,\
#                                     TASK_STATUS,\
#                                     ORIG_WSLOC,\
#                                     DEST_WS_LOC,\
#                                     KEYTIME,\
#                                     DATA_FROM,\
#                                     DATA_FROM_TYPE,\
#                                     NOTES) VALUES (
#                     '{qc_tos_tasks_queryresult['STS_NO']}',\
#                     {qc_tos_tasks_queryresult['TASK_ID']},\
#                     {qc_tos_tasks_queryresult['VBT_ID']},\
#                     '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                     '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                     '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                     '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                     '{qcGantryInstructionqueryresult['START_TIME']}',\
#                     '{DATA_FROM}',\
#                     '{DATA_FROM_TYPE}',\
#                     '{NOTES}'\
#                     )'''
#                                     # print(insertsql)
#                                     o.executesql(insertsql)
#                                 if index_gantry_single==1:#插入指令END_TIME时间
#                                     DATA_FROM='QCMSDB.QC_GANTRY_INSTRUCTION.END_TIME'#需要改的部分
#                                     DATA_FROM_TYPE='QCMS'
#                                     NOTES= f"""大车指令开始时间，指令状态{qcGantryInstructionqueryresult['INSTR_STATE']}"""#需要改的部分
#                                     insertsql = f'''insert into {tablename_for_kpi}(\
#                                     STS_NO,\
#                                     TASK_ID,\
#                                     VBT_ID,\
#                                     TASK_TYPE,\
#                                     TASK_STATUS,\
#                                     ORIG_WSLOC,\
#                                     DEST_WS_LOC,\
#                                     KEYTIME,\
#                                     DATA_FROM,\
#                                     DATA_FROM_TYPE,\
#                                     NOTES) VALUES (
#                     '{qc_tos_tasks_queryresult['STS_NO']}',\
#                     {qc_tos_tasks_queryresult['TASK_ID']},\
#                     {qc_tos_tasks_queryresult['VBT_ID']},\
#                     '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                     '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                     '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                     '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                     '{qcGantryInstructionqueryresult['END_TIME']}',\
#                     '{DATA_FROM}',\
#                     '{DATA_FROM_TYPE}',\
#                     '{NOTES}'\
#                     )'''
#                                     # print(insertsql)
#                                     o.executesql(insertsql)
#
#             if i==5:#处理QC_TROLLEY_INSTRUCTION的数据
#                 qcTrolleyInstructionquerysql = f"""select * from QC_TROLLEY_INSTRUCTION where TASK_REF_ID={qc_tos_tasks_queryresult['TASK_ID']} order by START_TIME asc"""
#                 qcTrolleyInstructionqueryresults = o.query(qcTrolleyInstructionquerysql)
#                 if len(qcTrolleyInstructionqueryresults)!=0:#查询出来数据不为空也就是能在QC_TROLLEY_INSTRUCTION查询出来数据
#                     #遍历查询出来的数据
#                     for qcTrolleyInstructionqueryresult in qcTrolleyInstructionqueryresults:
#                         #因为有开始时间和结束时间，因此需要遍历2遍
#                         for index_trolley_sing in range(2):#遍历2遍单独插入数据
#                             if index_trolley_sing == 0:  # 插入指令START_TIME时间
#                                 DATA_FROM=f"""QCMSDB.QC_TROLLEY_INSTRUCTION.{qcTrolleyInstructionqueryresult['INSTR_TYPE']}.START_TIME"""#需要改的部分
#                                 DATA_FROM_TYPE='QCMS'
#                                 NOTES= f"""小车指令开始时间，指令类型{qcTrolleyInstructionqueryresult['INSTR_TYPE']}，指令状态{qcTrolleyInstructionqueryresult['INSTR_STATE']}"""#需要改的部分
#                                 insertsql = f'''insert into {tablename_for_kpi}(\
#                                 STS_NO,\
#                                 TASK_ID,\
#                                 VBT_ID,\
#                                 TASK_TYPE,\
#                                 TASK_STATUS,\
#                                 ORIG_WSLOC,\
#                                 DEST_WS_LOC,\
#                                 KEYTIME,\
#                                 DATA_FROM,\
#                                 DATA_FROM_TYPE,\
#                                 NOTES) VALUES (
#                 '{qc_tos_tasks_queryresult['STS_NO']}',\
#                 {qc_tos_tasks_queryresult['TASK_ID']},\
#                 {qc_tos_tasks_queryresult['VBT_ID']},\
#                 '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                 '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                 '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                 '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                 '{qcTrolleyInstructionqueryresult['START_TIME']}',\
#                 '{DATA_FROM}',\
#                 '{DATA_FROM_TYPE}',\
#                 '{NOTES}'\
#                 )'''
#                                 # print(insertsql)
#                                 o.executesql(insertsql)
#                             if index_trolley_sing==1:#插入指令END_TIME时间
#                                 DATA_FROM=f"""QCMSDB.QC_TROLLEY_INSTRUCTION.{qcTrolleyInstructionqueryresult['INSTR_TYPE']}.END_TIME"""#需要改的部分
#                                 DATA_FROM_TYPE='QCMS'
#                                 NOTES= f"""小车指令结束时间，指令类型{qcTrolleyInstructionqueryresult['INSTR_TYPE']}，指令状态{qcTrolleyInstructionqueryresult['INSTR_STATE']}"""#需要改的部分
#                                 insertsql = f'''insert into {tablename_for_kpi}(\
#                                 STS_NO,\
#                                 TASK_ID,\
#                                 VBT_ID,\
#                                 TASK_TYPE,\
#                                 TASK_STATUS,\
#                                 ORIG_WSLOC,\
#                                 DEST_WS_LOC,\
#                                 KEYTIME,\
#                                 DATA_FROM,\
#                                 DATA_FROM_TYPE,\
#                                 NOTES) VALUES (
#                 '{qc_tos_tasks_queryresult['STS_NO']}',\
#                 {qc_tos_tasks_queryresult['TASK_ID']},\
#                 {qc_tos_tasks_queryresult['VBT_ID']},\
#                 '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                 '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                 '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                 '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                 '{qcTrolleyInstructionqueryresult['END_TIME']}',\
#                 '{DATA_FROM}',\
#                 '{DATA_FROM_TYPE}',\
#                 '{NOTES}'\
#                 )'''
#                                 # print(insertsql)
#                                 o.executesql(insertsql)
#
#             if i==6:#处理QC_CONTAINER_TRANSFER数据，QC_TROLLEY_TASK.TRANS_CHAIN_ID==QC_CONTAINER_TRANSFER.TRANS_CHAIN_ID
#                 #需要先从QC_TROLLEY_task中得到TRANS_CHAIN_ID，其中QC_TROLLEY_task.TASK_REF_ID=QC_TOS_TASK.TASK_ID，
#                 qcTrolleyTaskQuerySql=f"""select * from QC_TROLLEY_TASK where TASK_REF_ID={qc_tos_tasks_queryresult['TASK_ID']}"""
#                 qcTrolleyTaskQueryResults = o.query(qcTrolleyTaskQuerySql)
#                 if len(qcTrolleyTaskQueryResults)!=0:#能查到信息，那么得到QC_TROLLEY_TASK的TRANS_CHAIN_ID
#                     transChainId=qcTrolleyTaskQueryResults[0]['TRANS_CHAIN_ID']#拿查询出来的第一条数据得到TRANS_CHAIN_ID
#
#                     #再去查询QC_CONTAINER_TRANSFER数据
#                     qcContainerTransferQuerySql = f"""select * from QC_CONTAINER_TRANSFER where TRANS_CHAIN_ID='{transChainId}' order by CREATE_TIME asc"""
#                     qcContainerTransferQueryResults = o.query(qcContainerTransferQuerySql)
#                     if len(qcContainerTransferQueryResults)!=0:#查询出来的数据不为空，需要遍历数据
#                         for qcContainerTransferQueryResult in qcContainerTransferQueryResults:#因为每条数据只有创建时间，只需要插入创建时间就行
#                             DATA_FROM='QCMSDB.QC_CONTAINER_TRANSFER.CREATE_TIME'#需要改的部分
#                             DATA_FROM_TYPE='QCMS'
#
#                             accsRefIdL=qcContainerTransferQueryResult['ACCS_REF_ID_L']
#                             accsRefIdR=qcContainerTransferQueryResult['ACCS_REF_ID_R']
#                             if accsRefIdL!='':#查询出来不是空字符串
#                                 accsRefIdL = f"{qcContainerTransferQueryResult['ACCS_REF_ID_L']:.0f}"
#                             if accsRefIdR!='':#查询出来不是空字符串
#                                 accsRefIdR = f"{qcContainerTransferQueryResult['ACCS_REF_ID_R']:.0f}"
#
#                             # NOTES= f"""QC_CONTAINER_TRANSFER的指令类型{qcContainerTransferQueryResult['INSTR_TYPE']}，操作模式{qcContainerTransferQueryResult['OPERATE_MODE']},吊具尺寸{qcContainerTransferQueryResult['SPREADER_SIZE']},工作位置{qcContainerTransferQueryResult['WORK_LOCATION']},ACCS_REF_ID_L={accsRefIdL},ACCS_REF_ID_R={accsRefIdR}"""#需要改的部分
#
#                             NOTES= f"""QC_CONTAINER_TRANSFER的指令类型{qcContainerTransferQueryResult['INSTR_TYPE']}，操作模式{qcContainerTransferQueryResult['OPERATE_MODE']},吊具尺寸{qcContainerTransferQueryResult['SPREADER_SIZE']}"""#需要改的部分
#
#                             insertsql = f'''insert into {tablename_for_kpi}(\
#                             STS_NO,\
#                             TASK_ID,\
#                             VBT_ID,\
#                             TASK_TYPE,\
#                             TASK_STATUS,\
#                             ORIG_WSLOC,\
#                             DEST_WS_LOC,\
#                             KEYTIME,\
#                             DATA_FROM,\
#                             DATA_FROM_TYPE,\
#                             NOTES) VALUES (
#             '{qc_tos_tasks_queryresult['STS_NO']}',\
#             {qc_tos_tasks_queryresult['TASK_ID']},\
#             {qc_tos_tasks_queryresult['VBT_ID']},\
#             '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#             '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#             '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#             '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#             '{qcContainerTransferQueryResult['CREATE_TIME']}',\
#             '{DATA_FROM}',\
#             '{DATA_FROM_TYPE}',\
#             '{NOTES}'\
#             )'''
#                             # print(insertsql)
#                             o.executesql(insertsql)
#
#             if i==7:#处理QC_TP_INTERACTION_HIS数据
#                 #还是需要从QC_TOS_TASK_HIS去捞取交互状态
#                 qcTosTaskHisQuerySql = f"select * from qc_tos_task_his where TRIGGER_ACTION='INSERT' and TASK_ID={qc_tos_tasks_queryresult['TASK_ID']} order by TRIG_CREATED desc"
#                 qcTosTaskHisQueryResults = o.query(qcTosTaskHisQuerySql)#查询创建时间
#                 if len(qcTosTaskHisQueryResults)!=0:#能查询出来数据
#                     # print(qcTosTaskHisQueryResults)
#                     qcTosTaskCreateTime = qcTosTaskHisQueryResults[0]['TRIG_CREATED']
#
#                     #查询这条任务闭锁时间之前，到收到任务之后这段时间的交互
#                     qcTpInteractionHisQuerySql = f"""select * from QC_TP_INTERACTION_HIS where TRIG_CREATED>'{qcTosTaskCreateTime}' and TRIG_CREATED<'{qc_tos_tasks_queryresult['RESPONSE_TIME']}' and QC_ID={qc_tos_tasks_queryresult['STS_NO']} order by TRIG_CREATED asc"""
#                     qcTpInteractionHisQueryResults =o.query(qcTpInteractionHisQuerySql)
#                     if len(qcTpInteractionHisQueryResults)!=0:#如果查询到交互表数据不为空
#                         isWriteInit = 0  # 是否写入过INIT的标记，写入更新成1，没有写入是0
#                         isWriteArrived= 0  # 是否写入过Arrived的标记，写入更新成1，没有写入是0
#                         isWriteLocked = 0  # 是否写入过LOCKED的标记，写入更新成1，没有写入是0
#                         isWriteCancel = 0  # 是否写入过CANCEL的标记，写入更新成1，没有写入是0
#
#
#                         isWriteReqLock = 0  # 是否写入过REQ_LOCK的标记，写入更新成1，没有写入是0
#                         isWriteComplete = 0  # 是否写入过CANCEL的标记，写入更新成1，没有写入是0
#                         isWriteRelease = 0  # 是否写入过RELEASE的标记，写入更新成1，没有写入是0
#
#
#
#                         for qcTpInteractionHisQueryResult in qcTpInteractionHisQueryResults:#遍历交互表，记录第一次出现REQ_LOCK和LOCKED的时间
#                             #写入前，先判断之前有无写过REQ_LOCK和LOCKED时间
#
#                             if qcTpInteractionHisQueryResult['QC_STATUS']=='REQ_LOCK':
#                                 if isWriteReqLock==0:#没有写入过数据才记录
#                                     DATA_FROM='QCMSDB.QC_TP_INTERACTION_HIS.REQ_LOCK.TRIG_CREATED'#需要改的部分
#                                     DATA_FROM_TYPE='QCMS'
#                                     NOTES= f"""QCMS申请锁车REQ_LOCK时间"""#需要改的部分
#
#
#
#
#                                     insertsql = f'''insert into {tablename_for_kpi}(\
#                                     STS_NO,\
#                                     TASK_ID,\
#                                     VBT_ID,\
#                                     TASK_TYPE,\
#                                     TASK_STATUS,\
#                                     ORIG_WSLOC,\
#                                     DEST_WS_LOC,\
#                                     KEYTIME,\
#                                     DATA_FROM,\
#                                     DATA_FROM_TYPE,\
#                                     NOTES) VALUES (
#                     '{qc_tos_tasks_queryresult['STS_NO']}',\
#                     {qc_tos_tasks_queryresult['TASK_ID']},\
#                     {qc_tos_tasks_queryresult['VBT_ID']},\
#                     '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                     '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                     '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                     '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                     '{qcTpInteractionHisQueryResult['TRIG_CREATED']}',\
#                     '{DATA_FROM}',\
#                     '{DATA_FROM_TYPE}',\
#                     '{NOTES}'\
#                     )'''
#                                     # print(insertsql)
#                                     o.executesql(insertsql)
#                                     isWriteReqLock = 1#更新写入标志，表示已经写入过了
#
#                             if qcTpInteractionHisQueryResult['QC_STATUS'] == 'COMPLETE':
#                                 if isWriteComplete == 0:  # 没有写入过数据才记录
#                                     DATA_FROM = 'QCMSDB.QC_TP_INTERACTION_HIS.COMPLETE.TRIG_CREATED'  # 需要改的部分
#                                     DATA_FROM_TYPE = 'QCMS'
#                                     NOTES = f"""QCMS完成交互COMPLETE时间"""  # 需要改的部分
#                                     insertsql = f'''insert into {tablename_for_kpi}(\
#                                     STS_NO,\
#                                     TASK_ID,\
#                                     VBT_ID,\
#                                     TASK_TYPE,\
#                                     TASK_STATUS,\
#                                     ORIG_WSLOC,\
#                                     DEST_WS_LOC,\
#                                     KEYTIME,\
#                                     DATA_FROM,\
#                                     DATA_FROM_TYPE,\
#                                     NOTES) VALUES (
#                     '{qc_tos_tasks_queryresult['STS_NO']}',\
#                     {qc_tos_tasks_queryresult['TASK_ID']},\
#                     {qc_tos_tasks_queryresult['VBT_ID']},\
#                     '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                     '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                     '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                     '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                     '{qcTpInteractionHisQueryResult['TRIG_CREATED']}',\
#                     '{DATA_FROM}',\
#                     '{DATA_FROM_TYPE}',\
#                     '{NOTES}'\
#                     )'''
#                                     # print(insertsql)
#                                     o.executesql(insertsql)
#                                     isWriteComplete = 1  # 更新写入标志，表示已经写入过了
#
#                             if qcTpInteractionHisQueryResult['QC_STATUS'] == 'RELEASE':
#                                 if isWriteRelease == 0:  # 没有写入过数据才记录
#                                     DATA_FROM = 'QCMSDB.QC_TP_INTERACTION_HIS.RELEASE.TRIG_CREATED'  # 需要改的部分
#                                     DATA_FROM_TYPE = 'QCMS'
#                                     NOTES = f"""QCMS释放交互RELEASE时间"""  # 需要改的部分
#                                     insertsql = f'''insert into {tablename_for_kpi}(\
#                                         STS_NO,\
#                                         TASK_ID,\
#                                         VBT_ID,\
#                                         TASK_TYPE,\
#                                         TASK_STATUS,\
#                                         ORIG_WSLOC,\
#                                         DEST_WS_LOC,\
#                                         KEYTIME,\
#                                         DATA_FROM,\
#                                         DATA_FROM_TYPE,\
#                                         NOTES) VALUES (
#                         '{qc_tos_tasks_queryresult['STS_NO']}',\
#                         {qc_tos_tasks_queryresult['TASK_ID']},\
#                         {qc_tos_tasks_queryresult['VBT_ID']},\
#                         '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                         '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                         '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                         '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                         '{qcTpInteractionHisQueryResult['TRIG_CREATED']}',\
#                         '{DATA_FROM}',\
#                         '{DATA_FROM_TYPE}',\
#                         '{NOTES}'\
#                         )'''
#                                     # print(insertsql)
#                                     o.executesql(insertsql)
#                                     isWriteRelease = 1  # 更新写入标志，表示已经写入过了
#
#                             if qcTpInteractionHisQueryResult['FMS_AHT_STATUS']=='LOCKED':
#                                 if isWriteLocked==0:#没有写入过数据才记录
#                                     #还需要判断LOCKED的时间节点是在REQ_LOCKED时间之后才可以
#
#
#                                     #查询当前任务对应的REQ_LOCK的时间是多少
#                                     reqlocktimequerysql = f"""select * from {tablename_for_kpi} where TASK_ID={qc_tos_tasks_queryresult['TASK_ID']} and DATA_FROM='QCMSDB.QC_TP_INTERACTION_HIS.REQ_LOCK.TRIG_CREATED' order by KEYTIME desc"""
#
#                                     reqlocktimequeryResults= o.query(reqlocktimequerysql)
#                                     #判断查询出来不为空
#                                     if len(reqlocktimequeryResults)!=0:
#                                         reqlocktime=strChangeTime(reqlocktimequeryResults[0]['KEYTIME'])#将str转化为datetime类型
#
#                                         locktime=strChangeTime(qcTpInteractionHisQueryResult['TRIG_CREATED'])#查询出来锁车时间
#                                         if locktime>reqlocktime:#当锁车时间比申请锁车时间新的时候才记录locked时间，否则记录的可能是上一条任务的锁车时间
#
#                                             DATA_FROM='QCMSDB.QC_TP_INTERACTION_HIS.LOCKED.TRIG_CREATED'#需要改的部分
#                                             DATA_FROM_TYPE='QCMS'
#                                             NOTES= f"""FMS给的锁车LOCKED时间"""#需要改的部分
#                                             insertsql = f'''insert into {tablename_for_kpi}(\
#                                             STS_NO,\
#                                             TASK_ID,\
#                                             VBT_ID,\
#                                             TASK_TYPE,\
#                                             TASK_STATUS,\
#                                             ORIG_WSLOC,\
#                                             DEST_WS_LOC,\
#                                             KEYTIME,\
#                                             DATA_FROM,\
#                                             DATA_FROM_TYPE,\
#                                             NOTES) VALUES (
#                             '{qc_tos_tasks_queryresult['STS_NO']}',\
#                             {qc_tos_tasks_queryresult['TASK_ID']},\
#                             {qc_tos_tasks_queryresult['VBT_ID']},\
#                             '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                             '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                             '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                             '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                             '{qcTpInteractionHisQueryResult['TRIG_CREATED']}',\
#                             '{DATA_FROM}',\
#                             '{DATA_FROM_TYPE}',\
#                             '{NOTES}'\
#                             )'''
#                                             # print(insertsql)
#                                             o.executesql(insertsql)
#                                             isWriteLocked = 1#更新写入标志，表示已经写入过了
#
#                             if qcTpInteractionHisQueryResult['FMS_AHT_STATUS']=='INIT':
#                                 if isWriteInit==0:#没有写入过数据才记录
#                                     DATA_FROM='QCMSDB.QC_TP_INTERACTION_HIS.INIT.TRIG_CREATED'#需要改的部分
#                                     DATA_FROM_TYPE='QCMS'
#                                     NOTES= f"""FMS粗停INIT到位时间"""#需要改的部分
#                                     insertsql = f'''insert into {tablename_for_kpi}(\
#                                     STS_NO,\
#                                     TASK_ID,\
#                                     VBT_ID,\
#                                     TASK_TYPE,\
#                                     TASK_STATUS,\
#                                     ORIG_WSLOC,\
#                                     DEST_WS_LOC,\
#                                     KEYTIME,\
#                                     DATA_FROM,\
#                                     DATA_FROM_TYPE,\
#                                     NOTES) VALUES (
#                     '{qc_tos_tasks_queryresult['STS_NO']}',\
#                     {qc_tos_tasks_queryresult['TASK_ID']},\
#                     {qc_tos_tasks_queryresult['VBT_ID']},\
#                     '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                     '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                     '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                     '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                     '{qcTpInteractionHisQueryResult['TRIG_CREATED']}',\
#                     '{DATA_FROM}',\
#                     '{DATA_FROM_TYPE}',\
#                     '{NOTES}'\
#                     )'''
#                                     # print(insertsql)
#                                     o.executesql(insertsql)
#                                     isWriteInit = 1#更新写入标志，表示已经写入过了
#                             if qcTpInteractionHisQueryResult['FMS_AHT_STATUS']=='ARRIVED':
#                                 if isWriteArrived==0:#没有写入过数据才记录
#                                     DATA_FROM='QCMSDB.QC_TP_INTERACTION_HIS.ARRIVED.TRIG_CREATED'#需要改的部分
#                                     DATA_FROM_TYPE='QCMS'
#                                     NOTES= f"""FMS精停ARRIVED到位时间"""#需要改的部分
#                                     insertsql = f'''insert into {tablename_for_kpi}(\
#                                     STS_NO,\
#                                     TASK_ID,\
#                                     VBT_ID,\
#                                     TASK_TYPE,\
#                                     TASK_STATUS,\
#                                     ORIG_WSLOC,\
#                                     DEST_WS_LOC,\
#                                     KEYTIME,\
#                                     DATA_FROM,\
#                                     DATA_FROM_TYPE,\
#                                     NOTES) VALUES (
#                     '{qc_tos_tasks_queryresult['STS_NO']}',\
#                     {qc_tos_tasks_queryresult['TASK_ID']},\
#                     {qc_tos_tasks_queryresult['VBT_ID']},\
#                     '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                     '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                     '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                     '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                     '{qcTpInteractionHisQueryResult['TRIG_CREATED']}',\
#                     '{DATA_FROM}',\
#                     '{DATA_FROM_TYPE}',\
#                     '{NOTES}'\
#                     )'''
#                                     # print(insertsql)
#                                     o.executesql(insertsql)
#                                     isWriteArrived = 1#更新写入标志，表示已经写入过了
#                             if qcTpInteractionHisQueryResult['FMS_AHT_STATUS']=='CANCEL':
#                                 if isWriteCancel==0:#没有写入过数据才记录
#                                     DATA_FROM='QCMSDB.QC_TP_INTERACTION_HIS.CANCEL.TRIG_CREATED'#需要改的部分
#                                     DATA_FROM_TYPE='QCMS'
#                                     NOTES= f"""FMS取消交互CANCEL时间"""#需要改的部分
#                                     insertsql = f'''insert into {tablename_for_kpi}(\
#                                     STS_NO,\
#                                     TASK_ID,\
#                                     VBT_ID,\
#                                     TASK_TYPE,\
#                                     TASK_STATUS,\
#                                     ORIG_WSLOC,\
#                                     DEST_WS_LOC,\
#                                     KEYTIME,\
#                                     DATA_FROM,\
#                                     DATA_FROM_TYPE,\
#                                     NOTES) VALUES (
#                     '{qc_tos_tasks_queryresult['STS_NO']}',\
#                     {qc_tos_tasks_queryresult['TASK_ID']},\
#                     {qc_tos_tasks_queryresult['VBT_ID']},\
#                     '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                     '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                     '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                     '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                     '{qcTpInteractionHisQueryResult['TRIG_CREATED']}',\
#                     '{DATA_FROM}',\
#                     '{DATA_FROM_TYPE}',\
#                     '{NOTES}'\
#                     )'''
#                                     # print(insertsql)
#                                     o.executesql(insertsql)
#                                     isWriteCancel = 1#更新写入标志，表示已经写入过了
#
#             if i==8:#处理ACCS的'kpi_mt_step_log'的数据
#                 kpiMtStepLogQuerySql = f"select * from kpi_mt_step_log where task_id_low={qc_tos_tasks_queryresult['TASK_ID']} or task_id_high={qc_tos_tasks_queryresult['TASK_ID']} order by start_time asc"
#                 kpiMtStepLogQueryResults = o.query(kpiMtStepLogQuerySql)
#                 #查询出来不为空才进行下一步
#                 if len(kpiMtStepLogQueryResults)!=0:
#                     #遍历每条数据，并且每条数据都有start_time和end_time,应该进行for插入{tablename_for_kpi}
#                     for kpiMtStepLogQueryResult in kpiMtStepLogQueryResults:
#                         for i in range(2):#分别对start_time和end_time插入{tablename_for_kpi}处理
#                             if i ==0:#将这条任务对应的start_time记录到数据库中
#                                 DATA_FROM=f"""KPIDB.kpi_mt_step_log.{kpiMtStepLogQueryResult['step_id']}.start_time"""#需要改的部分
#                                 DATA_FROM_TYPE='KPI'
#                                 NOTES= f"""KPI记录的step={kpiMtStepLogQueryResult['step_id']}{stepTransToLanguage(kpiMtStepLogQueryResult['step_id'])} 对应的start_time"""#需要改的部分
#                                 insertsql = f'''insert into {tablename_for_kpi}(\
#                                 STS_NO,\
#                                 TASK_ID,\
#                                 VBT_ID,\
#                                 TASK_TYPE,\
#                                 TASK_STATUS,\
#                                 ORIG_WSLOC,\
#                                 DEST_WS_LOC,\
#                                 KEYTIME,\
#                                 DATA_FROM,\
#                                 DATA_FROM_TYPE,\
#                                 NOTES) VALUES (
#                 '{qc_tos_tasks_queryresult['STS_NO']}',\
#                 {qc_tos_tasks_queryresult['TASK_ID']},\
#                 {qc_tos_tasks_queryresult['VBT_ID']},\
#                 '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                 '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                 '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                 '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                 '{convertUtc_5(strChangeTime(kpiMtStepLogQueryResult['start_time']))}',\
#                 '{DATA_FROM}',\
#                 '{DATA_FROM_TYPE}',\
#                 '{NOTES}'\
#                 )'''
#                                 # print(insertsql)
#                                 o.executesql(insertsql)
#                             if i ==1:#将这条任务对应的end_time记录到数据库中
#                                 DATA_FROM=f"""KPIDB.kpi_mt_step_log.{kpiMtStepLogQueryResult['step_id']}.end_time"""#需要改的部分
#                                 DATA_FROM_TYPE='KPI'
#                                 NOTES= f"""KPI记录的step={kpiMtStepLogQueryResult['step_id']}{stepTransToLanguage(kpiMtStepLogQueryResult['step_id'])} 对应的end_time"""#需要改的部分
#                                 insertsql = f'''insert into {tablename_for_kpi}(\
#                                 STS_NO,\
#                                 TASK_ID,\
#                                 VBT_ID,\
#                                 TASK_TYPE,\
#                                 TASK_STATUS,\
#                                 ORIG_WSLOC,\
#                                 DEST_WS_LOC,\
#                                 KEYTIME,\
#                                 DATA_FROM,\
#                                 DATA_FROM_TYPE,\
#                                 NOTES) VALUES (
#                 '{qc_tos_tasks_queryresult['STS_NO']}',\
#                 {qc_tos_tasks_queryresult['TASK_ID']},\
#                 {qc_tos_tasks_queryresult['VBT_ID']},\
#                 '{qc_tos_tasks_queryresult['TASK_TYPE']}',\
#                 '{qc_tos_tasks_queryresult['TASK_STATUS']}',\
#                 '{qc_tos_tasks_queryresult['ORIG_WSLOC']}',\
#                 '{qc_tos_tasks_queryresult['DEST_WS_LOC']}',\
#                 '{convertUtc_5(strChangeTime(kpiMtStepLogQueryResult['end_time']))}',\
#                 '{DATA_FROM}',\
#                 '{DATA_FROM_TYPE}',\
#                 '{NOTES}'\
#                 )'''
#                                 # print(insertsql)
#                                 o.executesql(insertsql)
#
#
#
#
#
#
#
#
#
#
#
