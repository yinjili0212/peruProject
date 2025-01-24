import sqliteHandle
import datetime
from basicFunctionDefine import *
import time
import yaml
import json

envlist = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
#########
'根据船舶靠泊的VBT_ID查询这个船舶作业的开始时间和结束时间，以及在期间内QCMS拉取过的任务，以及每个任务中指令的情况' \
'1.处理任务创建时间，使用的是qc_tos_task_his中TRIGGER_ACTION="INSERT"的数据作为任务开始时间'
'2.处理任务闭锁时间，使用的是qc_tos_task中LOCK_TIME和UNLOCK_TIME字段的数据作为任务开始时间'
'3.处理任务完成时间（删除时间，因为QCMS拉取TOS任务时可能存在重复的任务），使用的是qc_tos_task_his中TRIGGER_ACTION="DELETE"的数据作为任务开始时间'
'4.处理大车运行的指令，使用的是qc_gantry_instruction中的数据'
'5.处理QC_TROLLEY_INSTRUCRION的指令'
'6.处理qc_container_transfer的指令'
'7.处理交互表qc_tp_interaction_his的数据'
'''8.处理ACCS的kpi_mt_step_log的数据'''
#########
#连接数据库
o=sqliteHandle.sqliteHandler(envlist['sqllitedbaddress'])

#对应表的名字
qc_tos_task=envlist['qc_tos_task_tablename']
qc_tos_task_his=envlist['qc_tos_task_his_tablename']
kpi_for_qcms=envlist['kpi_for_qcms_tablename']
qc_gantry_instruction=envlist['qc_gantry_instruction_tablename']
qc_trolley_instruction=envlist['qc_trolley_instruction_tablename']
qc_container_transfer=envlist['qc_container_transfer_tablename']
kpi_mt_step_log=envlist['kpi_mt_step_log_tablename']
kpi_mt_state_log=envlist['kpi_mt_state_log_tablename']
qc_tp_interaction_his = envlist['qc_tp_interaction_his_tablename']
qc_event_recorder_his = envlist['qc_event_recorder_his_tablename']
qc_event_define = envlist['qc_event_define_tablename']
MtWorkMode=envlist['mtworkmode_tablename']
MtInstructionStatus=envlist['mtinstructionstatus_tablename']
MhAboveSafeHeight=envlist['mhabovesafeheight_tablename']

##计算船舶作业的开始和结束时间，以qc_tos_task_his的RESPONSE_TIME字段开始和结束时间算
startTime=''
endTime=''
querySqlForQcTosTasks = f"""select * from {qc_tos_task_his}  where VBT_ID={envlist['VBT_ID']} and RESPONSE_TIME!='' order by RESPONSE_TIME asc"""
QcTosTaskQueryResults = o.query(querySqlForQcTosTasks)
if len(QcTosTaskQueryResults) >= 2:  # 如果检测到查询出来的数据大于=2条，才可
    startTime = QcTosTaskQueryResults[0]['RESPONSE_TIME']  # 这条船的当前岸桥的最小时间,str类型
    endTime = QcTosTaskQueryResults[-1]['RESPONSE_TIME']  # 这条船的当前岸桥的最大时间str类型
    print(f"船舶{envlist['VBT_ID']}作业开始时间：{startTime}；作业结束时间：{endTime}")


#计算出来给kpi_mt_step_log中的数据，因为此表计算的是UTC时间，需要将QCMS的时间+5个小时才可以作为kpi_mt_step_log使用
startTimeforKpi=timeChangeStr(strChangeTime(startTime)+timedelta(hours=5))
endTimeforKpi=timeChangeStr(strChangeTime(endTime)+timedelta(hours=5))


def handleQcTosTask():
    #######################处理qc_tos_task和qc_tos_task_his中的数据，将时间内的变化节点都打入数据中(有配对值)
    #查询qc_tos_task在规定的时间区间内到底有多少条任务
    # o.executesql(f"""delete from {kpi_for_qcms}""")
    print(f"开始处理{qc_tos_task}表数据")
    # qcTosTaskQuerySql = f'''select * from {qc_tos_task} where (LOCK_TIME>='{startTime}' and  LOCK_TIME<='{endTime}') or (UNLOCK_TIME>='{startTime}' and  UNLOCK_TIME<='{endTime}') order by TASK_ID asc'''
    qcTosTaskQuerySql = f'''select * from {qc_tos_task} where (LOCK_TIME>='{startTime}' and  LOCK_TIME<='{endTime}') or (UNLOCK_TIME>='{startTime}' and  UNLOCK_TIME<='{endTime}') order by TASK_ID asc'''
    qcTosTaskQueryResults = o.query(qcTosTaskQuerySql,t='df')
    if isinstance(qcTosTaskQueryResults,pd.DataFrame):#能查询出来任务
        for index,qcTosTaskQueryResult in qcTosTaskQueryResults.iterrows():#遍历每一条查询出来的数据

            ####配对值
            paired_value = snowFlakeId()
            #####qc_tos_task的LOCK_TIME处理记录###########
            #遍历任务中的LOCK_TIME和UNLOCK_TIME记录到数据库
            DATA_FROM='QCMSDB.qc_tos_task.LOCK_TIME'#需要改的部分
            DATA_FROM_TYPE='QCMS'
            NOTES= '吊具闭锁时间'#需要改的部分
            insertsql = f'''insert into {kpi_for_qcms}(\
            STS_NO,\
            TASK_ID,\
            VBT_ID,\
            TASK_TYPE,\
            TASK_STATUS,\
            ORIG_WSLOC,\
            DEST_WS_LOC,\
            KEYTIME,\
            DATA_FROM,\
            DATA_FROM_TYPE,\
            NOTES,\
            PAIRED_VALUE) VALUES (
    '{qcTosTaskQueryResult['STS_NO']}',\
    {qcTosTaskQueryResult['TASK_ID']},\
    {qcTosTaskQueryResult['VBT_ID']},\
    '{qcTosTaskQueryResult['TASK_TYPE']}',\
    '{qcTosTaskQueryResult['TASK_STATUS']}',\
    '{qcTosTaskQueryResult['ORIG_WSLOC']}',\
    '{qcTosTaskQueryResult['DEST_WS_LOC']}',\
    '{qcTosTaskQueryResult['LOCK_TIME']}',\
    '{DATA_FROM}',\
    '{DATA_FROM_TYPE}',\
    '{NOTES}',\
    {paired_value}
    )'''
            o.executesql(insertsql)
            #####qc_tos_task的LOCK_TIME处理记录###########

            #####qc_tos_task的UNLOCK_TIME处理记录###########
            #遍历任务中UNLOCK_TIME记录到数据库
            DATA_FROM='QCMSDB.qc_tos_task.UNLOCK_TIME'#需要改的部分
            DATA_FROM_TYPE='QCMS'
            NOTES= '吊具开锁时间'#需要改的部分
            insertsql = f'''insert into {kpi_for_qcms}(\
            STS_NO,\
            TASK_ID,\
            VBT_ID,\
            TASK_TYPE,\
            TASK_STATUS,\
            ORIG_WSLOC,\
            DEST_WS_LOC,\
            KEYTIME,\
            DATA_FROM,\
            DATA_FROM_TYPE,\
            NOTES,\
            PAIRED_VALUE) VALUES (
    '{qcTosTaskQueryResult['STS_NO']}',\
    {qcTosTaskQueryResult['TASK_ID']},\
    {qcTosTaskQueryResult['VBT_ID']},\
    '{qcTosTaskQueryResult['TASK_TYPE']}',\
    '{qcTosTaskQueryResult['TASK_STATUS']}',\
    '{qcTosTaskQueryResult['ORIG_WSLOC']}',\
    '{qcTosTaskQueryResult['DEST_WS_LOC']}',\
    '{qcTosTaskQueryResult['UNLOCK_TIME']}',\
    '{DATA_FROM}',\
    '{DATA_FROM_TYPE}',\
    '{NOTES}',\
    {paired_value}
    )'''
            o.executesql(insertsql)
            #####qc_tos_task的UNLOCK_TIME处理记录###########

            ###配对值
            paired_value = snowFlakeId()
            #####INSERT过的数据处理插入###########
            #同一条任务可能QCMS拉取过多次，因此，需要将qc_tos_task_his的数据也拉出来看看，到底INSERT过几次
            qcTosTaskHisQuerySql=f"""select * from {qc_tos_task_his} where TRIGGER_ACTION='INSERT' and TASK_ID={qcTosTaskQueryResult['TASK_ID']} order by TRIG_CREATED asc"""
            qcTosTaskHisQueryResults = o.query(qcTosTaskHisQuerySql,t='df')
            if isinstance(qcTosTaskHisQueryResults,pd.DataFrame):#查询出来数据不为空，将所有插入过的数据记录到对应的表中
                for qcTosTaskHisQueryResult in qcTosTaskHisQueryResults.iterrows():#遍历每一条插入过的数据
                    DATA_FROM = 'QCMSDB.qc_tos_task_his.TRIG_CREATED.INSERT'
                    DATA_FROM_TYPE='QCMS'
                    NOTES= '任务创建时间'
                    insertsql = f'''insert into {kpi_for_qcms}(\
                    STS_NO,\
                    TASK_ID,\
                    VBT_ID,\
                    TASK_TYPE,\
                    TASK_STATUS,\
                    ORIG_WSLOC,\
                    DEST_WS_LOC,\
                    KEYTIME,\
                    DATA_FROM,\
                    DATA_FROM_TYPE,\
                    NOTES,\
                    PAIRED_VALUE) VALUES (
    '{qcTosTaskHisQueryResult[1]['STS_NO']}',\
    {qcTosTaskHisQueryResult[1]['TASK_ID']},\
    {qcTosTaskHisQueryResult[1]['VBT_ID']},\
    '{qcTosTaskHisQueryResult[1]['TASK_TYPE']}',\
    '{qcTosTaskHisQueryResult[1]['TASK_STATUS']}',\
    '{qcTosTaskHisQueryResult[1]['ORIG_WSLOC']}',\
    '{qcTosTaskHisQueryResult[1]['DEST_WS_LOC']}',\
    '{qcTosTaskHisQueryResult[1]['TRIG_CREATED']}',\
    '{DATA_FROM}',\
    '{DATA_FROM_TYPE}',\
    '{NOTES}',\
    {paired_value}
    )'''
                    o.executesql(insertsql)
            #####INSERT过的数据处理插入###########

            #####DELETE过的数据处理插入###########
            #同一条任务可能QCMS拉取过多次，因此，QCMS可能删除过多次，需要将qc_tos_task_his的数据也拉出来看看，到底DELETE过几次
            qcTosTaskHisQuerySql=f"""select * from {qc_tos_task_his} where TRIGGER_ACTION='DELETE' and TASK_ID={qcTosTaskQueryResult['TASK_ID']} order by TRIG_CREATED asc"""
            qcTosTaskHisQueryResults = o.query(qcTosTaskHisQuerySql,t='df')
            if isinstance(qcTosTaskHisQueryResults,pd.DataFrame):#查询出来数据不为空，将所有插入过的数据记录到对应的表中
                for qcTosTaskHisQueryResult in qcTosTaskHisQueryResults.iterrows():#遍历每一条插入过的数据
                    DATA_FROM = 'QCMSDB.qc_tos_task_his.TRIGGER_ACTION(DELETE).TRIG_CREATED'
                    DATA_FROM_TYPE='QCMS'
                    NOTES= 'QCMS删除任务信息时间'
                    querydeletesql = f'''insert into {kpi_for_qcms}(\
                    STS_NO,\
                    TASK_ID,\
                    VBT_ID,\
                    TASK_TYPE,\
                    TASK_STATUS,\
                    ORIG_WSLOC,\
                    DEST_WS_LOC,\
                    KEYTIME,\
                    DATA_FROM,\
                    DATA_FROM_TYPE,\
                    NOTES,\
                    PAIRED_VALUE) VALUES (
    '{qcTosTaskHisQueryResult[1]['STS_NO']}',\
    {qcTosTaskHisQueryResult[1]['TASK_ID']},\
    {qcTosTaskHisQueryResult[1]['VBT_ID']},\
    '{qcTosTaskHisQueryResult[1]['TASK_TYPE']}',\
    '{qcTosTaskHisQueryResult[1]['TASK_STATUS']}',\
    '{qcTosTaskHisQueryResult[1]['ORIG_WSLOC']}',\
    '{qcTosTaskHisQueryResult[1]['DEST_WS_LOC']}',\
    '{qcTosTaskHisQueryResult[1]['TRIG_CREATED']}',\
    '{DATA_FROM}',\
    '{DATA_FROM_TYPE}',\
    '{NOTES}',\
    {paired_value}
    )'''
                    o.executesql(querydeletesql)
            #####DELETE过的数据处理插入###########


    #查询qc_tos_task_his表中TASK_ID没有在QC_TOA_TASK中出现的值
    querySqlForQcTosTaskHis = f"""select * from {qc_tos_task_his} where (TRIG_CREATED>='{startTime}' and TRIG_CREATED<='{endTime}') and TRIGGER_ACTION='INSERT' order by TRIG_CREATED asc"""
    qcTosTaskHisQueryResults = o.query(querySqlForQcTosTaskHis,t='df')
    if isinstance(qcTosTaskHisQueryResults,pd.DataFrame):#能查询出数据
        for qcTosTaskHisQueryResultindex,qcTosTaskHisQueryResult in qcTosTaskHisQueryResults.iterrows():

            #反向查询qc_tos_task有没有这个TASK_ID,如果没有才进行下一步，有时上面已经打印过了
            querySqlForQcTosTask=f'''select * from {qc_tos_task} where TASK_ID={qcTosTaskHisQueryResult['TASK_ID']}'''
            qcTosTaskQueryResults = o.query(querySqlForQcTosTask,t='df')
            if isinstance(qcTosTaskQueryResults,list):#在qc_tos_task没有查询到数据才可以进行下一步，将没查询到的TASK_ID且是INSERT插入数据库

                ##配对值
                paired_value = snowFlakeId()
                ###
                DATA_FROM = 'QCMSDB.qc_tos_task_his.TRIG_CREATED.INSERT'
                DATA_FROM_TYPE = 'QCMS'
                NOTES = '任务创建时间'
                querydeletesql = f'''insert into {kpi_for_qcms}(\
                                STS_NO,\
                                TASK_ID,\
                                VBT_ID,\
                                TASK_TYPE,\
                                TASK_STATUS,\
                                ORIG_WSLOC,\
                                DEST_WS_LOC,\
                                KEYTIME,\
                                DATA_FROM,\
                                DATA_FROM_TYPE,\
                                NOTES,\
                                PAIRED_VALUE) VALUES (
                '{qcTosTaskHisQueryResult['STS_NO']}',\
                {qcTosTaskHisQueryResult['TASK_ID']},\
                {qcTosTaskHisQueryResult['VBT_ID']},\
                '{qcTosTaskHisQueryResult['TASK_TYPE']}',\
                '{qcTosTaskHisQueryResult['TASK_STATUS']}',\
                '{qcTosTaskHisQueryResult['ORIG_WSLOC']}',\
                '{qcTosTaskHisQueryResult['DEST_WS_LOC']}',\
                '{qcTosTaskHisQueryResult['TRIG_CREATED']}',\
                '{DATA_FROM}',\
                '{DATA_FROM_TYPE}',\
                '{NOTES}',\
                {paired_value}
                )'''
                # print(querydeletesql)
                o.executesql(querydeletesql)
                #####

                querySqlForQcTosTaskHisdelete = f"""select * from {qc_tos_task_his} where TASK_ID={qcTosTaskHisQueryResult['TASK_ID']} and TRIGGER_ACTION='DELETE' and TRIG_CREATED>'{qcTosTaskHisQueryResult['TRIG_CREATED']}' order by TRIG_CREATED asc"""
                qcTosTaskHisDeleteQueryResults = o.query(querySqlForQcTosTaskHisdelete, t='df')
                if isinstance(qcTosTaskHisDeleteQueryResults,pd.DataFrame):
                    DATA_FROM = 'QCMSDB.qc_tos_task_his.TRIGGER_ACTION(DELETE).TRIG_CREATED'
                    DATA_FROM_TYPE='QCMS'
                    NOTES= 'QCMS删除任务信息时间'
                    querydeletesql = f'''insert into {kpi_for_qcms}(\
                    STS_NO,\
                    TASK_ID,\
                    VBT_ID,\
                    TASK_TYPE,\
                    TASK_STATUS,\
                    ORIG_WSLOC,\
                    DEST_WS_LOC,\
                    KEYTIME,\
                    DATA_FROM,\
                    DATA_FROM_TYPE,\
                    NOTES,\
                    PAIRED_VALUE) VALUES (
    '{qcTosTaskHisDeleteQueryResults.iloc[0]['STS_NO']}',\
    {qcTosTaskHisDeleteQueryResults.iloc[0]['TASK_ID']},\
    {qcTosTaskHisDeleteQueryResults.iloc[0]['VBT_ID']},\
    '{qcTosTaskHisDeleteQueryResults.iloc[0]['TASK_TYPE']}',\
    '{qcTosTaskHisDeleteQueryResults.iloc[0]['TASK_STATUS']}',\
    '{qcTosTaskHisDeleteQueryResults.iloc[0]['ORIG_WSLOC']}',\
    '{qcTosTaskHisDeleteQueryResults.iloc[0]['DEST_WS_LOC']}',\
    '{qcTosTaskHisDeleteQueryResults.iloc[0]['TRIG_CREATED']}',\
    '{DATA_FROM}',\
    '{DATA_FROM_TYPE}',\
    '{NOTES}',\
    {paired_value}
    )'''
                    # print(querydeletesql)
                    o.executesql(querydeletesql)





    #####INSERT过的数据处理插入###################################

def handleQcEventRecorderHis():
    ##############################################################################qc_event_recorder_his表的计算(有配对值)
    print(f"开始处理{qc_event_recorder_his}表数据")
    print(datetime.now())
    querySqlForQcEventDefine = f""f'select * from {qc_event_define}'
    qcEventDefineQueryResults=o.query(querySqlForQcEventDefine,t='df')
    if isinstance(qcEventDefineQueryResults,pd.DataFrame):
        #设置eventdefine初始
        dictForeventdefine={}
        for index,qcEventDefineQueryResult in qcEventDefineQueryResults.iterrows():
            dictForeventdefine[qcEventDefineQueryResult['CODE']]=json.loads(qcEventDefineQueryResult['REMARK'])['en-US']
        #得到eventdefine初始定义
        # print(dictForeventdefine)


        if dictForeventdefine!={}:
            querysqlforQcEventRecorderHis= f"""select * from {qc_event_recorder_his} where (CREATE_TIME>='{startTime}' and CREATE_TIME<='{endTime}') order by ID asc"""
            qcEventRecorderHisQueryResults = o.query(querysqlforQcEventRecorderHis,t='df')
            if isinstance(qcEventRecorderHisQueryResults,pd.DataFrame):#遍历大车移动过的数据

                for index,qcEventRecorderHisQueryResult in qcEventRecorderHisQueryResults.iterrows():
                    print(f"""{qcEventRecorderHisQueryResult['ID']}""")

                    DATA_FROM = f"""QCMSDB.{qc_event_recorder_his}.CREATE_TIME"""  # 需要改的部分{qcEventRecorderHisQueryResult['EVENT_CODE']}.
                    DATA_FROM_TYPE='QCMS'
                    NOTES= f"""QCMS记录的eventrecorderhis:{qcEventRecorderHisQueryResult['EVENT_CODE']}={dictForeventdefine.get(qcEventRecorderHisQueryResult['EVENT_CODE'])}"""#需要改的部分{dictForeventdefine[qcEventRecorderHisQueryResult['EVENT_CODE']]}
                    insertsql = f'''insert into {kpi_for_qcms}(\
                    STS_NO,\
                    KEYTIME,\
                    DATA_FROM,\
                    DATA_FROM_TYPE,\
                    EVENT_CODE,\
                    NOTES) VALUES (
                    '{qcEventRecorderHisQueryResult['QC_ID']}',\
                    '{qcEventRecorderHisQueryResult['CREATE_TIME']}',\
                    '{DATA_FROM}',\
                    '{DATA_FROM_TYPE}',\
                    {qcEventRecorderHisQueryResult['EVENT_CODE']},\
                    '{NOTES}'
                    )'''
                    # print(insertsql)
                    o.executesql(insertsql)
            ##############################################################################
    print(datetime.now())

def handleQcGantryInstruction():
    ##############################################################################qc_gantry_instruction表的计算(有配对值)
    print(f"开始处理{qc_gantry_instruction}表数据")
    querysqlforQcGantryInstruction= f"""select * from {qc_gantry_instruction} where (START_TIME>='{startTime}' and START_TIME<='{endTime}') or (END_TIME>='{startTime}' and END_TIME<='{endTime}') order by START_TIME asc"""
    qcGantryInstructionQueryResults = o.query(querysqlforQcGantryInstruction,t='df')
    if isinstance(qcGantryInstructionQueryResults,pd.DataFrame):#遍历大车移动过的数据
        for qcGantryInstructionQueryResult in qcGantryInstructionQueryResults.iterrows():

            #配对值
            paired_value = snowFlakeId()

            DATA_FROM = 'QCMSDB.qc_gantry_instruction.START_TIME'  # 需要改的部分
            DATA_FROM_TYPE='QCMS'
            NOTES= f"""大车指令开始时间，指令状态{qcGantryInstructionQueryResult[1]['INSTR_STATE']}"""#需要改的部分
            insertsql = f'''insert into {kpi_for_qcms}(\
            STS_NO,\
            TASK_REF_ID_FOR_GANTRY,\
            TASK_ID_FOR_GANTRY,\
            KEYTIME,\
            DATA_FROM,\
            DATA_FROM_TYPE,\
            NOTES,\
            PAIRED_VALUE) VALUES (
            '{qcGantryInstructionQueryResult[1]['QC_ID']}',\
            {qcGantryInstructionQueryResult[1]['TASK_REF_ID']},\
            '{qcGantryInstructionQueryResult[1]['TASK_ID']}',\
            '{qcGantryInstructionQueryResult[1]['START_TIME']}',\
            '{DATA_FROM}',\
            '{DATA_FROM_TYPE}',\
            '{NOTES}',\
            {paired_value}
            )'''
            # print(insertsql)
            o.executesql(insertsql)


            DATA_FROM = 'QCMSDB.qc_gantry_instruction.END_TIME'  # 需要改的部分
            DATA_FROM_TYPE='QCMS'
            NOTES= f"""大车指令结束时间，指令状态{qcGantryInstructionQueryResult[1]['INSTR_STATE']}"""#需要改的部分
            insertsql = f'''insert into {kpi_for_qcms}(\
            STS_NO,\
            TASK_REF_ID_FOR_GANTRY,\
            TASK_ID_FOR_GANTRY,\
            KEYTIME,\
            DATA_FROM,\
            DATA_FROM_TYPE,\
            NOTES,\
            PAIRED_VALUE) VALUES (
            '{qcGantryInstructionQueryResult[1]['QC_ID']}',\
            {qcGantryInstructionQueryResult[1]['TASK_REF_ID']},\
            '{qcGantryInstructionQueryResult[1]['TASK_ID']}',\
            '{qcGantryInstructionQueryResult[1]['END_TIME']}',\
            '{DATA_FROM}',\
            '{DATA_FROM_TYPE}',\
            '{NOTES}',\
            {paired_value}
            )'''
            # print(insertsql)
            o.executesql(insertsql)
    ##############################################################################
#
#
#
#
#

def handleQcTrolleyInstruction():
    ##############################################################################qc_trolley_instruction表的计算（有配对值）
    print(f"""开始处理{qc_trolley_instruction}表数据""")
    QcTrolleyInstruction= f"""select * from {qc_trolley_instruction} where (START_TIME>='{startTime}' and START_TIME<='{endTime}') or (END_TIME>='{startTime}' and END_TIME<='{endTime}') order by START_TIME asc"""
    QcTrolleyInstructionQueryResults = o.query(QcTrolleyInstruction,t='df')
    if isinstance(QcTrolleyInstructionQueryResults,pd.DataFrame):#遍历小车执行过的数据
        for QcTrolleyInstructionQueryResult in QcTrolleyInstructionQueryResults.iterrows():
            ##设置配对值
            paired_value = snowFlakeId()

            DATA_FROM =f'''QCMSDB.qc_trolley_instruction.{QcTrolleyInstructionQueryResult[1]['INSTR_TYPE']}.START_TIME'''  # 需要改的部分
            DATA_FROM_TYPE='QCMS'
            NOTES= f"""小车指令开始时间，指令类型{QcTrolleyInstructionQueryResult[1]['INSTR_TYPE']}，指令状态{QcTrolleyInstructionQueryResult[1]['INSTR_STATE']}"""#需要改的部分
            insertsql = f'''insert into {kpi_for_qcms}(\
            STS_NO,\
            INSTR_ID_FOR_MT_TROLLEY,\
            TASK_REF_ID_FOR_MT_TROLLEY,\
            TASK_ID_FOR_MT_TROLLEY,\
            KEYTIME,\
            DATA_FROM,\
            DATA_FROM_TYPE,\
            NOTES,\
            PAIRED_VALUE) VALUES (
            '{QcTrolleyInstructionQueryResult[1]['QC_ID']}',\
            '{QcTrolleyInstructionQueryResult[1]['INSTR_ID']}',\
            {QcTrolleyInstructionQueryResult[1]['TASK_REF_ID']},\
            '{QcTrolleyInstructionQueryResult[1]['TASK_ID']}',\
            '{QcTrolleyInstructionQueryResult[1]['START_TIME']}',\
            '{DATA_FROM}',\
            '{DATA_FROM_TYPE}',\
            '{NOTES}',\
            {paired_value}
            )'''
            # print(insertsql)
            o.executesql(insertsql)


            DATA_FROM =f'''QCMSDB.qc_trolley_instruction.{QcTrolleyInstructionQueryResult[1]['INSTR_TYPE']}.END_TIME'''  # 需要改的部分
            DATA_FROM_TYPE='QCMS'
            NOTES= f"""小车指令结束时间，指令类型{QcTrolleyInstructionQueryResult[1]['INSTR_TYPE']}，指令状态{QcTrolleyInstructionQueryResult[1]['INSTR_STATE']}"""#需要改的部分
            insertsql = f'''insert into {kpi_for_qcms}(\
            STS_NO,\
            INSTR_ID_FOR_MT_TROLLEY,\
            TASK_REF_ID_FOR_MT_TROLLEY,\
            TASK_ID_FOR_MT_TROLLEY,\
            KEYTIME,\
            DATA_FROM,\
            DATA_FROM_TYPE,\
            NOTES,\
            PAIRED_VALUE) VALUES (
            '{QcTrolleyInstructionQueryResult[1]['QC_ID']}',\
            '{QcTrolleyInstructionQueryResult[1]['INSTR_ID']}',\
            {QcTrolleyInstructionQueryResult[1]['TASK_REF_ID']},\
            '{QcTrolleyInstructionQueryResult[1]['TASK_ID']}',\
            '{QcTrolleyInstructionQueryResult[1]['END_TIME']}',\
            '{DATA_FROM}',\
            '{DATA_FROM_TYPE}',\
            '{NOTES}',\
            {paired_value}
            )'''
            # print(insertsql)
            o.executesql(insertsql)
    ##############################################################################



def handleQcContainerTransfer():
    ##############################################################################qc_container_transfer表的计算(无配对值)
    print(f"""开始处理{qc_container_transfer}表数据""")
    ###第1步骤：先将qc_container_transfer表的数据根据条件导入目标表kpi_for_qcms中
    QcContainerTransferQuerySql= f"""select * from {qc_container_transfer} where (CREATE_TIME>='{startTime}' and CREATE_TIME<='{endTime}') and GANTRY_POSITION!=0 and TROLLEY_POSITION!=0 and HOIST_POSITION!=0 order by CREATE_TIME asc"""
    QcContainerTransferQueryResults = o.query(QcContainerTransferQuerySql,t='df')
    if isinstance(QcContainerTransferQueryResults,pd.DataFrame):#遍历
        for QcContainerTransferQueryResult in QcContainerTransferQueryResults.iterrows():
            DATA_FROM = f'''QCMSDB.qc_container_transfer.{QcContainerTransferQueryResult[1]['INSTR_TYPE']}.CREATE_TIME'''  # 需要改的部分
            DATA_FROM_TYPE='QCMS'
            NOTES= f"""抓放箱记录指令类型{QcContainerTransferQueryResult[1]['INSTR_TYPE']}，吊具尺寸{QcContainerTransferQueryResult[1]['SPREADER_SIZE']}"""#需要改的部分
            insertsql = f'''insert into {kpi_for_qcms}(\
            STS_NO,\
            TRANS_CHAIN_ID,\
            OPERATE_MODE_FOR_CTNTRANS,\
            SPREADER_SIZE_FOR_CTNTRANS,\
            WORK_LOCATION_FOR_CTNTRANS,\
            KEYTIME,\
            DATA_FROM,\
            DATA_FROM_TYPE,\
            NOTES) VALUES (
            '{QcContainerTransferQueryResult[1]['QC_ID']}',\
            '{QcContainerTransferQueryResult[1]['TRANS_CHAIN_ID']}',\
            '{QcContainerTransferQueryResult[1]['OPERATE_MODE']}',\
            '{QcContainerTransferQueryResult[1]['SPREADER_SIZE']}',\
            '{QcContainerTransferQueryResult[1]['WORK_LOCATION']}',\
            '{QcContainerTransferQueryResult[1]['CREATE_TIME']}',\
            '{DATA_FROM}',\
            '{DATA_FROM_TYPE}',\
            '{NOTES}'\
            )'''
            # print(insertsql)
            o.executesql(insertsql)
     ###先将qc_container_transfer表的数据根据条件导入目标表kpi_for_qcms中

     #####第2步：将目标表中相同的TRANS_CHAIN_ID字段中的配对值字段PAIRED_VALUE更新成相同的值
    querySqlForTableForKpi = f"""select distinct TRANS_CHAIN_ID from {kpi_for_qcms} where (KEYTIME>='{startTime}' and KEYTIME<='{endTime}') order by KEYTIME asc"""
    tableForKpiQueryResults = o.query(querySqlForTableForKpi,t='df')
    if isinstance(tableForKpiQueryResults,pd.DataFrame):
        for indexTableForKpi,tableForKpiQueryResult in tableForKpiQueryResults.iterrows():#遍历每行数据
            #插入数据前生成唯一的值用来放到数据库配对
            paired_value = snowFlakeId()

            #更新数据库
            updateSql = f"""update {kpi_for_qcms} set PAIRED_VALUE={paired_value} where TRANS_CHAIN_ID='{tableForKpiQueryResult['TRANS_CHAIN_ID']}'"""
            o.executesql(updateSql)
    ##############################################################################



def handleKpiMtStepLog():
    ################################################################################单机 kpi_mt_step_log（有配对值）
    print(f"""开始处理{kpi_mt_step_log}表数据""")
    kpiMtStepLogQuerySql = f"select * from kpi_mt_step_log where (start_time>='{startTimeforKpi}' and start_time<='{endTimeforKpi}') and (end_time>='{startTimeforKpi}' and end_time<='{endTimeforKpi}') order by start_time asc"
    kpiMtStepLogQueryResults = o.query(kpiMtStepLogQuerySql,t='df')
    # 查询出来不为空才进行下一步
    if isinstance(kpiMtStepLogQueryResults,pd.DataFrame):
        # 遍历每条数据，并且每条数据都有start_time和end_time,应该进行for插入{kpi_for_qcms}
        for kpiMtStepLogQueryResult in kpiMtStepLogQueryResults.iterrows():

            ##插入数据前生成唯一的值用来放到数据库配对
            paired_value = snowFlakeId()

            DATA_FROM = f"""KPIDB.kpi_mt_step_log.{kpiMtStepLogQueryResult[1]['step_id']}.start_time"""  # 需要改的部分
            DATA_FROM_TYPE = 'KPI'
            NOTES = f"""KPI记录的step={kpiMtStepLogQueryResult[1]['step_id']}{stepTransToLanguage(kpiMtStepLogQueryResult[1]['step_id'])} 对应的start_time"""  # 需要改的部分
            insertsql = f'''insert into {kpi_for_qcms}(\
            STS_NO,\
            TASK_ID_FOR_KPI_MT,\
            KEYTIME,\
            DATA_FROM,\
            DATA_FROM_TYPE,\
            NOTES,\
            PAIRED_VALUE) VALUES (
        '{kpiMtStepLogQueryResult[1]['crane_id']}',\
        {kpiMtStepLogQueryResult[1]['task_id_low']},\
        '{convertUtc_5(strChangeTime(kpiMtStepLogQueryResult[1]['start_time']))}',\
        '{DATA_FROM}',\
        '{DATA_FROM_TYPE}',\
        '{NOTES}',\
        {paired_value}
        )'''
            print(insertsql)
            o.executesql(insertsql)


            DATA_FROM = f"""KPIDB.kpi_mt_step_log.{kpiMtStepLogQueryResult[1]['step_id']}.end_time"""  # 需要改的部分
            DATA_FROM_TYPE = 'KPI'
            NOTES = f"""KPI记录的step={kpiMtStepLogQueryResult[1]['step_id']}{stepTransToLanguage(kpiMtStepLogQueryResult[1]['step_id'])} 对应的end_time"""  # 需要改的部分
            insertsql = f'''insert into {kpi_for_qcms}(\
            STS_NO,\
            TASK_ID_FOR_KPI_MT,\
            KEYTIME,\
            DATA_FROM,\
            DATA_FROM_TYPE,\
            NOTES,\
            PAIRED_VALUE) VALUES (
        '{kpiMtStepLogQueryResult[1]['crane_id']}',\
        {kpiMtStepLogQueryResult[1]['task_id_low']},\
        '{convertUtc_5(strChangeTime(kpiMtStepLogQueryResult[1]['end_time']))}',\
        '{DATA_FROM}',\
        '{DATA_FROM_TYPE}',\
        '{NOTES}',\
        {paired_value}
        )'''
            print(insertsql)
            o.executesql(insertsql)
    ################################################################################kpi_mt_step_log


def handleKpiMtStateLog():#处理KPI
    ################################################################################单机 kpi_mt_state_log（有配对值）
    print(f"""开始处理{kpi_mt_state_log}表数据""")
    kpiMtStateLogQuerySql = f"select * from {kpi_mt_state_log} where (start_time>='{startTimeforKpi}' and start_time<='{endTimeforKpi}') and (end_time>='{startTimeforKpi}' and end_time<='{endTimeforKpi}') and duration>5000 order by start_time asc"
    # kpiMtStateLogQuerySql = f"select * from {kpi_mt_state_log} where (start_time>='2025-01-22 06:32:19.965000' and start_time<='2025-01-22 06:56:35.723000') and (end_time>='2025-01-22 06:32:19.965000' and end_time<='2025-01-22 06:56:35.723000') order by start_time asc"
    kpiMtStateLogQueryResults = o.query(kpiMtStateLogQuerySql,t='df')
    # 查询出来不为空才进行下一步
    if isinstance(kpiMtStateLogQueryResults,pd.DataFrame):
        # 遍历每条数据，并且每条数据都有start_time和end_time,应该进行for插入{kpi_for_qcms}
        for index,kpiMtStateLogQueryResult in kpiMtStateLogQueryResults.iterrows():

            ##插入数据前生成唯一的值用来放到数据库配对
            paired_value = snowFlakeId()

            DATA_FROM = f"""KPIDB.{kpi_mt_state_log}.{kpiMtStateLogQueryResult['state_id']}.start_time"""  # 需要改的部分
            DATA_FROM_TYPE = 'KPI'
            NOTES = f"""KPI记录的state={kpiMtStateLogQueryResult['state_id']}{stateTransToLanguage(kpiMtStateLogQueryResult['state_id'])} 对应的start_time"""  # 需要改的部分
            insertsql = f'''insert into {kpi_for_qcms}(\
            STS_NO,\
            TASK_ID_FOR_KPI_MT,\
            KEYTIME,\
            DATA_FROM,\
            DATA_FROM_TYPE,\
            NOTES,\
            PAIRED_VALUE) VALUES (
        '{kpiMtStateLogQueryResult['crane_id']}',\
        {kpiMtStateLogQueryResult['task_id_low']},\
        '{convertUtc_5(strChangeTime(kpiMtStateLogQueryResult['start_time']))}',\
        '{DATA_FROM}',\
        '{DATA_FROM_TYPE}',\
        '{NOTES}',\
        {paired_value}
        )'''
            print(insertsql)
            o.executesql(insertsql)


            DATA_FROM = f"""KPIDB.{kpi_mt_state_log}.{kpiMtStateLogQueryResult['state_id']}.end_time"""  # 需要改的部分
            DATA_FROM_TYPE = 'KPI'
            NOTES = f"""KPI记录的state={kpiMtStateLogQueryResult['state_id']}{stateTransToLanguage(kpiMtStateLogQueryResult['state_id'])} 对应的end_time"""  # 需要改的部分
            insertsql = f'''insert into {kpi_for_qcms}(\
            STS_NO,\
            TASK_ID_FOR_KPI_MT,\
            KEYTIME,\
            DATA_FROM,\
            DATA_FROM_TYPE,\
            NOTES,\
            PAIRED_VALUE) VALUES (
        '{kpiMtStateLogQueryResult['crane_id']}',\
        {kpiMtStateLogQueryResult['task_id_low']},\
        '{convertUtc_5(strChangeTime(kpiMtStateLogQueryResult['end_time']))}',\
        '{DATA_FROM}',\
        '{DATA_FROM_TYPE}',\
        '{NOTES}',\
        {paired_value}
        )'''
            print(insertsql)
            o.executesql(insertsql)
    ################################################################################{kpi_mt_state_log}



def handleQcTpInteractionHis():
    ############################################################################对qc_tp_interaction_his的处理
    print(f"""开始处理{qc_tp_interaction_his}表数据""")
    qcnos = [103,104,105,106,107,108]#岸桥编号
    lanenos = [1,2,3,4,5,6,7]#岸桥下车道编号
    for qcno in qcnos:
        for laneno in lanenos:
            #查询时间段内交互表的变化
            querySqlForQcTpInteractionHis = f'''select * from {qc_tp_interaction_his} where QC_ID={qcno} and LANE_ID={laneno} and (TRIG_CREATED>='{startTime}' and  TRIG_CREATED<='{endTime}') ORDER BY TRIG_CREATED asc'''
            qcTpInteractionHisQuryResults = o.query(querySqlForQcTpInteractionHis,t='df')
            if isinstance(qcTpInteractionHisQuryResults,pd.DataFrame):
                # 遍历行并比较相邻行的字段值
                #####遍历每一行数据
                for indexForQcTp,qcTpInteractionHisQuryResult in qcTpInteractionHisQuryResults.iterrows():
                    ############从第2行数据开始
                    if indexForQcTp >= 2:  # 表示从第2行开始计算数据
                        last_row = qcTpInteractionHisQuryResults.iloc[indexForQcTp-1]#上一行数据
                        current_row = qcTpInteractionHisQuryResults.iloc[indexForQcTp]#当前行数据
                        # 遍历上一行的每个字段和对应的值
                        changedata = {}
                        for column in ['QC_ID','LANE_ID','FMS_JOB_POS','FMS_AHT_ID','FMS_MOVE_KIND','FMS_AHT_STATUS','QC_REF1','QC_REF2','QC_STATUS']:#遍历这些字段乳沟有变化记录
                            last_val = last_row[column]
                            current_val = current_row[column]
                            if last_val!=current_val:#如果上一行字段跟下一行的值有变化则需要添加到字典中
                                changedata[column]=current_val

                        if changedata!={}:#如果当前行的有变化值
                            #将字典的值转换为逗号分隔的字符串
                            changedatavalues = '.'.join(str(value) for value in changedata.values())#将变化的值存一下，后面需要打入数据库中

                            DATA_FROM = f"""QCMSDB.{qc_tp_interaction_his}.{current_row['QC_ID']}.{current_row['LANE_ID']}.{changedatavalues}"""  # 需要改的部分
                            DATA_FROM_TYPE = 'QCMS'
                            NOTES = f'''{changedata}'''  # 需要改的部分
                            insertsql = f'''insert into {kpi_for_qcms}(\
                                STS_NO,\
                                KEYTIME,\
                                DATA_FROM,\
                                DATA_FROM_TYPE,\
                                NOTES) VALUES (
                        '{qcTpInteractionHisQuryResult['QC_ID']}',\
                        '{qcTpInteractionHisQuryResult['TRIG_CREATED']}',\
                        '{DATA_FROM}',\
                        '{DATA_FROM_TYPE}',\
                        "{NOTES}")'''
                        o.executesql(insertsql)
    #########################################################对qc_tp_interaction_his的处理




def handleMtWorkMode0():
    # 删除删除#########################################################对OPCUA数据处理：MtWorkMode的处理
    print(f"""开始处理{MtWorkMode}表数据""")
    qcnos = ['103','104','105','106','107','108']#岸桥编号
    ####第1步骤，删除错误数据
    deletesql=f"""delete from {MtWorkMode} where StatusCode='BadCommunicationError'"""
    o.executesql(deletesql)

    ###第2步：将数据库中字段带SourceTime''的时间去掉
    querySqlForMtWorkMode = f'''select * from {MtWorkMode}'''
    mtWorkModeQuryResults = o.query(querySqlForMtWorkMode,t='df')
    if isinstance(mtWorkModeQuryResults,pd.DataFrame):
        for indexForMtWorkMode, mtWorkModeQuryResult in mtWorkModeQuryResults.iterrows():  #####遍历每一行数据
            newtimestr = mtWorkModeQuryResult['SourceTime'].replace("'","")
            updatesql =f"""update {MtWorkMode} set SourceTime='{newtimestr}' where ID={mtWorkModeQuryResult['ID']}"""
            o.executesql(updatesql)

    #第3步：更新MtWorkMode表QC_ID,规定时间段内
    querySqlForMtWorkMode = f'''select * from {MtWorkMode} where (SourceTime>='{startTime}' and SourceTime<='{endTime}') order by SourceTime asc'''
    mtWorkModeQueryResults = o.query(querySqlForMtWorkMode,t='df')
    if isinstance(mtWorkModeQueryResults,pd.DataFrame):
        for index,mtWorkModeQueryResult in mtWorkModeQueryResults.iterrows():
            QC_ID=mtWorkModeQueryResult['TagName'][10:13]
            updateMtWorkModeSql=f'''update {MtWorkMode} set QC_ID='{QC_ID}' where ID={mtWorkModeQueryResult['ID']}'''
            o.executesql(updateMtWorkModeSql)
    #第1步：更新MtWorkMode表###############

    #第4步：更新MtWorkMode表中的NOTES字段标注
    o.executesql(f"""update {MtWorkMode} set notes='Normal Mode' where Value=1""")
    o.executesql(f"""update {MtWorkMode} set notes='Maintenance Mode' where Value=2""")
    o.executesql(f"""update {MtWorkMode} set notes='Local Mode' where Value=3""")
    ###############更新MtWorkMode表###############


    #第5步将重复的数据去掉
    for qcno in qcnos:
        # 将字符串转换为datetime对象
        querySqlForMtWorkMode = f'''select * from {MtWorkMode} where QC_ID='{qcno}' and (SourceTime>='{startTime}' and  SourceTime<='{endTime}') ORDER BY SourceTime asc'''
        mtWorkModeQuryResults = o.query(querySqlForMtWorkMode,t='df')
        if isinstance(mtWorkModeQuryResults, pd.DataFrame):
            # 遍历行并比较相邻行的字段值
            for indexForMtWorkMode,mtWorkModeQuryResult in mtWorkModeQuryResults.iterrows():#####遍历每一行数据
                ############从第2行数据开始
                if indexForMtWorkMode >= 2:  # 表示从第2行开始计算数据
                    last_row = mtWorkModeQuryResults.iloc[indexForMtWorkMode-1]#上一行数据
                    current_row = mtWorkModeQuryResults.iloc[indexForMtWorkMode]#当前行数据
                    if last_row['Value']==current_row['Value']:#检测到上一行数据的Value值和下一行一致，删除上一行数据
                        deletesql=f"""delete from {MtWorkMode} where ID={last_row['ID']}"""
                        o.executesql(deletesql)

    #第6步将需要的数据插入到目标数据库表中
    for qcno in qcnos:
        querySqlForMtWorkMode = f'''select * from {MtWorkMode} where QC_ID='{qcno}' and (SourceTime>='{startTime}' and  SourceTime<='{endTime}') ORDER BY SourceTime asc'''
        mtWorkModeQuryResults = o.query(querySqlForMtWorkMode,t='df')
        if isinstance(mtWorkModeQuryResults,pd.DataFrame):
            for indexForMtWorkMode,mtWorkModeQuryResult in mtWorkModeQuryResults.iterrows():#遍历每一行数据
                DATA_FROM = f"""QCMSDB.{MtWorkMode}.{mtWorkModeQuryResult['Value']}"""  # 需要改的部分
                DATA_FROM_TYPE = 'OPCUA'
                NOTES = f'''{mtWorkModeQuryResult['notes']}'''  # 需要改的部分
                insertsql = f'''insert into {kpi_for_qcms}(\
                    STS_NO,\
                    KEYTIME,\
                    DATA_FROM,\
                    DATA_FROM_TYPE,\
                    NOTES) VALUES (
            '{mtWorkModeQuryResult['QC_ID']}',\
            '{mtWorkModeQuryResult['SourceTime']}',\
            '{DATA_FROM}',\
            '{DATA_FROM_TYPE}',\
            "{NOTES}")'''
                o.executesql(insertsql)
    #########################################################对OPCUA数据处理：MtWorkMode的处理





def handleMtWorkMode():
    # #########################################################对OPCUA数据处理：MtWorkMode的处理
    print(f"""开始处理{MtWorkMode}表数据""")
    qcnos = [103,104,105,106,107,108]#岸桥编号
    ####第1步骤，删除错误数据
    deletesql=f"""delete from {MtWorkMode} where Quality=0"""
    o.executesql(deletesql)

    ###第2步：将数据库中字段带SourceTime''的时间去掉,需要筛选时间框
    querySqlForMtWorkMode = f"""select * from {MtWorkMode} where Timestamp like '%T%'"""
    mtWorkModeQuryResults = o.query(querySqlForMtWorkMode,t='df')
    if isinstance(mtWorkModeQuryResults,pd.DataFrame):
        for indexForMtWorkMode, mtWorkModeQuryResult in mtWorkModeQuryResults.iterrows():  #####遍历每一行数据
            newtimestr = mtWorkModeQuryResult['Timestamp'].replace("T"," ").replace('Z','')
            updatesql =f"""update {MtWorkMode} set Timestamp='{newtimestr}' where ID={mtWorkModeQuryResult['ID']}"""
            o.executesql(updatesql)
            # except:
            #     pass

    #第3步：更新MtWorkMode表中的NOTES字段标注
    o.executesql(f"""update {MtWorkMode} set notes='Normal Mode' where Value=1 and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}'""")
    o.executesql(f"""update {MtWorkMode} set notes='Maintenance Mode' where Value=2 and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}'""")
    o.executesql(f"""update {MtWorkMode} set notes='Local Mode' where Value=3 and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}'""")
    ###############更新MtWorkMode表###############


    #第4步将重复的数据去掉
    for qcno in qcnos:
        # 将字符串转换为datetime对象
        querySqlForMtWorkMode = f'''select * from {MtWorkMode} where MachineryName={qcno} and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}' ORDER BY Timestamp asc'''
        # querySqlForMtWorkMode = f'''select * from {MtWorkMode} where MachineryName={qcno} ORDER BY Timestamp asc'''
        mtWorkModeQuryResults = o.query(querySqlForMtWorkMode,t='df')
        if isinstance(mtWorkModeQuryResults, pd.DataFrame):
            # 遍历行并比较相邻行的字段值
            for indexForMtWorkMode,mtWorkModeQuryResult in mtWorkModeQuryResults.iterrows():#####遍历每一行数据
                ############从第2行数据开始
                if indexForMtWorkMode >= 2:  # 表示从第2行开始计算数据
                    last_row = mtWorkModeQuryResults.iloc[indexForMtWorkMode-1]#上一行数据
                    current_row = mtWorkModeQuryResults.iloc[indexForMtWorkMode]#当前行数据
                    if last_row['Value']==current_row['Value']:#检测到上一行数据的Value值和下一行一致，删除上一行数据
                        deletesql=f"""delete from {MtWorkMode} where ID={last_row['ID']}"""
                        o.executesql(deletesql)
                        print(deletesql)

    #第5步将开始和结束时间对应的时间节点前后各加一条数据
    for qcno in qcnos:
        querySqlForMtWorkMode = f'''select * from {MtWorkMode} where MachineryName={qcno} and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}' ORDER BY Timestamp asc'''
        mtWorkModeQuryResults = o.query(querySqlForMtWorkMode,t='df')
        if isinstance(mtWorkModeQuryResults, pd.DataFrame):
            mintimeforntworkmode = mtWorkModeQuryResults.iloc[0]['Timestamp']
            maxtimeforntworkmode = mtWorkModeQuryResults.iloc[-1]['Timestamp']
            if mintimeforntworkmode!='' and maxtimeforntworkmode!='':
                formtworkmodewholehourtimes=wholeHourTimeEnds(mintimeforntworkmode,maxtimeforntworkmode)
                for formtworkmodewholehourtime in formtworkmodewholehourtimes:

                    #整点时间-1秒的数据
                    formtworkmodewholehourtime_subtraction1second = timeChangeStr(strChangeTime(formtworkmodewholehourtime) - timedelta(seconds=1))
                    querySqlForMtWorkModeForWholeHourTimesubtraction1seconds = f'''select * from {MtWorkMode} where MachineryName={qcno} and Timestamp<'{formtworkmodewholehourtime_subtraction1second}' ORDER BY Timestamp desc'''
                    MtWorkModeForWholeHourTimesubtraction1secondsQueryResults = o.query(querySqlForMtWorkModeForWholeHourTimesubtraction1seconds,t='df')
                    if isinstance(MtWorkModeForWholeHourTimesubtraction1secondsQueryResults,pd.DataFrame):
                        firstQueryResult=MtWorkModeForWholeHourTimesubtraction1secondsQueryResults.iloc[0]
                        insertsql=f'''insert into {MtWorkMode}(
                        PortName,
                        MachineryName,
                        ItemName,
                        Quality,
                        DataType,
                        IsArray,
                        Value,
                        Timestamp,
                        Labels,
                        notes) VALUES (
                        '{firstQueryResult['PortName']}',
    {firstQueryResult['MachineryName']},
    '{firstQueryResult['ItemName']}',
    {firstQueryResult['Quality']},
    '{firstQueryResult['DataType']}',
    {firstQueryResult['IsArray']},
    {firstQueryResult['Value']},
    '{formtworkmodewholehourtime_subtraction1second}',
    '{firstQueryResult['Labels']}',
    '{firstQueryResult['notes']}'
                        )'''
                        o.executesql(insertsql)

                    # 整点时间+1秒的数据
                    formtworkmodewholehourtime_plus1second = timeChangeStr(strChangeTime(formtworkmodewholehourtime) + timedelta(seconds=1))
                    querySqlForMtWorkModeForWholeHourTimeplus1seconds = f'''select * from {MtWorkMode} where MachineryName={qcno} and Timestamp<'{formtworkmodewholehourtime_plus1second}' ORDER BY Timestamp desc'''
                    MtWorkModeForWholeHourTimeplus1secondsQueryResults = o.query(
                        querySqlForMtWorkModeForWholeHourTimeplus1seconds, t='df')
                    if isinstance(MtWorkModeForWholeHourTimeplus1secondsQueryResults, pd.DataFrame):
                        firstQueryResult = MtWorkModeForWholeHourTimeplus1secondsQueryResults.iloc[0]
                        insertsql = f'''insert into {MtWorkMode}(
                                        PortName,
                                        MachineryName,
                                        ItemName,
                                        Quality,
                                        DataType,
                                        IsArray,
                                        Value,
                                        Timestamp,
                                        Labels,
                                        notes) VALUES (
                                        '{firstQueryResult['PortName']}',
                    {firstQueryResult['MachineryName']},
                    '{firstQueryResult['ItemName']}',
                    {firstQueryResult['Quality']},
                    '{firstQueryResult['DataType']}',
                    {firstQueryResult['IsArray']},
                    {firstQueryResult['Value']},
                    '{formtworkmodewholehourtime_plus1second}',
                    '{firstQueryResult['Labels']}',
                    '{firstQueryResult['notes']}'
                                        )'''
                        o.executesql(insertsql)
    #
    #
    #第6步将需要的数据插入到目标数据库表中
    for qcno in qcnos:
        querySqlForMtWorkMode = f'''select * from {MtWorkMode} where MachineryName={qcno} and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}' ORDER BY Timestamp asc'''
        mtWorkModeQuryResults = o.query(querySqlForMtWorkMode,t='df')
        if isinstance(mtWorkModeQuryResults,pd.DataFrame):
            for indexForMtWorkMode,mtWorkModeQuryResult in mtWorkModeQuryResults.iterrows():#遍历每一行数据
                DATA_FROM = f"""OPCUA.{MtWorkMode}.{mtWorkModeQuryResult['Value']}"""  # 需要改的部分
                DATA_FROM_TYPE = 'OPCUA'
                NOTES = f'''{mtWorkModeQuryResult['notes']}'''  # 需要改的部分
                insertsql = f'''insert into {kpi_for_qcms}(\
                    STS_NO,\
                    KEYTIME,\
                    DATA_FROM,\
                    DATA_FROM_TYPE,\
                    NOTES) VALUES (
            '{mtWorkModeQuryResult['MachineryName']}',\
            '{convertUtc_5(strChangeTime(mtWorkModeQuryResult['Timestamp']))}',
            '{DATA_FROM}',\
            '{DATA_FROM_TYPE}',\
            '{NOTES}')'''
                o.executesql(insertsql)
                print(insertsql)
    #########################################################对OPCUA数据处理：MtWorkMode的处理
#
#
#
#
#
#
def handleMtInstructionStatus():
    # # #########################################################对OPCUA数据处理：MtInstructionStatus的处理
    print(f"""开始处理{MtInstructionStatus}表数据""")
    qcnos = [103,104,105,106,107,108]#岸桥编号
    ####第1步骤，删除错误数据
    deletesql=f"""delete from {MtInstructionStatus} where Quality='false' or Timestamp=''"""
    o.executesql(deletesql)


    ###第2步先将数据库中字段"Timestamp"带T和Z字样去掉,因为数据库导入时时间格式带有'2025-01-08T22:45:03.713Z'
    querySqlForMtInstructionStatus = f"""select * from {MtInstructionStatus} where Timestamp like '%T%'"""
    mtInstructionStatusQuryResults = o.query(querySqlForMtInstructionStatus,t='df')
    if isinstance(mtInstructionStatusQuryResults,pd.DataFrame):
        for indexForMtInstructionStatus, mtInstructionStatusQuryResult in mtInstructionStatusQuryResults.iterrows():  #####遍历每一行数据
            newtimestr = mtInstructionStatusQuryResult['Timestamp'].replace("T"," ").replace('Z','')
            updatesql =f"""update {MtInstructionStatus} set Timestamp='{newtimestr}' where ID={mtInstructionStatusQuryResult['ID']}"""
            try:
                o.executesql(updatesql)
                print(updatesql)
            except:
                pass


    #第3步：更新MtInstructionStatus表中的NOTES字段标注
    o.executesql(f"""update {MtInstructionStatus} set notes='Ready for ECS' where Value=1 and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}' """)
    o.executesql(f"""update {MtInstructionStatus} set notes='Not ready for ECS' where Value=2 and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}' """)
    ###############更新MtInstructionStatus表###############


    #第4步：将重复的数据去掉
    for qcno in qcnos:
        # 将字符串转换为datetime对象
        querySqlForMtInstructionStatus = f'''select * from {MtInstructionStatus} where MachineryName={qcno} and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}' ORDER BY Timestamp asc'''
        mtInstructionStatusQuryResults = o.query(querySqlForMtInstructionStatus,t='df')
        if isinstance(mtInstructionStatusQuryResults, pd.DataFrame):
            # 遍历行并比较相邻行的字段值
            for indexForMtInstructionStatus,mtInstructionStatusQuryResult in mtInstructionStatusQuryResults.iterrows():#####遍历每一行数据
                ############从第2行数据开始
                if indexForMtInstructionStatus >= 2:  # 表示从第2行开始计算数据
                    last_row = mtInstructionStatusQuryResults.iloc[indexForMtInstructionStatus-1]#上一行数据
                    current_row = mtInstructionStatusQuryResults.iloc[indexForMtInstructionStatus]#当前行数据
                    if last_row['Value']==current_row['Value']:#检测到上一行数据的Value值和下一行一致，删除上一行数据
                        deletesql=f"""delete from {MtInstructionStatus} where ID={last_row['ID']}"""
                        o.executesql(deletesql)
                        print(deletesql)

    #第5步将开始和结束时间对应的时间节点前后各加一条数据
    for qcno in qcnos:
        querySqlForMtInstructionStatus = f'''select * from {MtInstructionStatus} where MachineryName={qcno} and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}' ORDER BY Timestamp asc'''
        mtInstructionStatusQuryResults = o.query(querySqlForMtInstructionStatus,t='df')
        if isinstance(mtInstructionStatusQuryResults, pd.DataFrame):
            mintimeforntworkmode = mtInstructionStatusQuryResults.iloc[0]['Timestamp']
            maxtimeforntworkmode = mtInstructionStatusQuryResults.iloc[-1]['Timestamp']
            if mintimeforntworkmode!='' and maxtimeforntworkmode!='':
                formtworkmodewholehourtimes=wholeHourTimeEnds(mintimeforntworkmode,maxtimeforntworkmode)
                for formtworkmodewholehourtime in formtworkmodewholehourtimes: #遍历整点时间

                    #整点时间-1秒的数据
                    formtworkmodewholehourtime_subtraction1second = timeChangeStr(strChangeTime(formtworkmodewholehourtime) - timedelta(seconds=1))
                    querySqlForMtInstructionStatusForWholeHourTimesubtraction1seconds = f'''select * from {MtInstructionStatus} where MachineryName={qcno} and Timestamp<'{formtworkmodewholehourtime_subtraction1second}' ORDER BY Timestamp desc'''
                    MtInstructionStatusForWholeHourTimesubtraction1secondsQueryResults = o.query(querySqlForMtInstructionStatusForWholeHourTimesubtraction1seconds,t='df')
                    if isinstance(MtInstructionStatusForWholeHourTimesubtraction1secondsQueryResults,pd.DataFrame):
                        firstQueryResult=MtInstructionStatusForWholeHourTimesubtraction1secondsQueryResults.iloc[0]
                        insertsql=f'''insert into {MtInstructionStatus}(
                        PortName,
                        MachineryName,
                        ItemName,
                        Quality,
                        DataType,
                        IsArray,
                        Value,
                        Timestamp,
                        Labels,
                        notes) VALUES (
                        '{firstQueryResult['PortName']}',
    {firstQueryResult['MachineryName']},
    '{firstQueryResult['ItemName']}',
    {firstQueryResult['Quality']},
    '{firstQueryResult['DataType']}',
    {firstQueryResult['IsArray']},
    {firstQueryResult['Value']},
    '{formtworkmodewholehourtime_subtraction1second}',
    '{firstQueryResult['Labels']}',
    '{firstQueryResult['notes']}'
                        )'''
                        o.executesql(insertsql)
                        print(insertsql)

                    # 整点时间+1秒的数据
                    formtworkmodewholehourtime_plus1second = timeChangeStr(strChangeTime(formtworkmodewholehourtime) + timedelta(seconds=1))
                    querySqlForMtInstructionStatusForWholeHourTimeplus1seconds = f'''select * from {MtInstructionStatus} where MachineryName={qcno} and Timestamp<'{formtworkmodewholehourtime_plus1second}' ORDER BY Timestamp desc'''
                    MtInstructionStatusForWholeHourTimeplus1secondsQueryResults = o.query(
                        querySqlForMtInstructionStatusForWholeHourTimeplus1seconds, t='df')
                    if isinstance(MtInstructionStatusForWholeHourTimeplus1secondsQueryResults, pd.DataFrame):
                        firstQueryResult = MtInstructionStatusForWholeHourTimeplus1secondsQueryResults.iloc[0]
                        insertsql = f'''insert into {MtInstructionStatus}(
                                        PortName,
                                        MachineryName,
                                        ItemName,
                                        Quality,
                                        DataType,
                                        IsArray,
                                        Value,
                                        Timestamp,
                                        Labels,
                                        notes) VALUES (
                                        '{firstQueryResult['PortName']}',
                    {firstQueryResult['MachineryName']},
                    '{firstQueryResult['ItemName']}',
                    {firstQueryResult['Quality']},
                    '{firstQueryResult['DataType']}',
                    {firstQueryResult['IsArray']},
                    {firstQueryResult['Value']},
                    '{formtworkmodewholehourtime_plus1second}',
                    '{firstQueryResult['Labels']}',
                    '{firstQueryResult['notes']}'
                                        )'''
                        o.executesql(insertsql)
                        print(insertsql)

    #
    #第6步：将需要的数据插入到目标数据库表中
    for qcno in qcnos:
        querySqlForMtInstructionStatus = f'''select * from {MtInstructionStatus} where MachineryName={qcno} and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}' ORDER BY Timestamp asc'''
        mtInstructionStatusQuryResults = o.query(querySqlForMtInstructionStatus,t='df')
        if isinstance(mtInstructionStatusQuryResults,pd.DataFrame):
            for indexForMtInstructionStatus,mtInstructionStatusQuryResult in mtInstructionStatusQuryResults.iterrows():#遍历每一行数据
                DATA_FROM = f"""OPCUA.{MtInstructionStatus}.{mtInstructionStatusQuryResult['Value']}"""  # 需要改的部分
                DATA_FROM_TYPE = 'OPCUA'
                NOTES = f'''{mtInstructionStatusQuryResult['notes']}'''  # 需要改的部分
                insertsql = f'''insert into {kpi_for_qcms}(\
                    STS_NO,\
                    KEYTIME,\
                    DATA_FROM,\
                    DATA_FROM_TYPE,\
                    NOTES) VALUES (
            '{mtInstructionStatusQuryResult['MachineryName']}',\
            '{convertUtc_5(strChangeTime(mtInstructionStatusQuryResult['Timestamp']))}',\
            '{DATA_FROM}',\
            '{DATA_FROM_TYPE}',\
            "{NOTES}")'''
                o.executesql(insertsql)
                print(insertsql)
    #########################################################对OPCUA数据处理：MtInstructionStatus的处理





def handleMhAboveSafeHeight():
    #########################################################对OPCUA数据处理：MhAboveSafeHeight的处理
    #o.executesql(f"""delete from {kpi_for_qcms}""")
    print(f"""开始处理{MhAboveSafeHeight}表数据""")
    qcnos = [103,104,105,106,107,108]#岸桥编号
    ####第1步骤，删除错误数据
    deletesql=f"""delete from {MhAboveSafeHeight} where Quality='false'"""
    o.executesql(deletesql)
    print(deletesql)
    ####第1步骤，删除错误数据

    ###第2步先将数据库中字段"Timestamp"带T和Z字样去掉,因为数据库导入时时间格式带有'2025-01-08T22:45:03.713Z'
    querySqlForMhAboveSafeHeight = f"""select * from {MhAboveSafeHeight} where Timestamp like '%T%'"""
    mhAboveSafeHeightQuryResults = o.query(querySqlForMhAboveSafeHeight,t='df')
    if isinstance(mhAboveSafeHeightQuryResults,pd.DataFrame):
        for indexForMhAboveSafeHeight, mhAboveSafeHeightQuryResult in mhAboveSafeHeightQuryResults.iterrows():  #####遍历每一行数据
            newtimestr = mhAboveSafeHeightQuryResult['Timestamp'].replace("T"," ").replace('Z','')
            updatesql =f"""update {MhAboveSafeHeight} set Timestamp='{newtimestr}' where ID={mhAboveSafeHeightQuryResult['ID']}"""
            try:
                o.executesql(updatesql)
                print(updatesql)
            except:
                pass



    #第3步：更新MhAboveSafeHeight表中的NOTES字段标注
    o.executesql(f"""update {MhAboveSafeHeight} set notes='Above' where Value=1 and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}' """)
    o.executesql(f"""update {MhAboveSafeHeight} set notes='Below' where Value=2 and Timestamp>='{startTimeforKpi}' and Timestamp<='{endTimeforKpi}' """)
    ###############更新MhAboveSafeHeight表###############

    #第4步将重复的数据去掉,,想要的时间段内
    for qcno in qcnos:
        # 将字符串转换为datetime对象
        querySqlForMhAboveSafeHeight = f'''select * from {MhAboveSafeHeight} where MachineryName={qcno} and (Timestamp>='{startTimeforKpi}' and  Timestamp<='{endTimeforKpi}') ORDER BY Timestamp asc'''
        mhAboveSafeHeightQuryResults = o.query(querySqlForMhAboveSafeHeight,t='df')
        if isinstance(mhAboveSafeHeightQuryResults, pd.DataFrame):
            # 遍历行并比较相邻行的字段值
            for indexForMhAboveSafeHeight,mhAboveSafeHeightQuryResult in mhAboveSafeHeightQuryResults.iterrows():#####遍历每一行数据
                ############从第2行数据开始
                if indexForMhAboveSafeHeight >= 2:  # 表示从第2行开始计算数据
                    last_row = mhAboveSafeHeightQuryResults.iloc[indexForMhAboveSafeHeight-1]#上一行数据
                    current_row = mhAboveSafeHeightQuryResults.iloc[indexForMhAboveSafeHeight]#当前行数据
                    if last_row['Value']==current_row['Value']:#检测到上一行数据的Value值和下一行一致，删除上一行数据
                        deletesql=f"""delete from {MhAboveSafeHeight} where ID={last_row['ID']}"""
                        o.executesql(deletesql)
                        print(deletesql)

    ##第5步将开始和结束时间对应的时间节点前后各加一条数据
    for qcno in qcnos:
        querySqlForMhAboveSafeHeitht = f'''select * from {MhAboveSafeHeight} where MachineryName={qcno} and (Timestamp>='{startTimeforKpi}' and  Timestamp<='{endTimeforKpi}') ORDER BY Timestamp asc'''
        mhAboveSafeHeithtQuryResults = o.query(querySqlForMhAboveSafeHeitht,t='df')
        if isinstance(mhAboveSafeHeithtQuryResults, pd.DataFrame):
            mintimeforntworkmode = mhAboveSafeHeithtQuryResults.iloc[0]['Timestamp']
            maxtimeforntworkmode = mhAboveSafeHeithtQuryResults.iloc[-1]['Timestamp']
            if mintimeforntworkmode!='' and maxtimeforntworkmode!='':
                formtworkmodewholehourtimes=wholeHourTimeEnds(mintimeforntworkmode,maxtimeforntworkmode)
                for formtworkmodewholehourtime in formtworkmodewholehourtimes: #遍历整点时间

                    #整点时间-1秒的数据
                    formtworkmodewholehourtime_subtraction1second = timeChangeStr(strChangeTime(formtworkmodewholehourtime) - timedelta(seconds=1))
                    querySqlForMhAboveSafeHeithtForWholeHourTimesubtraction1seconds = f'''select * from {MhAboveSafeHeight} where MachineryName={qcno} and Timestamp<'{formtworkmodewholehourtime_subtraction1second}' ORDER BY Timestamp desc'''
                    MhAboveSafeHeithtForWholeHourTimesubtraction1secondsQueryResults = o.query(querySqlForMhAboveSafeHeithtForWholeHourTimesubtraction1seconds,t='df')
                    if isinstance(MhAboveSafeHeithtForWholeHourTimesubtraction1secondsQueryResults,pd.DataFrame):
                        firstQueryResult=MhAboveSafeHeithtForWholeHourTimesubtraction1secondsQueryResults.iloc[0]
                        insertsql=f'''insert into {MhAboveSafeHeight}(
                        PortName,
                        MachineryName,
                        ItemName,
                        Quality,
                        DataType,
                        IsArray,
                        Value,
                        Timestamp,
                        Labels,
                        notes) VALUES (
                        '{firstQueryResult['PortName']}',
    {firstQueryResult['MachineryName']},
    '{firstQueryResult['ItemName']}',
    {firstQueryResult['Quality']},
    '{firstQueryResult['DataType']}',
    {firstQueryResult['IsArray']},
    {firstQueryResult['Value']},
    '{formtworkmodewholehourtime_subtraction1second}',
    '{firstQueryResult['Labels']}',
    '{firstQueryResult['notes']}'
                        )'''
                        o.executesql(insertsql)
                        print(f"insert into {MhAboveSafeHeight}")

                    # 整点时间+1秒的数据
                    formtworkmodewholehourtime_plus1second = timeChangeStr(strChangeTime(formtworkmodewholehourtime) + timedelta(seconds=1))
                    querySqlForMhAboveSafeHeithtForWholeHourTimeplus1seconds = f'''select * from {MhAboveSafeHeight} where MachineryName={qcno} and Timestamp<'{formtworkmodewholehourtime_plus1second}' ORDER BY Timestamp desc'''
                    MhAboveSafeHeithtForWholeHourTimeplus1secondsQueryResults = o.query(
                        querySqlForMhAboveSafeHeithtForWholeHourTimeplus1seconds, t='df')
                    if isinstance(MhAboveSafeHeithtForWholeHourTimeplus1secondsQueryResults, pd.DataFrame):
                        firstQueryResult = MhAboveSafeHeithtForWholeHourTimeplus1secondsQueryResults.iloc[0]
                        insertsql = f'''insert into {MhAboveSafeHeight}(
                                        PortName,
                                        MachineryName,
                                        ItemName,
                                        Quality,
                                        DataType,
                                        IsArray,
                                        Value,
                                        Timestamp,
                                        Labels,
                                        notes) VALUES (
                                        '{firstQueryResult['PortName']}',
                    {firstQueryResult['MachineryName']},
                    '{firstQueryResult['ItemName']}',
                    {firstQueryResult['Quality']},
                    '{firstQueryResult['DataType']}',
                    {firstQueryResult['IsArray']},
                    {firstQueryResult['Value']},
                    '{formtworkmodewholehourtime_plus1second}',
                    '{firstQueryResult['Labels']}',
                    '{firstQueryResult['notes']}'
                                        )'''
                        o.executesql(insertsql)
                        print(f"insert into {MhAboveSafeHeight}")
    #第6步将需要的数据插入到目标数据库表中
    for qcno in qcnos:
        querySqlForMhAboveSafeHeight = f'''select * from {MhAboveSafeHeight} where MachineryName={qcno} and (Timestamp>='{startTimeforKpi}' and  Timestamp<='{endTimeforKpi}') ORDER BY Timestamp asc'''
        mhAboveSafeHeightQuryResults = o.query(querySqlForMhAboveSafeHeight,t='df')
        if isinstance(mhAboveSafeHeightQuryResults,pd.DataFrame):
            for indexForMhAboveSafeHeight,mhAboveSafeHeightQuryResult in mhAboveSafeHeightQuryResults.iterrows():#遍历每一行数据
                DATA_FROM = f'''OPCUA.{MhAboveSafeHeight}.{mhAboveSafeHeightQuryResult['Value']}'''  # 需要改的部分
                DATA_FROM_TYPE = 'OPCUA'
                NOTES = f'''{mhAboveSafeHeightQuryResult['notes']}'''  # 需要改的部分
                insertsql = f'''insert into {kpi_for_qcms}(\
                    STS_NO,\
                    KEYTIME,\
                    DATA_FROM,\
                    DATA_FROM_TYPE,\
                    NOTES) VALUES (
            '{mhAboveSafeHeightQuryResult['MachineryName']}',\
            '{convertUtc_5(strChangeTime(mhAboveSafeHeightQuryResult['Timestamp']))}',\
            '{DATA_FROM}',\
            '{DATA_FROM_TYPE}',\
            '{NOTES}')'''
                print(f"insert into {kpi_for_qcms}")
                o.executesql(insertsql)
    #########################################################对OPCUA数据处理：MhAboveSafeHeight的处理