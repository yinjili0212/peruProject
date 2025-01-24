import yaml
import sqliteHandle
import yaml
import pandas as pd
import logService
import datetime
#导入配置文件
envlist = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
log=logService.logfunc(envlist['lognameaddress'])
o=sqliteHandle.sqliteHandler(envlist['sqllitedbaddress'])

n =1#步骤开始的位置
print(f"""步骤{n}:将对应数据库表中数据只保留一个月数据""")
deletesql =f"""delete from {envlist['kpi_for_qcms_tablename']} where KEYTIME<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['qcms_kpi_for_container_transfer_tablename']} where PICKUP_TIME<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['qc_tos_task_tablename']} where RESPONSE_TIME<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['qc_tos_task_his_tablename']} where TRIG_CREATED<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)

deletesql =f"""delete from {envlist['qc_gantry_instruction_tablename']} where START_TIME<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['qc_trolley_instruction_tablename']} where START_TIME<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['qc_trolley_task_tablename']} where START_TIME<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['qc_container_transfer_tablename']} where CREATE_TIME<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['kpi_mt_step_log_tablename']} where start_time<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['kpi_mt_state_log_tablename']} where start_time<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['qc_tp_interaction_his_tablename']} where TRIG_CREATED<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['qc_event_recorder_his_tablename']} where CREATE_TIME<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['mtworkmode_tablename']} where Timestamp<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['mtinstructionstatus_tablename']} where Timestamp<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)
deletesql =f"""delete from {envlist['mhabovesafeheight_tablename']} where Timestamp<'{datetime.datetime.now()-datetime.timedelta(days=30)}'"""
o.executesql(deletesql)
print(deletesql)




n=n+1
print(f"""步骤{n}:计算{envlist['qc_tos_task_tablename']}表上一次导入的最新时间，从而计算需要从现场导出时间的脚本""")
querySqlforqctostask = f"""select * from {envlist['qc_tos_task_tablename']} where LOCK_TIME!='' and UNLOCK_TIME!='' and RESPONSE_TIME!='' order by RESPONSE_TIME desc"""
qctostaskQuerryReuslts = o.query(querySqlforqctostask,t='df')
if isinstance(qctostaskQuerryReuslts,pd.DataFrame):
    # print(qctostaskQuerryReuslts.iloc[0]['RESPONSE_TIME'])
    print(f"""上次从从现场拷贝的{envlist['qc_tos_task_tablename']}表的数据RESPONSE_TIME最后时间为{qctostaskQuerryReuslts.iloc[0]['RESPONSE_TIME']}""")
    print(f"""本次需要从现场拷贝QCMSDB的{envlist['qc_tos_task_tablename']}表的数据RESPONSE_TIME应该从上次时间后面提取，相关sql脚本如下：""")
    selectSqlForQcTosTask= f"""select * from {envlist['qc_tos_task_tablename']} where RESPONSE_TIME>'{qctostaskQuerryReuslts.iloc[0]['RESPONSE_TIME']}' order by RESPONSE_TIME asc;"""
    print(selectSqlForQcTosTask)
else:
    print(f"""需要将{envlist['qc_tos_task_tablename']}表数据全部导出""")
print('*************************')

n =n+1
print(f"""步骤{n}:计算{envlist['qc_tos_task_his_tablename']}表上一次导入的最新时间，从而计算需要从现场导出时间的脚本""")
querySqlfor = f"""select * from {envlist['qc_tos_task_his_tablename']} order by TRIG_CREATED desc"""
equerryReuslts = o.query(querySqlfor,t='df')
if isinstance(equerryReuslts,pd.DataFrame):
    print(f"""上次从从现场拷贝的{envlist['qc_tos_task_his_tablename']}表的数据TRIG_CREATED最后时间为{equerryReuslts.iloc[0]['TRIG_CREATED']}""")
    print(f"""本次需要从现场拷贝QCMSDB的{envlist['qc_tos_task_his_tablename']}表的数据TRIG_CREATED应该从上次时间后面提取，相关sql脚本如下：""")
    selectSqlForqctostaskhis= f"""select * from {envlist['qc_tos_task_his_tablename']} where TRIG_CREATED>TIMESTAMP '{equerryReuslts.iloc[0]['TRIG_CREATED']}' order by TRIG_CREATED asc;"""
    print(selectSqlForqctostaskhis)
else:
    print(f"""需要将{envlist['qc_tos_task_his_tablename']}表数据全部导出""")
print('*************************')

n =n+1
print(f"""步骤{n}:计算{envlist['qc_gantry_instruction_tablename']}表上一次导入的最新时间，从而计算需要从现场导出时间的脚本""")
querySqlfor = f"""select * from {envlist['qc_gantry_instruction_tablename']} where START_TIME!='' order by START_TIME desc"""
equerryReuslts = o.query(querySqlfor,t='df')
if isinstance(equerryReuslts,pd.DataFrame):
    print(f"""上次从从现场拷贝的{envlist['qc_gantry_instruction_tablename']}表的数据START_TIME最后时间为{equerryReuslts.iloc[0]['START_TIME']}""")
    print(f"""本次需要从现场拷贝QCMSDB的{envlist['qc_gantry_instruction_tablename']}表的数据START_TIME应该从上次时间后面提取，相关sql脚本如下：""")
    selectSqlForqcgantryinstruction= f"""select * from {envlist['qc_gantry_instruction_tablename']} where START_TIME>TIMESTAMP '{equerryReuslts.iloc[0]['START_TIME']}' order by START_TIME asc;"""
    print(selectSqlForqcgantryinstruction)
else:
    print(f"""需要将{envlist['qc_gantry_instruction_tablename']}表数据全部导出""")
print('*************************')


n =n+1
print(f"""步骤{n}:计算{envlist['qc_trolley_instruction_tablename']}表上一次导入的最新时间，从而计算需要从现场导出时间的脚本""")
querySqlfor = f"""select * from {envlist['qc_trolley_instruction_tablename']} where START_TIME!='' order by START_TIME desc"""
equerryReuslts = o.query(querySqlfor,t='df')
if isinstance(equerryReuslts,pd.DataFrame):
    print(f"""上次从从现场拷贝的{envlist['qc_trolley_instruction_tablename']}表的数据START_TIME最后时间为{equerryReuslts.iloc[0]['START_TIME']}""")
    print(f"""本次需要从现场拷贝QCMSDB的{envlist['qc_trolley_instruction_tablename']}表的数据START_TIME应该从上次时间后面提取，相关sql脚本如下：""")
    selectSqlForqctrolleyinstruction= f"""select * from {envlist['qc_trolley_instruction_tablename']} where START_TIME>TIMESTAMP '{equerryReuslts.iloc[0]['START_TIME']}' order by START_TIME asc;"""
    print(selectSqlForqctrolleyinstruction)
else:
    print(f"""需要将{envlist['qc_trolley_instruction_tablename']}表数据全部导出""")
print('*************************')


n =n+1
print(f"""步骤{n}:计算{envlist['qc_trolley_task_tablename']}表上一次导入的最新时间，从而计算需要从现场导出时间的脚本""")
querySqlfor = f"""select * from {envlist['qc_trolley_task_tablename']} where START_TIME!='' order by START_TIME desc"""
equerryReuslts = o.query(querySqlfor,t='df')
if isinstance(equerryReuslts,pd.DataFrame):
    print(f"""上次从从现场拷贝的{envlist['qc_trolley_task_tablename']}表的数据START_TIME最后时间为{equerryReuslts.iloc[0]['START_TIME']}""")
    print(f"""本次需要从现场拷贝QCMSDB的{envlist['qc_trolley_task_tablename']}表的数据START_TIME应该从上次时间后面提取，相关sql脚本如下：""")
    selectSqlForqctrolleytask= f"""select * from {envlist['qc_trolley_task_tablename']} where START_TIME>TIMESTAMP '{equerryReuslts.iloc[0]['START_TIME']}' order by START_TIME asc;"""
    print(selectSqlForqctrolleytask)
else:
    print(f"""需要将{envlist['qc_trolley_task_tablename']}表数据全部导出""")
print('*************************')


n =n+1
print(f"""步骤{n}:计算{envlist['qc_container_transfer_tablename']}表上一次导入的最新时间，从而计算需要从现场导出时间的脚本""")
querySqlfor = f"""select * from {envlist['qc_container_transfer_tablename']} where CREATE_TIME!='' order by CREATE_TIME desc"""
equerryReuslts = o.query(querySqlfor,t='df')
if isinstance(equerryReuslts,pd.DataFrame):
    print(f"""上次从从现场拷贝的{envlist['qc_container_transfer_tablename']}表的数据CREATE_TIME最后时间为{equerryReuslts.iloc[0]['CREATE_TIME']}""")
    print(f"""本次需要从现场拷贝QCMSDB的{envlist['qc_container_transfer_tablename']}表的数据CREATE_TIME应该从上次时间后面提取，相关sql脚本如下：""")
    selectSqlForqccontainertransfer= f"""select * from {envlist['qc_container_transfer_tablename']} where CREATE_TIME>TIMESTAMP '{equerryReuslts.iloc[0]['CREATE_TIME']}' order by CREATE_TIME asc;"""
    print(selectSqlForqccontainertransfer)
else:
    print(f"""需要将{envlist['qc_container_transfer_tablename']}表数据全部导出""")
print('*************************')


n =n+1
print(f"""步骤{n}:计算{envlist['qc_tp_interaction_his_tablename']}表上一次导入的最新时间，从而计算需要从现场导出时间的脚本""")
querySqlfor = f"""select * from {envlist['qc_tp_interaction_his_tablename']} where TRIG_CREATED!='' order by TRIG_CREATED desc"""
equerryReuslts = o.query(querySqlfor,t='df')
if isinstance(equerryReuslts,pd.DataFrame):
    print(f"""上次从从现场拷贝的{envlist['qc_tp_interaction_his_tablename']}表的数据TRIG_CREATED最后时间为{equerryReuslts.iloc[0]['TRIG_CREATED']}""")
    print(f"""本次需要从现场拷贝QCMSDB的{envlist['qc_tp_interaction_his_tablename']}表的数据TRIG_CREATED应该从上次时间后面提取，相关sql脚本如下：""")
    selectSqlForqctpinteractionhis= f"""select * from {envlist['qc_tp_interaction_his_tablename']} where TRIG_CREATED>TIMESTAMP '{equerryReuslts.iloc[0]['TRIG_CREATED']}' order by TRIG_CREATED asc;"""
    print(selectSqlForqctpinteractionhis)
else:
    print(f"""需要将{envlist['qc_tp_interaction_his_tablename']}表数据全部导出""")
print('*************************')


n =n+1
print(f"""步骤{n}:计算{envlist['qc_event_recorder_his_tablename']}表上一次导入的最新时间，从而计算需要从现场导出时间的脚本""")
querySqlfor = f"""select * from {envlist['qc_event_recorder_his_tablename']} where CREATE_TIME!='' order by CREATE_TIME desc"""
equerryReuslts = o.query(querySqlfor,t='df')
if isinstance(equerryReuslts,pd.DataFrame):
    print(f"""上次从从现场拷贝的{envlist['qc_event_recorder_his_tablename']}表的数据CREATE_TIME最后时间为{equerryReuslts.iloc[0]['CREATE_TIME']}""")
    print(f"""本次需要从现场拷贝QCMSDB的{envlist['qc_event_recorder_his_tablename']}表的数据CREATE_TIME应该从上次时间后面提取，相关sql脚本如下：""")
    selectSqlForqceventrecorderhis= f"""select * from {envlist['qc_event_recorder_his_tablename']} where CREATE_TIME>TIMESTAMP '{equerryReuslts.iloc[0]['CREATE_TIME']}' order by CREATE_TIME asc;"""
    print(selectSqlForqceventrecorderhis)
else:
    print(f"""需要将{envlist['qc_event_recorder_his_tablename']}表数据全部导出""")
print('*************************')


n =n+1
print(f"""步骤{n}:计算{envlist['kpi_mt_step_log_tablename']}表上一次导入的最新时间，从而计算需要从现场导出时间的脚本""")
querySqlfor = f"""select * from {envlist['kpi_mt_step_log_tablename']} where start_time!='' order by start_time desc"""
equerryReuslts = o.query(querySqlfor,t='df')
if isinstance(equerryReuslts,pd.DataFrame):
    print(f"""上次从从现场拷贝的{envlist['kpi_mt_step_log_tablename']}表的数据start_time最后时间为{equerryReuslts.iloc[0]['start_time']}""")
    print(f"""本次需要从现场拷贝KPIDB的{envlist['kpi_mt_step_log_tablename']}表的数据start_time应该从上次时间后面提取，相关sql脚本如下：""")
    selectSqlForkpimtsteplog= f"""select * from {envlist['kpi_mt_step_log_tablename']} where start_time>'{equerryReuslts.iloc[0]['start_time']}' order by start_time asc;"""
    print(selectSqlForkpimtsteplog)
else:
    print(f"""需要将{envlist['kpi_mt_step_log_tablename']}表数据全部导出""")
print('*************************')



# n =n+1
# print(f"""步骤{n}:计算{envlist['kpi_mt_state_log_tablename']}表上一次导入的最新时间，从而计算需要从现场导出时间的脚本""")
# querySqlfor = f"""select * from {envlist['kpi_mt_state_log_tablename']} where start_time!='' order by start_time desc"""
# equerryReuslts = o.query(querySqlfor,t='df')
# if isinstance(equerryReuslts,pd.DataFrame):
#     print(f"""上次从从现场拷贝的{envlist['kpi_mt_state_log_tablename']}表的数据start_time最后时间为{equerryReuslts.iloc[0]['start_time']}""")
#     print(f"""本次需要从现场拷贝KPIDB的{envlist['kpi_mt_state_log_tablename']}表的数据start_time应该从上次时间后面提取，相关sql脚本如下：""")
#     selectSqlForkpimtstatelog= f"""select * from {envlist['kpi_mt_state_log_tablename']} where start_time>'{equerryReuslts.iloc[0]['start_time']}' order by start_time asc;"""
#     print(selectSqlForkpimtstatelog)
# else:
#     print(f"""需要将{envlist['kpi_mt_state_log_tablename']}表数据全部导出""")
# print('*************************')


print(selectSqlForQcTosTask)
print(selectSqlForqctostaskhis)
print(selectSqlForqcgantryinstruction)
print(selectSqlForqctrolleyinstruction)
print(selectSqlForqctrolleytask)
print(selectSqlForqccontainertransfer)
print(selectSqlForqctpinteractionhis)
print(selectSqlForqceventrecorderhis)
print(selectSqlForkpimtsteplog)
# print(selectSqlForkpimtstatelog)

print('*************************')


n =n+1
print(f"""步骤{n}:需要导出现场设备OPCUA的数据，请先登录swagger地址：http://10.11.6.25:30082/swagger/index.html找到多点查询""")
querySqlfor = f"""select * from {envlist['mtworkmode_tablename']} where Timestamp!='' order by Timestamp desc"""
equerryReuslts = o.query(querySqlfor,t='df')
if isinstance(equerryReuslts,pd.DataFrame):
    print(f"""上次从从现场拷贝的{envlist['mtworkmode_tablename']}的数据Timestamp最后时间为{equerryReuslts.iloc[0]['Timestamp']}""")
    print(f"""本次需要从现场拷贝OPCUA的{envlist['mtworkmode_tablename']}的数据Timestamp应该从上次时间后面提取，相关sql脚本如下：""")
    selectSqlForNewData= f"""本次需要从OPCUA拉取的数据开始时间{equerryReuslts.iloc[0]['Timestamp']}，结束时间=当前时间,需要换算为云平台识别的格式，开始时间={equerryReuslts.iloc[0]['Timestamp'].replace(' ','T')+'Z'},结束时间={datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(' ','T')+'Z'}"""
    strformatforquery ='''[
  {
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MtWorkMode",
    "machineryName": "103",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MtWorkMode",
    "machineryName": "104",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MtWorkMode",
    "machineryName": "105",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MtWorkMode",
    "machineryName": "106",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MtWorkMode",
    "machineryName": "107",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MtWorkMode",
    "machineryName": "108",
    "portName": "Peru"
  }
]'''
    print(selectSqlForNewData)
    print(strformatforquery)
else:
    print(f"""需要将{envlist['mtworkmode_tablename']}OPCUA数据全部导出""")
print('*************************')


n =n+1
print(f"""步骤{n}:需要导出现场设备OPCUA的数据，请先登录swagger地址：http://10.11.6.25:30082/swagger/index.html找到多点查询""")
querySqlfor = f"""select * from {envlist['mtinstructionstatus_tablename']} where Timestamp!='' order by Timestamp desc"""
equerryReuslts = o.query(querySqlfor,t='df')
if isinstance(equerryReuslts,pd.DataFrame):
    print(f"""上次从从现场拷贝的{envlist['mtinstructionstatus_tablename']}的数据Timestamp最后时间为{equerryReuslts.iloc[0]['Timestamp']}""")
    print(f"""本次需要从现场拷贝OPCUA的{envlist['mtinstructionstatus_tablename']}的数据Timestamp应该从上次时间后面提取，相关sql脚本如下：""")
    selectSqlForNewData= f"""本次需要从OPCUA拉取的数据开始时间{equerryReuslts.iloc[0]['Timestamp']}，结束时间=当前时间,需要换算为云平台识别的格式，开始时间={equerryReuslts.iloc[0]['Timestamp'].replace(' ','T')+'Z'},结束时间={datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(' ','T')+'Z'}"""
    strformatforquery ='''[
  {
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MtInstructionStatus",
    "machineryName": "103",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MtInstructionStatus",
    "machineryName": "104",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MtInstructionStatus",
    "machineryName": "105",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MtInstructionStatus",
    "machineryName": "106",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MtInstructionStatus",
    "machineryName": "107",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MtInstructionStatus",
    "machineryName": "108",
    "portName": "Peru"
  }
]'''
    print(selectSqlForNewData)
    print(strformatforquery)
else:
    print(f"""需要将{envlist['mtinstructionstatus_tablename']}OPCUA数据全部导出""")
print('*************************')


n =n+1
print(f"""步骤{n}:需要导出现场设备OPCUA的数据，请先登录swagger地址：http://10.11.6.25:30082/swagger/index.html找到多点查询""")
querySqlfor = f"""select * from {envlist['mhabovesafeheight_tablename']} where Timestamp!='' order by Timestamp desc"""
equerryReuslts = o.query(querySqlfor,t='df')
if isinstance(equerryReuslts,pd.DataFrame):
    print(f"""上次从从现场拷贝的{envlist['mhabovesafeheight_tablename']}的数据Timestamp最后时间为{equerryReuslts.iloc[0]['Timestamp']}""")
    print(f"""本次需要从现场拷贝OPCUA的{envlist['mhabovesafeheight_tablename']}的数据Timestamp应该从上次时间后面提取，相关sql脚本如下：""")
    selectSqlForNewData= f"""本次需要从OPCUA拉取的数据开始时间{equerryReuslts.iloc[0]['Timestamp']}，结束时间=当前时间,需要换算为云平台识别的格式，开始时间={equerryReuslts.iloc[0]['Timestamp'].replace(' ','T')+'Z'},结束时间={datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(' ','T')+'Z'}"""
    strformatforquery ='''[
  {
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MhAboveSafeHeight",
    "machineryName": "103",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MhAboveSafeHeight",
    "machineryName": "104",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MhAboveSafeHeight",
    "machineryName": "105",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MhAboveSafeHeight",
    "machineryName": "106",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MhAboveSafeHeight",
    "machineryName": "107",
    "portName": "Peru"
  },
{
    "itemName": "QCMS_ACCS.PLC_OPC_UI.MhAboveSafeHeight",
    "machineryName": "108",
    "portName": "Peru"
  }
]'''
    print(selectSqlForNewData)
    print(strformatforquery)
else:
    print(f"""需要将{envlist['mhabovesafeheight_tablename']}OPCUA数据全部导出""")
print('*************************')