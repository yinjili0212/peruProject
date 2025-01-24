
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqliteHandle
from basicFunctionDefine import *
import ast
import datetime
import os
import yaml
import json
#########
'''
1.根据kpi_for_qcms的数据画对应的图，任务执行时间，指令执行时间，单机KPI记录的时间
'''
#########
envlist = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')


o=sqliteHandle.sqliteHandler(envlist['sqllitedbaddress'])


#设置需要从目标表的表读取数据
kpi_for_qcms=envlist['kpi_for_qcms_tablename']
kpi_mt_step_log=envlist['kpi_mt_step_log_tablename']
kpi_mt_state_log=envlist['kpi_mt_state_log_tablename']
qc_event_define=envlist['qc_event_define_tablename']

stsids = ['103','104','105','106','107','108']
# stsids = ['104']
# 对图形的颜色预先定义
color_for_gantt_orange1 = '#FFA500'  # 橙色1
color_for_gantt_red = '#FF0000'  # 大红
color_for_gantt_rosered = '#FF00FF'  # 玫红色
color_for_gantt_yellow = '#FFFF00'  # 黄色
color_for_gantt_green = '#00FF00'  # 绿色
color_for_gantt_darkblue = '#0000FF'  # 深蓝色
color_for_gantt_skyblue = '#00FFFF'  # 天蓝色
color_for_gantt_orange2 = '#FFD12D'  # 橘色
color_for_gantt_black = '#000000'  # 黑色
color_for_gantt_purple1 = '#C6A3A7'  # 紫色1
color_for_gantt_purple2 = '#A19AAC'  # 紫色2
color_for_gantt_orange3 = '#EDDBAD'  # 橙色3
color_for_gantt_black1 = '#293B13'  # 黑色2
color_for_gantt_blue = '#308E98'  # 蓝色
color_for_gantt_purple3 = '#612B4B'  # 紫色3
color_for_gantt_green1 = '#7CDB05'  # 绿色2
color_for_gantt_purple = '#5407A3'  # 紫色
color_for_simple_green = '#728C79'  # 浅绿色

# 设置y方向上，每个层级对应的高度,画线的高度设置
min_height = 0  # 从0开始
max_height = 50  # 100结束

################
"""
1.1~5:QCMS任务范围的画图
2.5~10：QCMS发送过的大车/小车的大车移动,PARK,PICKUP,GROUND指令的区域
3.10~15：QCMS的workmode:1=Normal Mode;2=Maintenance Mode;3=Local Mode
4.15~20:QCMS的InstructionStatus:1=Ready for ECS;2=Not ready for ECS
5.20~25:QCMS的AboveSafeHeight:1=Above;2=Below
6.25~30:单机KPI的数据
7.30~35:container_transfer画表
"""
################
###QCMS整个任务完成的时间高度区域（OK）
qcms_min = 1
qcms_max = 5

###QCMS各阶段任务完成的时间高度区域（OK）
qcmstask_min = 5
qcmstask_max = 10

###QCMS_event_recorder_his
waitevent_min = 10
waitevent_max = 15

###QCMS工作模式高度区域
mtWorkMode_min = 15
mtWorkMode_max = 20
###QCMS指令状态
mtInstructionStatus_min = 20
mtInstructionStatus_max = 25
###QCMS工作模式高度区域
mhAboveSafeHeight_min = 25
mhAboveSafeHeight_max = 30

###KPI各阶段任务完成的时间高度区域（OK）
kpistep_min = 30
kpistep_max = 35

###KPI各阶段任务完成的时间高度区域（OK）
kpistate_min = 35
kpistate_max = 40

###containertransfermin和max
containertransfer_min = 40
containertransfer_max = 45

querySqlForQcEventDefine = f""f'select * from {qc_event_define}'
qcEventDefineQueryResults = o.query(querySqlForQcEventDefine, t='df')
if isinstance(qcEventDefineQueryResults, pd.DataFrame):
    # 设置eventdefine初始
    dictForeventdefine = {}
    for index, qcEventDefineQueryResult in qcEventDefineQueryResults.iterrows():
        dictForeventdefine[qcEventDefineQueryResult['CODE']] = \
            json.loads(qcEventDefineQueryResult['REMARK'])['en-US']
    # 得到eventdefine初始定义
    # print(dictForeventdefine)



#查询之前去掉字段KEYTIME是空的时间
o.executesql(f"delete from {kpi_for_qcms} where KEYTIME='' or KEYTIME='0001-01-01 00:00:00'")

# ##########步骤1：查询kpi_for_qcms的最小时间和最大时间
minTime=''
maxTime=''
querySqlForQcTosTasks = f"""select * from {envlist['qc_tos_task_his_tablename']}  where VBT_ID={envlist['VBT_ID']} and RESPONSE_TIME!='' order by RESPONSE_TIME asc"""
QcTosTaskQueryResults = o.query(querySqlForQcTosTasks)
if len(QcTosTaskQueryResults) >= 2:  # 如果检测到查询出来的数据大于=2条，才可
    minTime = QcTosTaskQueryResults[0]['RESPONSE_TIME']  # 这条船的当前岸桥的最小时间,str类型
    maxTime = QcTosTaskQueryResults[-1]['RESPONSE_TIME']  # 这条船的当前岸桥的最大时间str类型


##########步骤1：查询kpi_for_qcms的最小时间和最大时间
#步骤2:在规定时间内画是散点图################################
if minTime!='' and maxTime!='':#最大和最小时间均能找到
    # print(minTime,maxTime)
    for stsid in stsids:#遍历岸桥编号
        # wholeHourTimes=wholeHourTime3Ends(minTime,maxTime)
        wholeHourTimes = wholeHourTimeEnds(minTime, maxTime)
        if wholeHourTimes!=[]:#能查询出整点时间
            for wholeHourTimeIndex,wholeHourTime in enumerate(wholeHourTimes):
                # #创建要画的图
                fig = make_subplots(cols=1, rows=1, subplot_titles='')  # cols表示设备数量，rows表示最大任务数量
                if wholeHourTimeIndex==0:#查询出第一个整点时间
                    querysqlforstsno_task = f"SELECT * FROM {kpi_for_qcms} WHERE STS_NO = '{stsid}' and KEYTIME<='{wholeHourTime}' and KEYTIME>='{minTime}' order by KEYTIME asc"
                else:#查询出第一个整点时间
                    querysqlforstsno_task = f"SELECT * FROM {kpi_for_qcms} WHERE STS_NO = '{stsid}' and (KEYTIME>='{wholeHourTimes[wholeHourTimeIndex-1]}' and KEYTIME<='{wholeHourTime}') order by KEYTIME asc"
                print(querysqlforstsno_task)

                filtered_dfs=o.query(querysqlforstsno_task,t='df')#整点时间内的数据
                if isinstance(filtered_dfs,pd.DataFrame):#能查询出来数据
                    filtered_dfs['KEYTIME'] = pd.to_datetime(filtered_dfs['KEYTIME'], format='mixed')  # 利用pandas将数据转为时间格式
                    # filtered_dfs['KEYTIME'] = pd.to_datetime(filtered_dfs['KEYTIME'])  # 利用pandas将数据转为时间格式
                    for idx, row in filtered_dfs.iterrows():  # forstsno_taskQueryResults.iterrows对查询出来的任务画图
                        #创建散点图，将每个时间节点都打印出来
                        if row['DATA_FROM_TYPE']=='QCMS' or row['DATA_FROM_TYPE']=='OPCUA':
                            corlorline= color_for_simple_green
                        elif row['DATA_FROM_TYPE']=='KPI':
                            corlorline = color_for_gantt_orange1
                        else:
                            corlorline=color_for_gantt_black

                        if row['DATA_FROM_TYPE'] not in ('KPI','OPCUA'):#屏蔽掉KPI的散点图
                            fig.add_trace(go.Scatter(x=[row['KEYTIME'],row['KEYTIME']],y=[min_height,max_height],
                                                     mode="lines",#指定绘制模式。常用的值包括 'markers'（仅绘制散点）、'lines'（绘制线条，连接数据点）、'lines+markers'（同时绘制线条和散点）等
                                                     name=str(row['STS_NO'])+'设备'+str(row['KEYTIME'])+str(row['NOTES']),#为散点图轨迹指定名称，该名称将显示在图例中
                                                     marker=dict(
                                                         color=[corlorline,corlorline],# 设置散点颜色
                                                         size=[5,5],# 设置散点大小
                                                         symbol=['square','square']# 设置散点形状['circle', 'square', 'diamond', 'cross', 'x']  # 每个散点的形状
                                                     ),#marker：一个字典或 Marker 对象，用于自定义散点的样式（如颜色、大小、形状等）
                                                     line=dict(
                                                         color=corlorline,        # 线的颜色设置为蓝色
                                                         width=2,             # 线的宽度
                                                         dash='solid'         # 线的样式（solid, dash虚线, dot点线, dashdot点划线）
                                                        ),#line：一个字典或 Line 对象，用于自定义线条的样式（如颜色、宽度、虚线模式等）。当 mode 包含 'lines' 时使用。
                                                     text=row['NOTES']),
                                          row=1,#任务的index
                                          col=1)



                    ###########################################################创建QCMS任务执行时间的

                    ###############步骤1：画出QCMS整个任务持续时间的填充图，以“INSERT为计算节点”
                    # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.qc_tos_task.RESPONSE_TIME'的行作为整个任务的起点
                    matching_rows_for_qctaskstart = filtered_dfs[
                        filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_tos_task_his.TRIG_CREATED.INSERT']
                    # 从匹配的行中提取索引值
                    qctaskStart_Qcmsindexs = matching_rows_for_qctaskstart.index.tolist()#[1,3]查询出来的是个列表]
                    print(qctaskStart_Qcmsindexs)
                    for taskstartindex in qctaskStart_Qcmsindexs:#查询QCMS插入时间的所有值
                        qctaskstart = filtered_dfs.iloc[taskstartindex]  # 开始的某一行数据

                        ###################判断有无删除过数据
                        # 查找任务中有无删除过数据
                        matching_rows_for_qctaskends = filtered_dfs[
                            (filtered_dfs['PAIRED_VALUE'] == qctaskstart['PAIRED_VALUE']) &
                            (filtered_dfs['KEYTIME'] > qctaskstart['KEYTIME']) &
                            (filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_tos_task_his.TRIGGER_ACTION(DELETE).TRIG_CREATED')]
                        # 判断查询出来的数据是否为空
                        # 从匹配的行中提取索引值
                        qcdeleteStart_Qcmsindexs = matching_rows_for_qctaskends.index.tolist()  # [1,3]查询出来的是个列表
                        print(qcdeleteStart_Qcmsindexs)
                        if qcdeleteStart_Qcmsindexs!=[]:#表示delete数据能查到，则找第一条数据，画出图
                            qctaskdelete = filtered_dfs.iloc[qcdeleteStart_Qcmsindexs[0]]  # 结束的某一行数据

                            #计算持续时间
                            durationforqctask=qctaskdelete['KEYTIME']-qctaskstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask=f"""QCMS删除过的任务{qctaskdelete['TASK_ID']}信息，持续时间{durationforqctask},任务类型:{qctaskdelete['TASK_TYPE']},任务状态:{qctaskdelete['TASK_STATUS']},任务初始位置:{qctaskdelete['ORIG_WSLOC']},任务目的位置:{qctaskdelete['DEST_WS_LOC']}"""
                            fig.add_trace(go.Scatter(x=[qctaskstart['KEYTIME'],qctaskstart['KEYTIME'],qctaskdelete['KEYTIME'],qctaskdelete['KEYTIME'],qctaskstart['KEYTIME']],y=[qcms_min,qcms_max,qcms_max,qcms_min,qcms_min],fill='toself',fillcolor=color_for_gantt_red,line=dict(color='black'),name=namefortask),row=1,col=1)

                            fig.add_annotation(
                                dict(x=qctaskstart['KEYTIME'] + (qctaskdelete['KEYTIME'] - qctaskstart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                     y=(qcms_min+qcms_max)/2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                        elif qcdeleteStart_Qcmsindexs==[]:#表示没有找到删除过的信息，则以放箱结束时间为画任务持续图
                            # 查找任务中有无删除过数据，则需要判断当前放箱结束时间的时间
                            matching_rows_for_qctaskends = filtered_dfs[
                                (filtered_dfs['KEYTIME'] > qctaskstart['KEYTIME']) &
                                ((filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_trolley_instruction.Ground.END_TIME')|
                                 (filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_tos_task.UNLOCK_TIME'))]
                            qcend_Qcmsindexs = matching_rows_for_qctaskends.index.tolist()  # [1,3]查询出来的是个列表
                            if qcend_Qcmsindexs != []:  # 表示Ground数据能查到，则找第一条数据，画出图
                                qctaskend = filtered_dfs.iloc[qcend_Qcmsindexs[0]]  # 开始的某一行数据


                                # 计算持续时间
                                durationforqctask = qctaskend['KEYTIME'] - qctaskstart['KEYTIME']
                                durationforqctask = timeChange(durationforqctask)
                                namefortask = f"""QCMS整个任务{qctaskstart['TASK_ID']}持续的时间{durationforqctask}，任务类型:{qctaskstart['TASK_TYPE']},任务状态:{qctaskstart['TASK_STATUS']},任务初始位置:{qctaskstart['ORIG_WSLOC']},任务目的位置:{qctaskstart['DEST_WS_LOC']}"""
                                fig.add_trace(go.Scatter(
                                    x=[qctaskstart['KEYTIME'], qctaskstart['KEYTIME'], qctaskend['KEYTIME'],
                                       qctaskend['KEYTIME'], qctaskstart['KEYTIME']],
                                    y=[qcms_min, qcms_max, qcms_max, qcms_min, qcms_min], fill='toself',
                                    fillcolor=color_for_simple_green, line=dict(color='black'), name=namefortask), row=1, col=1)
                                fig.add_annotation(
                                    dict(x=qctaskstart['KEYTIME'] + (qctaskend['KEYTIME'] - qctaskstart['KEYTIME']) / 2,
                                         # x 位置为两个 KEYTIME 的中点
                                         y=(qcms_min + qcms_max) / 2,  # y 位置为矩形高度的一半
                                         text=f'{durationforqctask}',
                                         showarrow=False, xref="x1", yref="y1"))
                            else:#Ground时间也找不到，则以整点最后时间为结束画任务图
                                # 计算持续时间
                                durationforqctask = strChangeTime(wholeHourTime) - qctaskstart['KEYTIME']
                                durationforqctask = timeChange(durationforqctask)
                                namefortask = f"""QCMS部分任务{qctaskstart['TASK_ID']}持续的时间{durationforqctask}，任务类型:{qctaskstart['TASK_TYPE']},任务状态:{qctaskstart['TASK_STATUS']},任务初始位置:{qctaskstart['ORIG_WSLOC']},任务目的位置:{qctaskstart['DEST_WS_LOC']}"""
                                fig.add_trace(go.Scatter(
                                    x=[qctaskstart['KEYTIME'], qctaskstart['KEYTIME'], strChangeTime(wholeHourTime),
                                       strChangeTime(wholeHourTime), qctaskstart['KEYTIME']],
                                    y=[qcms_min, qcms_max, qcms_max, qcms_min, qcms_min], fill='toself',
                                    fillcolor=color_for_simple_green, line=dict(color='black'), name=namefortask),
                                    row=1, col=1)
                                fig.add_annotation(
                                    dict(x=qctaskstart['KEYTIME'] + (strChangeTime(wholeHourTime) - qctaskstart['KEYTIME']) / 2,
                                         # x 位置为两个 KEYTIME 的中点
                                         y=(qcms_min + qcms_max) / 2,  # y 位置为矩形高度的一半
                                         text=f'{durationforqctask}',
                                         showarrow=False, xref="x1", yref="y1"))
                    ###############步骤1：画出QCMS整个任务持续时间的填充图，以“INSERT为计算节点”


                    ###############步骤2：查询单个整点时间内的任务结束时间，初始时间以wholeHourTime-1为开始
                    matching_rows_for_qctaskendsforwholehour = filtered_dfs[
                        (filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_tos_task_his.TRIGGER_ACTION(DELETE).TRIG_CREATED')|
                        ((filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_trolley_instruction.Ground.END_TIME'))]
                    # 从匹配的行中提取索引值
                    qctaskendforwholehour_indexs = matching_rows_for_qctaskendsforwholehour.index.tolist()  # [1,3]查询出来的是个列表
                    if qctaskendforwholehour_indexs!=[]:#查询出来不为空，则取第一条数据作为当前整点时间内第一个结束
                        qctaskend = filtered_dfs.iloc[qctaskendforwholehour_indexs[0]]  # 查询到结束的的某一行数据

                        #查询前面有无对应的插入时间
                        qctaskstartsforwhole = filtered_dfs[(filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_tos_task_his.TRIG_CREATED.INSERT')&
                                                            (filtered_dfs['KEYTIME']<qctaskend['KEYTIME'])]                        # 从匹配的行中提取索引值
                        qctaskstartsforwhole_indexs = qctaskstartsforwhole.index.tolist()  # [1,3]查询出来的是个列表]

                        if qctaskstartsforwhole_indexs==[]:#表示第一条删除或者ground的指令没有对应的开始时间，则以wholehouretime-1小时为开始时间
                            durationforqctask = qctaskend['KEYTIME']-(strChangeTime(wholeHourTime)-timedelta(hours=1))
                            durationforqctask = timeChange(durationforqctask)
                            namefortask = f"""QCMS部分任务{qctaskend['TASK_ID']}持续的时间{durationforqctask}，任务类型:{qctaskend['TASK_TYPE']},任务状态:{qctaskend['TASK_STATUS']},任务初始位置:{qctaskend['ORIG_WSLOC']},任务目的位置:{qctaskend['DEST_WS_LOC']}"""
                            fig.add_trace(go.Scatter(
                                x=[strChangeTime(wholeHourTime)-timedelta(hours=1),strChangeTime(wholeHourTime)-timedelta(hours=1), qctaskend['KEYTIME'],qctaskend['KEYTIME'], strChangeTime(wholeHourTime)-timedelta(hours=1)],y=[qcms_min, qcms_max, qcms_max, qcms_min, qcms_min], fill='toself',
                                fillcolor=color_for_simple_green, line=dict(color='black'), name=namefortask),
                                row=1, col=1)
                            fig.add_annotation(
                                dict(x=strChangeTime(wholeHourTime)-timedelta(hours=1) + (
                                            qctaskend['KEYTIME'] - (strChangeTime(wholeHourTime)-timedelta(hours=1))) / 2,
                                     # x 位置为两个 KEYTIME 的中点
                                     y=(qcms_min + qcms_max) / 2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                        ###############步骤2：查询单个整点时间内的任务结束时间，初始时间以wholeHourTime-1为开始


                    ############################################################创建QCMS任务执行时间的#







                    ###############画出QCMS发送的大车移动指令的填充图
                    # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                    # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.qc_gantry_instruction.START_TIME'的行
                    matching_rows_for_gantrystart = filtered_dfs[filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_gantry_instruction.START_TIME']
                    # 从匹配的行中提取索引值
                    gantryStart_Qcmsindexs = matching_rows_for_gantrystart.index.tolist()#[1,3]查询出来的是个列表]


                    for gantrystartindex in gantryStart_Qcmsindexs:#查询QCMSgantry指令发送的所有值
                        qcgantrystart = filtered_dfs.iloc[gantrystartindex]  # 开始的某一行数据
                        ###################判断有无对应的end数据
                        matching_rows_for_gantryends = filtered_dfs[
                            (filtered_dfs['PAIRED_VALUE'] == qcgantrystart['PAIRED_VALUE']) &
                            (filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_gantry_instruction.END_TIME')]
                        # 判断查询出来的数据是否为空
                        # 从匹配的行中提取索引值
                        qcgantryend_Qcmsindexs = matching_rows_for_gantryends.index.tolist()  # [1,3]查询出来的是个列表
                        if qcgantryend_Qcmsindexs!=[]:#表示end数据能查到，则找第一条数据，画出图
                            qcgantryend = filtered_dfs.iloc[qcgantryend_Qcmsindexs[0]]  # 结束的某一行数据


                            #计算持续时间
                            durationforqctask=qcgantryend['KEYTIME']-qcgantrystart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask=f"""QCMS发送大车移动指令到完成的时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(x=[qcgantrystart['KEYTIME'],qcgantrystart['KEYTIME'],qcgantryend['KEYTIME'],qcgantryend['KEYTIME'],qcgantrystart['KEYTIME']],y=[qcmstask_min,qcmstask_max,qcmstask_max,qcmstask_min,qcmstask_min],fill='toself',fillcolor=color_for_gantt_red,line=dict(color='black'),name=namefortask),row=1,col=1)

                            fig.add_annotation(
                                dict(x=qcgantrystart['KEYTIME'] + (qcgantryend['KEYTIME'] - qcgantrystart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                     y=(qcmstask_min+qcmstask_max)/2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                        else:#表示大车结束位置找不到，则以整点最后一个时间节点画图
                            # 计算持续时间
                            durationforqctask = strChangeTime(wholeHourTime) - qcgantrystart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask = f"""QCMS发送大车移动指令到完成的时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(
                                x=[qcgantrystart['KEYTIME'], qcgantrystart['KEYTIME'], strChangeTime(wholeHourTime),
                                   strChangeTime(wholeHourTime), qcgantrystart['KEYTIME']],
                                y=[qcmstask_min, qcmstask_max, qcmstask_max, qcmstask_min, qcmstask_min], fill='toself',
                                fillcolor=color_for_gantt_red, line=dict(color='black'), name=namefortask), row=1,
                                          col=1)

                            fig.add_annotation(
                                dict(x=qcgantrystart['KEYTIME'] + (
                                            strChangeTime(wholeHourTime) - qcgantrystart['KEYTIME']) / 2,
                                     # x 位置为两个 KEYTIME 的中点
                                     y=(qcmstask_min + qcmstask_max) / 2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                    ###############画出QCMS发送的gantry指令的填充图



                    ###############画出QCMS发送的park指令的填充图
                    # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                    # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.qc_trolley_instruction.Park.START_TIME'的行
                    matching_rows_for_parkstart = filtered_dfs[filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_trolley_instruction.Park.START_TIME']
                    # 从匹配的行中提取索引值
                    parkStart_Qcmsindexs = matching_rows_for_parkstart.index.tolist()#[1,3]查询出来的是个列表]
                    # print(parkStart_Qcmsindexs)
                    # print(filtered_dfs['DATA_FROM'])

                    for parkstartindex in parkStart_Qcmsindexs:#查询QCMSpark指令发送的所有值
                        qcparkstart = filtered_dfs.iloc[parkstartindex]  # 开始的某一行数据

                        ###################判断有无对应的end数据
                        matching_rows_for_parkends = filtered_dfs[
                            (filtered_dfs['PAIRED_VALUE'] == qcparkstart['PAIRED_VALUE']) &
                            (filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_trolley_instruction.Park.END_TIME')]
                        # 判断查询出来的数据是否为空
                        # 从匹配的行中提取索引值
                        qcparkend_Qcmsindexs = matching_rows_for_parkends.index.tolist()  # [1,3]查询出来的是个列表
                        if qcparkend_Qcmsindexs!=[]:#表示end数据能查到，则找第一条数据，画出图
                            qcparkend = filtered_dfs.iloc[qcparkend_Qcmsindexs[0]]  # 结束的某一行数据

                            #计算持续时间
                            durationforqctask=qcparkend['KEYTIME']-qcparkstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask=f"""QCMS发送Park指令到完成的时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(x=[qcparkstart['KEYTIME'],qcparkstart['KEYTIME'],qcparkend['KEYTIME'],qcparkend['KEYTIME'],qcparkstart['KEYTIME']],y=[qcmstask_min,qcmstask_max,qcmstask_max,qcmstask_min,qcmstask_min],fill='toself',fillcolor=color_for_simple_green,line=dict(color='black'),name=namefortask),row=1,col=1)

                            fig.add_annotation(
                                dict(x=qcparkstart['KEYTIME'] + (qcparkend['KEYTIME'] - qcparkstart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                     y=(qcmstask_min+qcmstask_max)/2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                        else:#表示找不到则以整点最后时间数据为节点画持续图
                            # 计算持续时间
                            durationforqctask = strChangeTime(wholeHourTime) - qcparkstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask = f"""QCMS发送Park指令到完成的时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(
                                x=[qcparkstart['KEYTIME'], qcparkstart['KEYTIME'], strChangeTime(wholeHourTime),
                                   strChangeTime(wholeHourTime), qcparkstart['KEYTIME']],
                                y=[qcmstask_min, qcmstask_max, qcmstask_max, qcmstask_min, qcmstask_min], fill='toself',
                                fillcolor=color_for_simple_green, line=dict(color='black'), name=namefortask), row=1,
                                          col=1)

                            fig.add_annotation(
                                dict(x=qcparkstart['KEYTIME'] + (strChangeTime(wholeHourTime) - qcparkstart['KEYTIME']) / 2,
                                     # x 位置为两个 KEYTIME 的中点
                                     y=(qcmstask_min + qcmstask_max) / 2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                    ###############画出QCMS发送的park指令的填充图







                    ###############画出QCMS发送的Pickup指令的填充图
                    # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                    # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.qc_trolley_instruction.Pickup.START_TIME'的行
                    matching_rows_for_pickupstart = filtered_dfs[filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_trolley_instruction.Pickup.START_TIME']
                    # 从匹配的行中提取索引值
                    pickupStart_Qcmsindexs = matching_rows_for_pickupstart.index.tolist()#[1,3]查询出来的是个列表]


                    for pickupstartindex in pickupStart_Qcmsindexs:#查询QCMSpickup指令发送的所有值
                        qcpickupstart = filtered_dfs.iloc[pickupstartindex]  # 开始的某一行数据

                        ###################判断有无对应的end数据
                        matching_rows_for_pickupends = filtered_dfs[
                            (filtered_dfs['PAIRED_VALUE'] == qcpickupstart['PAIRED_VALUE']) &
                            (filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_trolley_instruction.Pickup.END_TIME')]
                        # 判断查询出来的数据是否为空
                        # 从匹配的行中提取索引值
                        qcpickupend_Qcmsindexs = matching_rows_for_pickupends.index.tolist()  # [1,3]查询出来的是个列表
                        if qcpickupend_Qcmsindexs!=[]:#表示end数据能查到，则找第一条数据，画出图
                            qcpickupend = filtered_dfs.iloc[qcpickupend_Qcmsindexs[0]]  # 结束的某一行数据


                            #计算持续时间
                            durationforqctask=qcpickupend['KEYTIME']-qcpickupstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask=f"""QCMS发送Pickup指令到完成的时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(x=[qcpickupstart['KEYTIME'],qcpickupstart['KEYTIME'],qcpickupend['KEYTIME'],qcpickupend['KEYTIME'],qcpickupstart['KEYTIME']],y=[qcmstask_min,qcmstask_max,qcmstask_max,qcmstask_min,qcmstask_min],fill='toself',fillcolor=color_for_simple_green,line=dict(color='black'),name=namefortask),row=1,col=1)

                            fig.add_annotation(
                                dict(x=qcpickupstart['KEYTIME'] + (qcpickupend['KEYTIME'] - qcpickupstart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                     y=(qcmstask_min+qcmstask_max)/2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                        else:  # 表示end数据找不到，则以整点结束时间为最后时间画图
                            # 计算持续时间
                            durationforqctask = strChangeTime(wholeHourTime) - qcpickupstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask = f"""QCMS发送Pickup指令到完成的时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(
                                x=[qcpickupstart['KEYTIME'], qcpickupstart['KEYTIME'], strChangeTime(wholeHourTime),
                                   strChangeTime(wholeHourTime), qcpickupstart['KEYTIME']],
                                y=[qcmstask_min, qcmstask_max, qcmstask_max, qcmstask_min, qcmstask_min], fill='toself',
                                fillcolor=color_for_simple_green, line=dict(color='black'), name=namefortask), row=1,
                                          col=1)

                            fig.add_annotation(
                                dict(x=qcpickupstart['KEYTIME'] + (
                                            strChangeTime(wholeHourTime) - qcpickupstart['KEYTIME']) / 2,
                                     # x 位置为两个 KEYTIME 的中点
                                     y=(qcmstask_min + qcmstask_max) / 2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                    ###############画出QCMS发送的pickup指令的填充图



                    ###############画出QCMS发送的Ground指令的填充图
                    # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                    # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.qc_trolley_instruction.Ground.START_TIME'的行
                    matching_rows_for_groundstart = filtered_dfs[filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_trolley_instruction.Ground.START_TIME']
                    # 从匹配的行中提取索引值
                    groundStart_Qcmsindexs = matching_rows_for_groundstart.index.tolist()#[1,3]查询出来的是个列表]


                    for groundstartindex in groundStart_Qcmsindexs:#查询QCMSground指令发送的所有值
                        qcgroundstart = filtered_dfs.iloc[groundstartindex]  # 开始的某一行数据

                        ###################判断有无对应的end数据
                        matching_rows_for_groundends = filtered_dfs[
                            (filtered_dfs['PAIRED_VALUE'] == qcgroundstart['PAIRED_VALUE']) &
                            (filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_trolley_instruction.Ground.END_TIME')]
                        # 判断查询出来的数据是否为空
                        # 从匹配的行中提取索引值
                        qcgroundend_Qcmsindexs = matching_rows_for_groundends.index.tolist()  # [1,3]查询出来的是个列表
                        if qcgroundend_Qcmsindexs!=[]:#表示end数据能查到，则找第一条数据，画出图
                            qcgroundend = filtered_dfs.iloc[qcgroundend_Qcmsindexs[0]]  # 结束的某一行数据


                            #计算持续时间
                            durationforqctask=qcgroundend['KEYTIME']-qcgroundstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask=f"""QCMS发送Ground指令到完成的时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(x=[qcgroundstart['KEYTIME'],qcgroundstart['KEYTIME'],qcgroundend['KEYTIME'],qcgroundend['KEYTIME'],qcgroundstart['KEYTIME']],y=[qcmstask_min,qcmstask_max,qcmstask_max,qcmstask_min,qcmstask_min],fill='toself',fillcolor=color_for_simple_green,line=dict(color='black'),name=namefortask),row=1,col=1)

                            fig.add_annotation(
                                dict(x=qcgroundstart['KEYTIME'] + (qcgroundend['KEYTIME'] - qcgroundstart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                     y=(qcmstask_min+qcmstask_max)/2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                        else:  # 表示end数据找不到，则以整点结束时间为最后时间画图
                            # 计算持续时间
                            durationforqctask = strChangeTime(wholeHourTime) - qcgroundstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask = f"""QCMS发送Ground指令到完成的时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(
                                x=[qcgroundstart['KEYTIME'], qcgroundstart['KEYTIME'], strChangeTime(wholeHourTime),
                                   strChangeTime(wholeHourTime), qcgroundstart['KEYTIME']],
                                y=[qcmstask_min, qcmstask_max, qcmstask_max, qcmstask_min, qcmstask_min], fill='toself',
                                fillcolor=color_for_simple_green, line=dict(color='black'), name=namefortask), row=1,
                                          col=1)

                            fig.add_annotation(
                                dict(x=qcgroundstart['KEYTIME'] + (
                                            strChangeTime(wholeHourTime) - qcgroundstart['KEYTIME']) / 2,
                                     # x 位置为两个 KEYTIME 的中点
                                     y=(qcmstask_min + qcmstask_max) / 2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                    ###############画出QCMS发送的ground指令的填充图





                    ###############画出QCMS发送的event指令的填充图,
                    ###步骤2：处理第2行到第N行的数据，第一行的数据应该单独处理
                    # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                    # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.qc_event_recorder_his.CREATE_TIME'的行
                    matching_rows_for_qcmseventstart = filtered_dfs[(filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_event_recorder_his.CREATE_TIME')]
                    matching_rows_for_qcmseventstart=matching_rows_for_qcmseventstart.sort_values(by='ID', ascending=True)#对字段进行排序
                    # 获取排序后的 DataFrame 的索引
                    indices = matching_rows_for_qcmseventstart.index.tolist()

                    if len(indices)>1:#查询出数据不为空
                        #处理list中前N行的数据
                        # 遍历索引（除了最后一个）
                        for i in range(len(indices)):
                            if i==0:#处理第一条数据前面的数据，需要查询上个整点的最后一个等待时间
                                current_index = indices[i]
                                print(f"""我执行了没有错wu{current_index}""")
                                qcqcmseventend = filtered_dfs.iloc[current_index]

                                lastwholetimelastwaiteventquerysql = f"""select * from {kpi_for_qcms} where KEYTIME<'{timeChangeStr(strChangeTime(wholeHourTime) - timedelta(hours=1))}' and DATA_FROM='QCMSDB.qc_event_recorder_his.CREATE_TIME' and STS_NO='{stsid}' order by ID desc"""
                                lastwholetimelastwaiteventQueryReesults = o.query(lastwholetimelastwaiteventquerysql,t='df')
                                if isinstance(lastwholetimelastwaiteventQueryReesults,pd.DataFrame):  # 查询出来数据不为空，只判断第一条数据就行
                                    lastwholetimelastwaitevent = lastwholetimelastwaiteventQueryReesults.iloc[0]  # 上个整点时间内按照ID降序排列得到的第一条数据即是上个整点的最后一条等待event
                                    if lastwholetimelastwaitevent['EVENT_CODE'] not in (10001, 10002, 10003, 10010, 10011, 10012, 10021, 10022, 10031,10032):  # 检测到不在正常event中，才需要画图
                                        # 计算持续时间
                                        durationforqctask = qcqcmseventend['KEYTIME'] - (strChangeTime(wholeHourTime) - timedelta(hours=1))  ##这是一个Timedelta 对象，数值为 '0 days 00:00:01.749000'
                                        durationforqctasktotalseconds = durationforqctask.total_seconds()  # 持续时间的秒数
                                        if durationforqctasktotalseconds > 0:  # 只有当计算出来的数值>5秒才进行以下流程
                                            durationforqctask = timeChange(durationforqctask)  # 00:01:35

                                            namefortask = f"""等待eventcode={lastwholetimelastwaitevent['EVENT_CODE']},等待原因{dictForeventdefine.get(lastwholetimelastwaitevent['EVENT_CODE'])}，持续时间{durationforqctask}"""
                                            fig.add_trace(go.Scatter(
                                                x=[strChangeTime(wholeHourTime)-timedelta(hours=1), strChangeTime(wholeHourTime)-timedelta(hours=1),
                                                   qcqcmseventend['KEYTIME'],
                                                   qcqcmseventend['KEYTIME'], strChangeTime(wholeHourTime)-timedelta(hours=1)],
                                                y=[waitevent_min, waitevent_max, waitevent_max, waitevent_min,
                                                   waitevent_min],
                                                fill='toself',
                                                fillcolor=color_for_gantt_red, line=dict(color='black'),
                                                name=namefortask), row=1,
                                                col=1)

                                            fig.add_annotation(
                                                dict(x=(strChangeTime(wholeHourTime)-timedelta(hours=1)) + (
                                                        qcqcmseventend['KEYTIME'] - (strChangeTime(wholeHourTime)-timedelta(hours=1))) / 2,
                                                     # x 位置为两个 KEYTIME 的中点
                                                     y=(waitevent_min + waitevent_max) / 2,  # y 位置为矩形高度的一半
                                                     text=f'{durationforqctask}',
                                                     showarrow=False, xref="x1", yref="y1"))


                            if i<(len(indices)-1):#处理列表除了最后一条的数据（处理前N-1行数据）
                                print("我没错587")
                                current_index = indices[i]
                                print("我没错589")
                                next_index = indices[i + 1]
                                print(f"""我没错591  {i}current_index={current_index},next_index={next_index}""")

                                print(matching_rows_for_qcmseventstart)

                                #获取当前行和下一行的数据
                                qcqcmseventstart = filtered_dfs.iloc[current_index]

                                print("我没错597")
                                qcqcmseventend = filtered_dfs.iloc[next_index]

                                if qcqcmseventstart['EVENT_CODE'] not in (10001, 10002, 10003, 10010, 10011, 10012, 10021, 10022, 10031, 10032):
                                    # 计算持续时间
                                    durationforqctask = qcqcmseventend['KEYTIME'] - qcqcmseventstart[
                                        'KEYTIME']  ##这是一个Timedelta 对象，数值为 '0 days 00:00:01.749000'
                                    durationforqctasktotalseconds = durationforqctask.total_seconds()  # 持续时间的秒数
                                    if durationforqctasktotalseconds > 0:  # 只有当计算出来的数值>5秒才进行以下流程
                                        durationforqctask = timeChange(durationforqctask)  # 00:01:35

                                        namefortask = f"""等待eventcode={qcqcmseventstart['EVENT_CODE']},等待原因{dictForeventdefine.get(qcqcmseventstart['EVENT_CODE'])}，持续时间{durationforqctask}"""
                                        fig.add_trace(go.Scatter(
                                            x=[qcqcmseventstart['KEYTIME'], qcqcmseventstart['KEYTIME'], qcqcmseventend['KEYTIME'],
                                               qcqcmseventend['KEYTIME'], qcqcmseventstart['KEYTIME']],
                                            y=[waitevent_min, waitevent_max, waitevent_max, waitevent_min, waitevent_min],
                                            fill='toself',
                                            fillcolor=color_for_gantt_red, line=dict(color='black'), name=namefortask), row=1,
                                            col=1)

                                        fig.add_annotation(
                                            dict(x=qcqcmseventstart['KEYTIME'] + (
                                                    qcqcmseventend['KEYTIME'] - qcqcmseventstart['KEYTIME']) / 2,
                                                 # x 位置为两个 KEYTIME 的中点
                                                 y=(waitevent_min + waitevent_max) / 2,  # y 位置为矩形高度的一半
                                                 text=f'{durationforqctask}',
                                                 showarrow=False, xref="x1", yref="y1"))
                            if i == (len(indices) - 1):  # 仅处理最后一条的数据
                                current_index = indices[i]
                                print(f"""{i}current_index={current_index}""")
                                print("我没错630")
                                qcqcmseventstart = filtered_dfs.iloc[current_index]#开始的一行
                                print("我没错632")


                                durationforqctask = strChangeTime(wholeHourTime) - qcqcmseventstart['KEYTIME']
                                durationforqctasktotalseconds = durationforqctask.total_seconds()  # 持续时间的秒数
                                if durationforqctasktotalseconds>0:#只有当时间大于0才画出对应的图
                                    durationforqctask = timeChange(durationforqctask)

                                    namefortask = f"""部分信息：等待eventcode={qcqcmseventstart['EVENT_CODE']},等待原因{dictForeventdefine.get(qcqcmseventstart['EVENT_CODE'])}，持续时间{durationforqctask}"""
                                    fig.add_trace(go.Scatter(
                                        x=[qcqcmseventstart['KEYTIME'], qcqcmseventstart['KEYTIME'],
                                           strChangeTime(wholeHourTime),
                                           strChangeTime(wholeHourTime), qcqcmseventstart['KEYTIME']],
                                        y=[waitevent_min, waitevent_max, waitevent_max, waitevent_min, waitevent_min],
                                        fill='toself',
                                        fillcolor=color_for_gantt_red, line=dict(color='black'), name=namefortask),
                                        row=1,
                                        col=1)

                                    fig.add_annotation(
                                        dict(x=qcqcmseventstart['KEYTIME'] + (
                                                strChangeTime(wholeHourTime) - qcqcmseventstart['KEYTIME']) / 2,
                                             # x 位置为两个 KEYTIME 的中点
                                             y=(waitevent_min + waitevent_max) / 2,  # y 位置为矩形高度的一半
                                             text=f'{durationforqctask}',
                                             showarrow=False, xref="x1", yref="y1"))
                    ##步骤2：处理第2行到第N行的数据，第一行的数据应该单独处理
                    ###############画出QCMS发送的qcmsevent指令的填充图





                    ###############画出KPI发送的各种step指令的填充图
                    # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                    # 使用布尔索引来查找 DATA_FROM 字段等于特定值'KPIDB.kpi_mt_step_log.21.start_time'的行
                    steps = [11,12,13,14,15,16,17,18,21,22,23,24,25,26,27,28,31,32,33,34,35,36,37,38]
                    for step in steps:#遍历step的值
                        if step in [11,12,13,14,15,16,17,18]:
                            color_for_kpi=color_for_gantt_green
                        elif step in [21,22,23,24,25,26,27,28]:
                            color_for_kpi = color_for_gantt_red
                        elif step in [31,32,33,34,35,36,37,38]:
                            color_for_kpi = color_for_gantt_red


                        #步骤1：设置正常任务时间段内有配对或者整点时间将结束以整点时间结束为配对值
                        matching_rows_for_stepstart = filtered_dfs[
                            filtered_dfs['DATA_FROM'] == f'KPIDB.kpi_mt_step_log.{step}.start_time']
                        # matching_rows_for_statestart = filtered_dfs[
                        #     filtered_dfs['DATA_FROM'].str.contains(r'^KPIDB\.kpi_mt_state_log\.\d+\.start_time$',
                        #                                            na=False, regex=True)]
                        # 从匹配的行中提取索引值
                        stepStart_kpiindexs = matching_rows_for_stepstart.index.tolist()#[1,3]查询出来的是个列表
                        for stepStart_kpiindex in stepStart_kpiindexs:#查询QCMSground指令发送的所有值
                            stepstart = filtered_dfs.iloc[stepStart_kpiindex]  # 开始的某一行数据
                            ##################判断有无对应的end数据
                            matching_rows_for_stepends = filtered_dfs[
                                (filtered_dfs['PAIRED_VALUE'] == stepstart['PAIRED_VALUE']) &
                                (filtered_dfs['DATA_FROM'] == f'KPIDB.kpi_mt_step_log.{step}.end_time')]
                            # 判断查询出来的数据是否为空
                            # 从匹配的行中提取索引值
                            stepend_Qcmsindexs = matching_rows_for_stepends.index.tolist()  # [1,3]查询出来的是个列表
                            if stepend_Qcmsindexs!=[]:#表示end数据能查到，则找第一条数据，画出图
                                stepend = filtered_dfs.iloc[stepend_Qcmsindexs[0]]  # 结束的某一行数据

                                # 计算持续时间
                                durationforstep = stepend['KEYTIME'] - stepstart['KEYTIME']
                                durationforstep = timeChange(durationforstep)


                                namefortask=f"kpi发送step={step}指令{stepTransToLanguage(step)}到完成的时间{durationforstep}"
                                fig.add_trace(go.Scatter(
                                    x=[stepstart['KEYTIME'], stepstart['KEYTIME'], stepend['KEYTIME'], stepend['KEYTIME'],
                                       stepstart['KEYTIME']],
                                    y=[kpistep_min, kpistep_max, kpistep_max, kpistep_min, kpistep_min], fill='toself',
                                    fillcolor=color_for_kpi, line=dict(color='black'),
                                    name=namefortask), row=1, col=1)
                                # # 注意：这里的位置是手动设置的，可能需要根据实际情况调整

                                fig.add_annotation(
                                    dict(x=stepstart['KEYTIME'] + (stepend['KEYTIME'] - stepstart['KEYTIME']) / 2,
                                         # x 位置为两个 KEYTIME 的中点
                                         y=(kpistep_min + kpistep_max) / 2,  # y 位置为矩形高度的一半
                                         text=f"""{durationforstep}""",
                                         showarrow=False, xref=f"x1", yref=f"y1"))
                            else:  # 表示end数据找不到，则以整点结束时间为最后时间画图
                                # 计算持续时间
                                durationforstep = strChangeTime(wholeHourTime) - stepstart['KEYTIME']
                                durationforstep = timeChange(durationforstep)

                                namefortask = f"部分数据：kpi发送step={step}指令{stepTransToLanguage(step)}到完成的时间{durationforstep}"
                                fig.add_trace(go.Scatter(
                                    x=[stepstart['KEYTIME'], stepstart['KEYTIME'], strChangeTime(wholeHourTime),
                                       strChangeTime(wholeHourTime),
                                       stepstart['KEYTIME']],
                                    y=[kpistep_min, kpistep_max, kpistep_max, kpistep_min, kpistep_min], fill='toself',
                                    fillcolor=color_for_kpi, line=dict(color='black'),
                                    name=namefortask), row=1, col=1)
                                # # 注意：这里的位置是手动设置的，可能需要根据实际情况调整

                                fig.add_annotation(
                                    dict(x=stepstart['KEYTIME'] + (strChangeTime(wholeHourTime) - stepstart['KEYTIME']) / 2,
                                         # x 位置为两个 KEYTIME 的中点
                                         y=(kpistep_min + kpistep_max) / 2,  # y 位置为矩形高度的一半
                                         text=f"""{durationforstep}""",
                                         showarrow=False, xref=f"x1", yref=f"y1"))
                        # 步骤1：设置正常任务时间段内有配对或者整点时间将结束以整点时间结束为配对值

                        # 步骤2：整点开始时间可能有些任务在前一个时间整点内，需要把这些数据挪过来
                        matching_rows_for_stependtime= filtered_dfs[
                            filtered_dfs['DATA_FROM'] == f'KPIDB.kpi_mt_step_log.{step}.end_time']
                        # 从匹配的行中提取索引值
                        stependtimesforwholehour_kpiindexs = matching_rows_for_stependtime.index.tolist()  # [1,3]查询出来的是个列表
                        if stependtimesforwholehour_kpiindexs!=[]:#查询出来不为空，则取第一条数据作为当前整点时间内第一个结束
                            stependtimeforwholehour = filtered_dfs.iloc[stependtimesforwholehour_kpiindexs[0]]  # 查询到结束的的某一行数据

                            #查询前面有无对应的插入时间
                            stependtimeforwholehourstart = filtered_dfs[(filtered_dfs['PAIRED_VALUE'] == stependtimeforwholehour['PAIRED_VALUE'])&
                                                                           (filtered_dfs['KEYTIME'] < stependtimeforwholehour['KEYTIME'])] # 从匹配的行中提取索引值
                            stependtimeforwholehourstart_indexs = stependtimeforwholehourstart.index.tolist()  # [1,3]查询出来的是个列表]
                            if stependtimeforwholehourstart_indexs==[]:#表示第一条ground的放箱记录没有对应的开始时间，则以wholehouretime-1小时为开始时间
                                durationforqctask = stependtimeforwholehour['KEYTIME']-(strChangeTime(wholeHourTime)-timedelta(hours=1))
                                durationforqctask = timeChange(durationforqctask)
                                namefortask = f"部分数据：kpi发送step={step}指令{stepTransToLanguage(step)}到完成的时间{durationforqctask}"
                                fig.add_trace(go.Scatter(
                                    x=[strChangeTime(wholeHourTime)-timedelta(hours=1),strChangeTime(wholeHourTime)-timedelta(hours=1), stependtimeforwholehour['KEYTIME'],stependtimeforwholehour['KEYTIME'], strChangeTime(wholeHourTime)-timedelta(hours=1)],
                                    y=[kpistep_min, kpistep_max, kpistep_max, kpistep_min, kpistep_min], fill='toself',
                                    fillcolor=color_for_kpi, line=dict(color='black'), name=namefortask),
                                    row=1, col=1)
                                fig.add_annotation(
                                    dict(x=strChangeTime(wholeHourTime)-timedelta(hours=1) + (
                                                stependtimeforwholehour['KEYTIME'] - (strChangeTime(wholeHourTime)-timedelta(hours=1))) / 2,
                                         # x 位置为两个 KEYTIME 的中点
                                         y=(kpistep_min + kpistep_max) / 2,  # y 位置为矩形高度的一半
                                         text=f'{durationforqctask}',
                                         showarrow=False, xref="x1", yref="y1"))


                    #########################################kpi_mt_step_log逻辑




                    # ###############画出KPI发送的各种state指令的填充图
                    # # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                    # # 使用布尔索引来查找 DATA_FROM 字段等于特定值'KPIDB.kpi_mt_state_log.21.start_time'的行
                    # #步骤1：设置正常任务时间段内有配对或者整点时间将结束以整点时间结束为配对值
                    # matching_rows_for_statestart = filtered_dfs[
                    #     filtered_dfs['DATA_FROM'].str.contains(r'^KPIDB\.kpi_mt_state_log\.\d+\.start_time$',
                    #                                            na=False, regex=True)]
                    # # 从匹配的行中提取索引值
                    # stateStart_kpiindexs = matching_rows_for_statestart.index.tolist()#[1,3]查询出来的是个列表
                    # # if len(stateStart_kpiindexs)!=0:
                    # #     print(f"""找到值了{stateStart_kpiindexs}""")
                    # for stateStart_kpiindex in stateStart_kpiindexs:#查询QCMSground指令发送的所有值
                    #     statestart = filtered_dfs.iloc[stateStart_kpiindex]  # 开始的某一行数据
                    #     ##################判断有无对应的end数据
                    #     matching_rows_for_stateends = filtered_dfs[(filtered_dfs['PAIRED_VALUE'] == statestart['PAIRED_VALUE'])&
                    #                                                (filtered_dfs['DATA_FROM'].str.contains(r'^KPIDB\.kpi_mt_state_log\.\d+\.end_time$',na=False, regex=True))]
                    #     # 判断查询出来的数据是否为空
                    #     # 从匹配的行中提取索引值
                    #     stateend_Qcmsindexs = matching_rows_for_stateends.index.tolist()  # [1,3]查询出来的是个列表
                    #     if stateend_Qcmsindexs!=[]:#表示end数据能查到，则找第一条数据，画出图
                    #         stateend = filtered_dfs.iloc[stateend_Qcmsindexs[0]]  # 结束的某一行数据
                    #
                    #         # 计算持续时间
                    #         durationforstate = stateend['KEYTIME'] - statestart['KEYTIME']
                    #         durationforstate = timeChange(durationforstate)
                    #         # print(f"""找到了，持续时间{durationforstate}""")
                    #
                    #
                    #         namefortask=f"kpi发送{statestart['NOTES']}"
                    #         fig.add_trace(go.Scatter(
                    #             x=[statestart['KEYTIME'], statestart['KEYTIME'], stateend['KEYTIME'], stateend['KEYTIME'],
                    #                statestart['KEYTIME']],
                    #             y=[kpistate_min, kpistate_max, kpistate_max, kpistate_min, kpistate_min], fill='toself',
                    #             fillcolor=color_for_gantt_red, line=dict(color='black'),
                    #             name=namefortask), row=1, col=1)
                    #         # # 注意：这里的位置是手动设置的，可能需要根据实际情况调整
                    #
                    #         fig.add_annotation(
                    #             dict(x=statestart['KEYTIME'] + (stateend['KEYTIME'] - statestart['KEYTIME']) / 2,
                    #                  # x 位置为两个 KEYTIME 的中点
                    #                  y=(kpistate_min + kpistate_max) / 2,  # y 位置为矩形高度的一半
                    #                  text=f"""{durationforstate}""",
                    #                  showarrow=False, xref=f"x1", yref=f"y1"))
                    #     else:  # 表示end数据找不到，则以整点结束时间为最后时间画图
                    #         # 计算持续时间
                    #         durationforstate = strChangeTime(wholeHourTime) - statestart['KEYTIME']
                    #         durationforstate = timeChange(durationforstate)
                    #         # print(f"""没找到，持续时间{durationforstate}""")
                    #
                    #         namefortask = f"部分数据：kpi发送{statestart['NOTES']}"
                    #         fig.add_trace(go.Scatter(
                    #             x=[statestart['KEYTIME'], statestart['KEYTIME'], strChangeTime(wholeHourTime),
                    #                strChangeTime(wholeHourTime),
                    #                statestart['KEYTIME']],
                    #             y=[kpistate_min, kpistate_max, kpistate_max, kpistate_min, kpistate_min], fill='toself',
                    #             fillcolor=color_for_gantt_red, line=dict(color='black'),
                    #             name=namefortask), row=1, col=1)
                    #         # # 注意：这里的位置是手动设置的，可能需要根据实际情况调整
                    #
                    #         fig.add_annotation(
                    #             dict(x=statestart['KEYTIME'] + (strChangeTime(wholeHourTime) - statestart['KEYTIME']) / 2,
                    #                  # x 位置为两个 KEYTIME 的中点
                    #                  y=(kpistate_min + kpistate_max) / 2,  # y 位置为矩形高度的一半
                    #                  text=f"""{durationforstate}""",
                    #                  showarrow=False, xref=f"x1", yref=f"y1"))
                    # # 步骤1：设置正常任务时间段内有配对或者整点时间将结束以整点时间结束为配对值
                    # #########################################kpi_mt_state_log逻辑


                    ###############画出单机指令安全高度以下的部分
                    # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                    # 使用布尔索引来查找 DATA_FROM 字段等于特定值'OPCUA.mhabovesafeheight.2'的,2=安全高度以下，1=安全高度以上
                    matching_rows_for_mhabovesafeheightstart = filtered_dfs[filtered_dfs['DATA_FROM'] == 'OPCUA.mhabovesafeheight.2']
                    # 从匹配的行中提取索引值
                    mhabovesafeheightStart_indexs = matching_rows_for_mhabovesafeheightstart.index.tolist()#[1,3]查询出来的是个列表]


                    for mhabovesafeheightStart_index in mhabovesafeheightStart_indexs:#查询单机安全高度以下的所有值
                        mhabovesafeheightstart = filtered_dfs.iloc[mhabovesafeheightStart_index]  # 开始的某一行数据

                        ###################判断有无对应的安全高度以上的数据
                        matching_rows_for_mhabovesafeheightends = filtered_dfs[(filtered_dfs['DATA_FROM'] == 'OPCUA.mhabovesafeheight.1') &
                                                                               (filtered_dfs['KEYTIME'] > mhabovesafeheightstart['KEYTIME'])]
                        # 判断查询出来的数据是否为空
                        # 从匹配的行中提取索引值
                        mhabovesafeheightend_indexs = matching_rows_for_mhabovesafeheightends.index.tolist()  # [1,3]查询出来的是个列表
                        if mhabovesafeheightend_indexs!=[]:#表示end数据能查到，则找第一条数据，画出图
                            mhabovesafeheightend = filtered_dfs.iloc[mhabovesafeheightend_indexs[0]]  # 结束的某一行数据


                            #计算持续时间
                            durationforqctask=mhabovesafeheightend['KEYTIME']-mhabovesafeheightstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask=f"""单机安全高度以下的持续时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(x=[mhabovesafeheightstart['KEYTIME'],mhabovesafeheightstart['KEYTIME'],mhabovesafeheightend['KEYTIME'],mhabovesafeheightend['KEYTIME'],mhabovesafeheightstart['KEYTIME']],y=[mhAboveSafeHeight_min,mhAboveSafeHeight_max,mhAboveSafeHeight_max,mhAboveSafeHeight_min,mhAboveSafeHeight_min],fill='toself',fillcolor=color_for_gantt_red,line=dict(color='black'),name=namefortask),row=1,col=1)

                            fig.add_annotation(
                                dict(x=mhabovesafeheightstart['KEYTIME'] + (mhabovesafeheightend['KEYTIME'] - mhabovesafeheightstart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                     y=(mhAboveSafeHeight_min+mhAboveSafeHeight_max)/2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                        else:
                            # 计算持续时间
                            durationforqctask = strChangeTime(wholeHourTime) - mhabovesafeheightstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask = f"""单机安全高度以下的持续时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(
                                x=[mhabovesafeheightstart['KEYTIME'], mhabovesafeheightstart['KEYTIME'],
                                   strChangeTime(wholeHourTime), strChangeTime(wholeHourTime),
                                   mhabovesafeheightstart['KEYTIME']],
                                y=[mhAboveSafeHeight_min, mhAboveSafeHeight_max, mhAboveSafeHeight_max,
                                   mhAboveSafeHeight_min, mhAboveSafeHeight_min], fill='toself',
                                fillcolor=color_for_gantt_red, line=dict(color='black'), name=namefortask), row=1,
                                          col=1)

                            fig.add_annotation(
                                dict(x=mhabovesafeheightstart['KEYTIME'] + (
                                            strChangeTime(wholeHourTime) - mhabovesafeheightstart['KEYTIME']) / 2,
                                     # x 位置为两个 KEYTIME 的中点
                                     y=(mhAboveSafeHeight_min + mhAboveSafeHeight_max) / 2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                    ###############画出单机指令安全高度以下的部分




                    ###############画出单机指令状态从not ready for ecs->ready for ecs持续时间的部分
                    # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                    # 使用布尔索引来查找 DATA_FROM 字段等于特定值'OPCUA.mtinstructionstatus.2'的,2=安全高度以下，1=安全高度以上
                    matching_rows_for_mtinstructionstatusstart = filtered_dfs[filtered_dfs['DATA_FROM'] == 'OPCUA.mtinstructionstatus.2']
                    # 从匹配的行中提取索引值
                    mtinstructionstatusStart_indexs = matching_rows_for_mtinstructionstatusstart.index.tolist()#[1,3]查询出来的是个列表]


                    for mtinstructionstatusStart_index in mtinstructionstatusStart_indexs:#查询OPCUA.mtinstructionstatus.2的所有值
                        mtinstructionstatusstart = filtered_dfs.iloc[mtinstructionstatusStart_index]  # 开始的某一行数据

                        ###################判断有无对应的ready for ecs的数据
                        matching_rows_for_mtinstructionstatusends = filtered_dfs[(filtered_dfs['DATA_FROM'] == 'OPCUA.mtinstructionstatus.1') &
                                                                               (filtered_dfs['KEYTIME'] > mtinstructionstatusstart['KEYTIME'])]
                        # 判断查询出来的数据是否为空
                        # 从匹配的行中提取索引值
                        mtinstructionstatusend_indexs = matching_rows_for_mtinstructionstatusends.index.tolist()  # [1,3]查询出来的是个列表
                        if mtinstructionstatusend_indexs!=[]:#表示end数据能查到，则找第一条数据，画出图
                            mtinstructionstatusend = filtered_dfs.iloc[mtinstructionstatusend_indexs[0]]  # 结束的某一行数据


                            #计算持续时间
                            durationforqctask=mtinstructionstatusend['KEYTIME']-mtinstructionstatusstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask=f"""not ready for ecs持续时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(x=[mtinstructionstatusstart['KEYTIME'],mtinstructionstatusstart['KEYTIME'],mtinstructionstatusend['KEYTIME'],mtinstructionstatusend['KEYTIME'],mtinstructionstatusstart['KEYTIME']],y=[mtInstructionStatus_min,mtInstructionStatus_max,mtInstructionStatus_max,mtInstructionStatus_min,mtInstructionStatus_min],fill='toself',fillcolor=color_for_gantt_red,line=dict(color='black'),name=namefortask),row=1,col=1)

                            fig.add_annotation(
                                dict(x=mtinstructionstatusstart['KEYTIME'] + (mtinstructionstatusend['KEYTIME'] - mtinstructionstatusstart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                     y=(mtInstructionStatus_min+mtInstructionStatus_max)/2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                        else:
                            # 计算持续时间
                            durationforqctask = strChangeTime(wholeHourTime) - mtinstructionstatusstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask = f"""not ready for ecs持续时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(
                                x=[mtinstructionstatusstart['KEYTIME'], mtinstructionstatusstart['KEYTIME'],
                                   strChangeTime(wholeHourTime), strChangeTime(wholeHourTime),
                                   mtinstructionstatusstart['KEYTIME']],
                                y=[mtInstructionStatus_min, mtInstructionStatus_max, mtInstructionStatus_max,
                                   mtInstructionStatus_min, mtInstructionStatus_min], fill='toself',
                                fillcolor=color_for_gantt_red, line=dict(color='black'), name=namefortask), row=1,
                                          col=1)

                            fig.add_annotation(
                                dict(x=mtinstructionstatusstart['KEYTIME'] + (
                                            strChangeTime(wholeHourTime) - mtinstructionstatusstart[
                                        'KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                     y=(mtInstructionStatus_min + mtInstructionStatus_max) / 2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                    ###############画出单机指令状态从not ready for ecs->ready for ecs持续时间的部分



                    ###############画出单机指令状态从mtworkmode持续时间的部分1=Normal Mode;2=Maintenance Mode;3=Local Mode
                    # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                    # 使用布尔索引来查找 DATA_FROM 字段等于特定值'OPCUA.mtworkmode.2'或者'OPCUA.mtworkmode.3'的
                    matching_rows_for_mtworkmodestart = filtered_dfs[(filtered_dfs['DATA_FROM'] == 'OPCUA.mtworkmode.3') |
                                                                     (filtered_dfs['DATA_FROM'] == 'OPCUA.mtworkmode.2') |
                                                                     (filtered_dfs['DATA_FROM'] == 'OPCUA.mtworkmode.0')]
                    # 从匹配的行中提取索引值
                    mtworkmodeStart_indexs = matching_rows_for_mtworkmodestart.index.tolist()#[1,3]查询出来的是个列表]


                    for mtworkmodeStart_index in mtworkmodeStart_indexs:#查询mtworkmode的所有值
                        mtworkmodestart = filtered_dfs.iloc[mtworkmodeStart_index]  # 开始的某一行数据

                        ###################判断有无对应的mtworkmode的数据
                        matching_rows_for_mtworkmodeends = filtered_dfs[(filtered_dfs['DATA_FROM'] == 'OPCUA.mtworkmode.1') &
                                                                               (filtered_dfs['KEYTIME'] > mtworkmodestart['KEYTIME'])]
                        # 判断查询出来的数据是否为空
                        # 从匹配的行中提取索引值
                        mtworkmodeend_indexs = matching_rows_for_mtworkmodeends.index.tolist()  # [1,3]查询出来的是个列表
                        if mtworkmodeend_indexs!=[]:#表示end数据能查到，则找第一条数据，画出图
                            mtworkmodeend = filtered_dfs.iloc[mtworkmodeend_indexs[0]]  # 结束的某一行数据


                            #计算持续时间
                            durationforqctask=mtworkmodeend['KEYTIME']-mtworkmodestart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask=f"""Local Mode或者Maintenance Mode持续时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(x=[mtworkmodestart['KEYTIME'],mtworkmodestart['KEYTIME'],mtworkmodeend['KEYTIME'],mtworkmodeend['KEYTIME'],mtworkmodestart['KEYTIME']],y=[mtWorkMode_min,mtWorkMode_max,mtWorkMode_max,mtWorkMode_min,mtWorkMode_min],fill='toself',fillcolor=color_for_gantt_red,line=dict(color='black'),name=namefortask),row=1,col=1)

                            fig.add_annotation(
                                dict(x=mtworkmodestart['KEYTIME'] + (mtworkmodeend['KEYTIME'] - mtworkmodestart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                     y=(mtWorkMode_min+mtWorkMode_max)/2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                        else:
                            # 计算持续时间
                            durationforqctask = strChangeTime(wholeHourTime) - mtworkmodestart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask = f"""Local Mode或者Maintenance Mode持续时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(
                                x=[mtworkmodestart['KEYTIME'], mtworkmodestart['KEYTIME'], strChangeTime(wholeHourTime),
                                   strChangeTime(wholeHourTime), mtworkmodestart['KEYTIME']],
                                y=[mtWorkMode_min, mtWorkMode_max, mtWorkMode_max, mtWorkMode_min, mtWorkMode_min],
                                fill='toself', fillcolor=color_for_gantt_red, line=dict(color='black'),
                                name=namefortask), row=1, col=1)

                            fig.add_annotation(
                                dict(x=mtworkmodestart['KEYTIME'] + (
                                            strChangeTime(wholeHourTime) - mtworkmodestart['KEYTIME']) / 2,
                                     # x 位置为两个 KEYTIME 的中点
                                     y=(mtWorkMode_min + mtWorkMode_max) / 2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))

                    ###############画出单机指令状态从mtworkmode持续时间的部分1=Normal Mode;2=Maintenance Mode;3=Local Mode





                    ##############画出QCMS发送的QCMSDB.qc_container_transfer.Pickup.CREATE_TIME到QCMSDB.qc_container_transfer.Ground.CREATE_TIME指令的填充图

                    # 步骤1：以整点时间画出整点时间内的，快结束时以整点结束时间为截止时间
                    # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.qc_container_transfer.Pickup.CREATE_TIME'的行
                    matching_rows_for_containertransferstart = filtered_dfs[filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_container_transfer.Pickup.CREATE_TIME']
                    # 从匹配的行中提取索引值
                    containertransfer_indexs = matching_rows_for_containertransferstart.index.tolist()#[1,3]查询出来的是个列表]

                    for containertransfer_index in containertransfer_indexs:#查询QCMSDB.qc_container_transfer.Pickup.CREATE_TIME开始时间
                        containertransferstart = filtered_dfs.iloc[containertransfer_index]  # 开始的某一行数据
                        ###################判断有无对应的end数据
                        matching_rows_for_containertransferends = filtered_dfs[
                            (filtered_dfs['PAIRED_VALUE'] == containertransferstart['PAIRED_VALUE']) &
                            (filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_container_transfer.Ground.CREATE_TIME')]
                        # 判断查询出来的数据是否为空
                        # 从匹配的行中提取索引值
                        containertransferend_indexs = matching_rows_for_containertransferends.index.tolist()  # [1,3]查询出来的是个列表
                        if containertransferend_indexs!=[]:#表示end数据能查到，则找第一条数据，画出图
                            containertransferend = filtered_dfs.iloc[containertransferend_indexs[0]]  # 结束的某一行数据
                            #计算持续时间
                            durationforqctask=containertransferend['KEYTIME']-containertransferstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask=f"""containertransfer从pickup到ground持续时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(x=[containertransferstart['KEYTIME'],containertransferstart['KEYTIME'],containertransferend['KEYTIME'],containertransferend['KEYTIME'],containertransferstart['KEYTIME']],y=[containertransfer_min,containertransfer_max,containertransfer_max,containertransfer_min,containertransfer_min],fill='toself',fillcolor=color_for_gantt_orange1,line=dict(color='black'),name=namefortask),row=1,col=1)

                            fig.add_annotation(
                                dict(x=containertransferstart['KEYTIME'] + (containertransferend['KEYTIME'] - containertransferstart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                     y=(containertransfer_min+containertransfer_max)/2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                        else:  # 表示end数据找不到，则以整点结束时间为最后时间画图
                            # 计算持续时间
                            durationforqctask = strChangeTime(wholeHourTime) - containertransferstart['KEYTIME']
                            durationforqctask = timeChange(durationforqctask)

                            namefortask = f"""部分数据：containertransfer从pickup到ground持续时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(
                                x=[containertransferstart['KEYTIME'], containertransferstart['KEYTIME'],
                                   strChangeTime(wholeHourTime), strChangeTime(wholeHourTime),
                                   containertransferstart['KEYTIME']],
                                y=[containertransfer_min, containertransfer_max, containertransfer_max,
                                   containertransfer_min, containertransfer_min], fill='toself',
                                fillcolor=color_for_gantt_orange1, line=dict(color='black'), name=namefortask), row=1,
                                          col=1)

                            fig.add_annotation(
                                dict(x=containertransferstart['KEYTIME'] + (
                                            strChangeTime(wholeHourTime) - containertransferstart['KEYTIME']) / 2,
                                     # x 位置为两个 KEYTIME 的中点
                                     y=(containertransfer_min + containertransfer_max) / 2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                    # 步骤1：以整点时间画出整点时间内的，快结束时以整点结束时间为截止时间


                    # 步骤2：整点时间开始没有配对时需要画数据
                    matching_rows_for_containertransfergroundwholetimes = filtered_dfs[filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_container_transfer.Ground.CREATE_TIME']
                    # 从匹配的行中提取索引值
                    containertransfergroundforwholehour_indexs = matching_rows_for_containertransfergroundwholetimes.index.tolist()  # [1,3]查询出来的是个列表
                    if containertransfergroundforwholehour_indexs!=[]:#查询出来不为空，则取第一条数据作为当前整点时间内第一个结束
                        containertransferground = filtered_dfs.iloc[containertransfergroundforwholehour_indexs[0]]  # 查询到结束的的某一行数据

                        #查询前面有无对应的插入时间
                        containertransfergroundforwhole = filtered_dfs[(filtered_dfs['PAIRED_VALUE'] == containertransferground['PAIRED_VALUE'])&
                                                                       (filtered_dfs['KEYTIME'] < containertransferground['KEYTIME'])] # 从匹配的行中提取索引值
                        containertransfergroundforwhole_indexs = containertransfergroundforwhole.index.tolist()  # [1,3]查询出来的是个列表]
                        if containertransfergroundforwhole_indexs==[]:#表示第一条ground的放箱记录没有对应的开始时间，则以wholehouretime-1小时为开始时间
                            durationforqctask = containertransferground['KEYTIME']-(strChangeTime(wholeHourTime)-timedelta(hours=1))
                            durationforqctask = timeChange(durationforqctask)
                            namefortask = f"""部分数据：containertransfer从pickup到ground持续时间{durationforqctask}"""
                            fig.add_trace(go.Scatter(
                                x=[strChangeTime(wholeHourTime)-timedelta(hours=1),strChangeTime(wholeHourTime)-timedelta(hours=1), containertransferground['KEYTIME'],containertransferground['KEYTIME'], strChangeTime(wholeHourTime)-timedelta(hours=1)],y=[containertransfer_min, containertransfer_max, containertransfer_max, containertransfer_min, containertransfer_min], fill='toself',
                                fillcolor=color_for_gantt_orange1, line=dict(color='black'), name=namefortask),
                                row=1, col=1)
                            fig.add_annotation(
                                dict(x=strChangeTime(wholeHourTime)-timedelta(hours=1) + (
                                            containertransferground['KEYTIME'] - (strChangeTime(wholeHourTime)-timedelta(hours=1))) / 2,
                                     # x 位置为两个 KEYTIME 的中点
                                     y=(containertransfer_min + containertransfer_max) / 2,  # y 位置为矩形高度的一半
                                     text=f'{durationforqctask}',
                                     showarrow=False, xref="x1", yref="y1"))
                    #     ###############步骤2：查询单个整点时间内的任务结束时间，初始时间以wholeHourTime-1为开始



                    ###############画出QCMS发送的qc_container_transfer的填充图

                    # # 更新布局以设置初始的 x 轴范围，并允许缩放
                    # fig.update_layout(
                    #     xaxis_range=[minTime, timeChangeStr(strChangeTime(minTime) + timedelta(hours=0.3))],
                    #     # 设置初始范围为 1 小时
                    #     xaxis_fixedrange=False  # 允许 x 轴缩放
                    # )

                    filenameFortime = str(wholeHourTime).replace('-', '').replace(' ', '').replace(':', '').replace('.', '')
                    Formintime = str(minTime).replace('-', '').replace(' ', '').replace(':', '').replace('.', '')
                    Formaxtime = str(maxTime).replace('-', '').replace(' ', '').replace(':', '').replace('.', '')
                    # #打入文件夹的开始时间
                    titleforsingle=f'''岸桥{stsid}的{filenameFortime}整点时间内所有任务明细'''
                    fig.update_layout(title=titleforsingle)#更新整张表的标题

                    output_path=f"./documents_vbt_id/{Formintime[:8]}_{Formaxtime[:8]}documents_vbt_id{envlist['VBT_ID']}/{stsid}/岸桥{stsid}的整点时间内{filenameFortime}任务明细.html"
                    # 提取目录路径
                    directory = os.path.dirname(output_path)
                    # 检查目录是否存在，如果不存在则创建
                    if not os.path.exists(directory):
                        os.makedirs(directory)


                    #当查询出来的数据包含QCMS才画图，否则不画图
                    filtered_df = filtered_dfs[filtered_dfs['DATA_FROM_TYPE'] == 'QCMS']
                    if not filtered_df.empty:
                        # 保存图形到html格式
                        fig.write_html(output_path, full_html=False)






