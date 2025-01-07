
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqliteHandle
from basicFunctionDefine import *
import ast
import datetime
import os
# o=sqliteHandle.sqliteHandler('kpiForEcs.db')

# o=sqliteHandle.sqliteHandler(r'E:\python完整脚本\bilu\KPI20250103\kpiforQcms20250103.db')
# o=sqliteHandle.sqliteHandler(r'E:\pythonlearn\KPI\KPI20241223\kpiforQcms20241223.db')
o=sqliteHandle.sqliteHandler(r'./kpiforQcms20250103.db')

#设置需要写入目标表的表名字
tablename_for_kpi='kpi_for_qcms212467'

# stsids = ['103']
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

###QCMS整个任务完成的时间高度区域
qcms_min = 1
qcms_max = 5

###QCMS各阶段任务完成的时间高度区域
qcmstask_min = 5
qcmstask_max = 10

###KPI各阶段任务完成的时间高度区域
kpistep_min = 10
kpistep_max = 15

#查询之前去掉字段KEYTIME是空的时间
o.executesql(f"delete from {tablename_for_kpi} where KEYTIME='' or KEYTIME='0001-01-01 00:00:00'")
########步骤1：确定某个岸桥在某些整点时间内有多少条任务
for stsid_index,stsid in enumerate(stsids):#遍历岸桥号
    wholeTimeTasksQuerySql= f"""select * from {tablename_for_kpi} where STS_NO='{stsid}' and DATA_FROM_TYPE='QCMS' order by KEYTIME asc"""#查询的目的是为了检查某个岸桥下所有任务的开始时间和结束时间为了提取出整点使用
    wholeTimeTasksQueryResults = o.query(wholeTimeTasksQuerySql,t='df')
    if len(wholeTimeTasksQueryResults)!=0:#如果查询出来的数据不为空
        # wholeTimeTasksQueryResults['KEYTIME'] = pd.to_datetime(wholeTimeTasksQueryResults['KEYTIME'], format='mixed')  # 利用pandas将数据转为时间格式
        wholeTimeTasksQueryResults['KEYTIME'] = pd.to_datetime(wholeTimeTasksQueryResults['KEYTIME'])  # 利用pandas将数据转为时间格式
        # 接下来，我们找出'KEYTIME'列中的最小和最大日期时间值
        min_time = wholeTimeTasksQueryResults['KEYTIME'].min()
        max_time = wholeTimeTasksQueryResults['KEYTIME'].max()

        wholeHourTimeforStss=wholeHourTimes(min_time,max_time)#得到当前岸桥作业时间整点的时间，list类型，但是里面的时间格式时str=['2024-12-20 18:00:00', '2024-12-20 19:00:00', '2024-12-20 20:00:00']
        #判断得到整点时间的长度时多少
        lengthWholeHourTimesforSts=len(wholeHourTimeforStss)
        if lengthWholeHourTimesforSts!=0:#当得到整点时间不为0才可以进行下一步,画出整点时间内的数据
            for wholeHourTimeforSts_index,wholeHourTimeforSts in enumerate(wholeHourTimeforStss):#遍历整点时间内的岸桥数据,wholeHourTimeforSts是str类型，如'2024-12-20 18:00:00'


                if (wholeHourTimeforSts_index+1)<lengthWholeHourTimesforSts:#注意不能有=号
                    #查询的目的是为了查询整点时间范围内当前岸桥到底有多少task_id
                    querysqlforstsno_task = f"SELECT DISTINCT TASK_ID FROM {tablename_for_kpi} WHERE STS_NO = '{stsid}' and KEYTIME>='{wholeHourTimeforSts}' and KEYTIME<='{wholeHourTimeforStss[wholeHourTimeforSts_index+1]}' order by KEYTIME asc"  #
                    forstsno_taskQueryResults=o.query(querysqlforstsno_task,t='df')
                    if forstsno_taskQueryResults is not None  and isinstance(forstsno_taskQueryResults, pd.DataFrame):#如果能查询出来任务遍历任务TASK_ID,并且是DatFrame类型
                        # # #创建要画的图
                        fig = make_subplots(cols=1, rows=1, subplot_titles='')  # cols表示设备数量，rows表示最大任务数量

                        for index_x, forstsno_taskQueryResult in forstsno_taskQueryResults.iterrows():  # forstsno_taskQueryResults.iterrows对查询出来的任务画图
                            querysqlforstsno_task = f"SELECT * FROM {tablename_for_kpi} WHERE STS_NO = '{stsid}' and TASK_ID={forstsno_taskQueryResult['TASK_ID']} order by KEYTIME asc"  #
                            filtered_df=o.query(querysqlforstsno_task,t='df')
                            # filtered_df['KEYTIME'] = pd.to_datetime(filtered_df['KEYTIME'],format='mixed')#利用pandas将数据转为时间格式
                            filtered_df['KEYTIME'] = pd.to_datetime(filtered_df['KEYTIME'])  # 利用pandas将数据转为时间格式
                            if filtered_df is not None:#查询出来的数据不为空
                                # 对于每个时间点，添加一条垂直线
                                # #创建要画的图
                                # fig = make_subplots(cols=1, rows=1, subplot_titles='')  # cols表示设备数量，rows表示最大任务数量

                                for idx, row in filtered_df.iterrows():#filtered_df.iterrows对查询出来的任务画

                                    #创建散点图，将每个时间节点都打印出来
                                    if row['DATA_FROM_TYPE']=='QCMS':
                                        corlorline= color_for_simple_green
                                    elif row['DATA_FROM_TYPE']=='KPI':
                                        corlorline = color_for_gantt_orange1
                                    else:
                                        corlorline=color_for_gantt_black
                                    fig.add_trace(go.Scatter(x=[row['KEYTIME'],row['KEYTIME']],y=[min_height,max_height],
                                                             mode="lines",#指定绘制模式。常用的值包括 'markers'（仅绘制散点）、'lines'（绘制线条，连接数据点）、'lines+markers'（同时绘制线条和散点）等
                                                             name=str(row['STS_NO'])+'设备'+str(row['TASK_ID'])+str(row['NOTES']),#为散点图轨迹指定名称，该名称将显示在图例中
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





                            #######################################创建QCMS单个指令和单机KPI指令单个完成时间的逻辑
                            ###############画出QCMS整个任务持续时间的填充图
                            # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.QC_TOS_TASK.RESPONSE_TIME'的行作为整个任务的起点
                            matching_rows_for_qctaskstart = filtered_df[
                                filtered_df['DATA_FROM'] == 'QCMSDB.QC_TOS_TASK_HIS.TRIG_CREATED.INSERT']
                            # 从匹配的行中提取索引值
                            qctaskStart_Qcmsindexs = matching_rows_for_qctaskstart.index.tolist()#[1,3]查询出来的是个列表]

                            qctaskDelete_Qcmsindexs=[]
                            # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.QC_TOS_TASK_HIS.TRIGGER_ACTION(DELETE).TRIG_CREATED'的行作为某条任务删除的时间点
                            matching_rows_for_qctaskdelete= filtered_df[
                                filtered_df['DATA_FROM'] == 'QCMSDB.QC_TOS_TASK_HIS.TRIGGER_ACTION(DELETE).TRIG_CREATED']
                            # 从匹配的行中提取索引值
                            qctaskDelete_Qcmsindexs = matching_rows_for_qctaskdelete.index.tolist()#[1,3]查询出来的是个列表


                            # ## 尝试使用使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.QC_TROLLEY_INSTRUCTION.Ground.END_TIME'的行作为整个任务的终点
                            # matching_rows_for_qctaskend = filtered_df[
                            #     filtered_df['DATA_FROM'] == 'QCMSDB.QC_TROLLEY_INSTRUCTION.Ground.END_TIME']
                            # # 检查是否找到了匹配的行
                            # if not matching_rows_for_qctaskend.empty:
                            #     # 如果找到了，则提取这些行的索引值
                            #     qctaskEnd_Qcmsindexs = matching_rows_for_qctaskend.index.tolist()
                            # else:
                            #     # 如果没有找到，判断当前任务是否只找到了1个开始位置，则使用filtered_df的最后一个索引
                            #     if (len(qctaskStart_Qcmsindexs)+1)==len(qctaskDelete_Qcmsindexs):#表示此时数据库中插入数据和删除数据不匹配
                            #         qctaskEnd_Qcmsindexs = qctaskDelete_Qcmsindexs+[filtered_df.index[-1]]
                            if len(qctaskStart_Qcmsindexs)==(len(qctaskDelete_Qcmsindexs)+1):#表示此时数据库中插入数据和删除数据不匹配
                                qctaskEnd_Qcmsindexs = qctaskDelete_Qcmsindexs+[filtered_df.index[-1]]
                            #可能会出现qctaskEnd_Qcmsindexs为空的情况


                            #任务删除的节点
                            # qctaskEnd_Qcmsindexs=qctaskEnd_Qcmsindexs+qctaskDelete_Qcmsindexs
                            qctaskEnd_Qcmsindexs=sorted(qctaskEnd_Qcmsindexs)




                            #得到qctaskstart的index的长度，假设endqctask跟startqctask是同样的长度
                            length_qctaskStart_Qcmsindexs = len(qctaskStart_Qcmsindexs)
                            for qctask_index in range(length_qctaskStart_Qcmsindexs):#遍历查询出来的qctask的start的对应几条数据，有多少条数据就得画几个长方形
                                qctaskstart = filtered_df.iloc[qctaskStart_Qcmsindexs[qctask_index]]#开始的某一行数据
                                #当索引超出时，将结束的配对为开始时间
                                try:
                                    qctaskend = filtered_df.iloc[qctaskEnd_Qcmsindexs[qctask_index]]#结束的某一行数据
                                except IndexError:
                                    qctaskend = filtered_df.iloc[qctaskStart_Qcmsindexs[qctask_index]]

                                #同时需要知道更新到哪个子图中
                                figure_location_list_qctask = ast.literal_eval('[1,1]')
                                rowcount=figure_location_list_qctask[0]
                                colcount = figure_location_list_qctask[1]

                                if qctaskend['DATA_FROM']=='QCMSDB.QC_TOS_TASK_HIS.TRIGGER_ACTION(DELETE).TRIG_CREATED':#如果是删除的数据
                                    color_for_task=color_for_gantt_red
                                    namefortask=f"""QCMS删除过的任务{qctaskend['TASK_ID']}信息，任务类型:{qctaskend['TASK_TYPE']},任务状态:{qctaskend['TASK_STATUS']},任务初始位置:{qctaskend['ORIG_WSLOC']},任务目的位置:{qctaskend['DEST_WS_LOC']}"""
                                else:
                                    color_for_task = color_for_simple_green
                                    namefortask = f"""QCMS整个任务{qctaskend['TASK_ID']}持续的时间，任务类型:{qctaskend['TASK_TYPE']},任务状态:{qctaskend['TASK_STATUS']},任务初始位置:{qctaskend['ORIG_WSLOC']},任务目的位置:{qctaskend['DEST_WS_LOC']}"""
                                fig.add_trace(go.Scatter(x=[qctaskstart['KEYTIME'],qctaskstart['KEYTIME'],qctaskend['KEYTIME'],qctaskend['KEYTIME'],qctaskstart['KEYTIME']],y=[qcms_min,qcms_max,qcms_max,qcms_min,qcms_min],fill='toself',fillcolor=color_for_task,line=dict(color='black'),name=namefortask),row=1,col=1)
                                # # 注意：这里的位置是手动设置的，可能需要根据实际情况调整
                                #计算持续时间
                                durationforqctask=qctaskend['KEYTIME']-qctaskstart['KEYTIME']
                                durationforqctask = timeChange(durationforqctask)
                                fig.add_annotation(
                                    dict(x=qctaskstart['KEYTIME'] + (qctaskend['KEYTIME'] - qctaskstart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                         y=(qcms_min+qcms_max)/2,  # y 位置为矩形高度的一半
                                         text=f'{durationforqctask}',
                                         showarrow=False, xref="x1", yref="y1"))
                            ###############画出QCMS发送的qctask指令的填充图




                            ###############画出QCMS发送的park指令的填充图
                            # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                            # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.QC_TROLLEY_INSTRUCTION.Park.START_TIME'的行
                            matching_rows_for_parkstart = filtered_df[
                                filtered_df['DATA_FROM'] == 'QCMSDB.QC_TROLLEY_INSTRUCTION.Park.START_TIME']
                            # 从匹配的行中提取索引值
                            parkStart_Qcmsindexs = matching_rows_for_parkstart.index.tolist()#[1,3]查询出来的是个列表

                            # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.QC_TROLLEY_INSTRUCTION.Park.END_TIME'的行
                            matching_rows_for_parkend = filtered_df[
                                filtered_df['DATA_FROM'] == 'QCMSDB.QC_TROLLEY_INSTRUCTION.Park.END_TIME']
                            # 从匹配的行中提取索引值
                            parkEnd_Qcmsindexs = matching_rows_for_parkend.index.tolist()#[1,3]查询出来的是个列表

                            if len(parkStart_Qcmsindexs) == len(parkEnd_Qcmsindexs):  # 只有2个长度相等时才画图

                                #得到parkstart的index的长度，假设endpark跟startpark是同样的长度
                                length_parkStart_Qcmsindexs = len(parkStart_Qcmsindexs)
                                for park_index in range(length_parkStart_Qcmsindexs):#遍历查询出来的park的start的对应几条数据，有多少条数据就得画几个长方形
                                    parkstart = filtered_df.iloc[parkStart_Qcmsindexs[park_index]]#第一行数据
                                    parkend = filtered_df.iloc[parkEnd_Qcmsindexs[park_index]]
                                    #同时需要知道更新到哪个子图中
                                    figure_location_list_park = ast.literal_eval('[1,1]')
                                    rowcount=figure_location_list_park[0]
                                    colcount = figure_location_list_park[1]

                                    fig.add_trace(go.Scatter(x=[parkstart['KEYTIME'],parkstart['KEYTIME'],parkend['KEYTIME'],parkend['KEYTIME'],parkstart['KEYTIME']],y=[qcmstask_min,qcmstask_max,qcmstask_max,qcmstask_min,qcmstask_min],fill='toself',fillcolor=color_for_simple_green,line=dict(color='black'),name='QCMS发送Park指令到完成的时间'),row=1,col=1)
                                    # # 注意：这里的位置是手动设置的，可能需要根据实际情况调整
                                    #计算持续时间
                                    durationforpark=parkend['KEYTIME']-parkstart['KEYTIME']
                                    durationforpark = timeChange(durationforpark)
                                    fig.add_annotation(
                                        dict(x=parkstart['KEYTIME'] + (parkend['KEYTIME'] - parkstart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                             y=(qcmstask_min+qcmstask_max)/2,  # y 位置为矩形高度的一半
                                             text=f'{durationforpark}',
                                             showarrow=False, xref=f"x1", yref=f"y1"))
                            ###############画出QCMS发送的park指令的填充图



                            ###############画出QCMS发送的Pickup指令的填充图
                            # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                            # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.QC_TROLLEY_INSTRUCTION.Pickup.START_TIME'的行
                            matching_rows_for_pickupstart = filtered_df[
                                filtered_df['DATA_FROM'] == 'QCMSDB.QC_TROLLEY_INSTRUCTION.Pickup.START_TIME']
                            # 从匹配的行中提取索引值
                            pickupStart_Qcmsindexs = matching_rows_for_pickupstart.index.tolist()#[1,3]查询出来的是个列表

                            # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.QC_TROLLEY_INSTRUCTION.pickup.END_TIME'的行
                            matching_rows_for_pickupend = filtered_df[
                                filtered_df['DATA_FROM'] == 'QCMSDB.QC_TROLLEY_INSTRUCTION.Pickup.END_TIME']
                            # 从匹配的行中提取索引值
                            pickupEnd_Qcmsindexs = matching_rows_for_pickupend.index.tolist()#[1,3]查询出来的是个列表

                            if len(pickupStart_Qcmsindexs)==len(pickupEnd_Qcmsindexs):#只有2个长度相等时才画图
                                #得到pickupstart的index的长度，假设endpickup跟startpickup是同样的长度
                                length_pickupStart_Qcmsindexs = len(pickupStart_Qcmsindexs)
                                for pickup_index in range(length_pickupStart_Qcmsindexs):#遍历查询出来的pickup的start的对应几条数据，有多少条数据就得画几个长方形
                                    pickupstart = filtered_df.iloc[pickupStart_Qcmsindexs[pickup_index]]#第一行数据
                                    pickupend = filtered_df.iloc[pickupEnd_Qcmsindexs[pickup_index]]
                                    #同时需要知道更新到哪个子图中
                                    figure_location_list_pickup = ast.literal_eval('[1,1]')
                                    rowcount=figure_location_list_pickup[0]
                                    colcount = figure_location_list_pickup[1]

                                    fig.add_trace(go.Scatter(x=[pickupstart['KEYTIME'],pickupstart['KEYTIME'],pickupend['KEYTIME'],pickupend['KEYTIME'],pickupstart['KEYTIME']],y=[qcmstask_min,qcmstask_max,qcmstask_max,qcmstask_min,qcmstask_min],fill='toself',text='QCMS发送Pickup指令到指令完成时间',fillcolor=color_for_simple_green,line=dict(color='black'),name='QCMS发送Pickup指令到完成的时间'),row=1,col=1)
                                    # # 注意：这里的位置是手动设置的，可能需要根据实际情况调整
                                    # 计算持续时间
                                    durationforpickup = pickupend['KEYTIME'] - pickupstart['KEYTIME']
                                    durationforpickup = timeChange(durationforpickup)
                                    fig.add_annotation(
                                        dict(x=pickupstart['KEYTIME'] + (pickupend['KEYTIME'] - pickupstart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                             y=(qcmstask_min+qcmstask_max)/2,  # y 位置为矩形高度的一半
                                             text=f'{durationforpickup}',
                                             showarrow=False, xref=f"x1", yref=f"y1"))
                            ###############画出QCMS发送的Pickup指令的填充图




                            ###############画出QCMS发送的Ground指令的填充图
                            # 假设 filtered_df 是你的 DataFrame，且包含 DATA_FROM 列,filtered_df类型=<class 'pandas.core.frame.DataFrame'>
                            # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.QC_TROLLEY_INSTRUCTION.Ground.START_TIME'的行
                            matching_rows_for_groundstart = filtered_df[
                                filtered_df['DATA_FROM'] == 'QCMSDB.QC_TROLLEY_INSTRUCTION.Ground.START_TIME']
                            # 从匹配的行中提取索引值
                            groundStart_Qcmsindexs = matching_rows_for_groundstart.index.tolist()#[1,3]查询出来的是个列表

                            # 使用布尔索引来查找 DATA_FROM 字段等于特定值'QCMSDB.QC_TROLLEY_INSTRUCTION.Ground.END_TIME'的行
                            matching_rows_for_groundend = filtered_df[
                                filtered_df['DATA_FROM'] == 'QCMSDB.QC_TROLLEY_INSTRUCTION.Ground.END_TIME']
                            # 从匹配的行中提取索引值
                            groundEnd_Qcmsindexs = matching_rows_for_groundend.index.tolist()#[1,3]查询出来的是个列表


                            if len(groundStart_Qcmsindexs)==len(groundEnd_Qcmsindexs):#判断只有当2个长度相等时才画
                                #得到groundstart的index的长度，假设endground跟startground是同样的长度
                                length_groundStart_Qcmsindexs = len(groundStart_Qcmsindexs)
                                for ground_index in range(length_groundStart_Qcmsindexs):#遍历查询出来的ground的start的对应几条数据，有多少条数据就得画几个长方形
                                    groundstart = filtered_df.iloc[groundStart_Qcmsindexs[ground_index]]#第一行数据
                                    groundend = filtered_df.iloc[groundEnd_Qcmsindexs[ground_index]]

                                    #同时需要知道更新到哪个子图中
                                    figure_location_list = ast.literal_eval('[1,1]')
                                    rowcount=figure_location_list[0]
                                    colcount = figure_location_list[1]


                                    fig.add_trace(go.Scatter(x=[groundstart['KEYTIME'],groundstart['KEYTIME'],groundend['KEYTIME'],groundend['KEYTIME'],groundstart['KEYTIME']],y=[qcmstask_min,qcmstask_max,qcmstask_max,qcmstask_min,qcmstask_min],fill='toself',fillcolor=color_for_simple_green,line=dict(color='black'),name='QCMS发送Ground指令到完成的时间'),row=1,col=1)
                                    # # 注意：这里的位置是手动设置的，可能需要根据实际情况调整
                                    # 计算持续时间
                                    durationforground = groundend['KEYTIME'] - groundstart['KEYTIME']
                                    durationforground = timeChange(durationforground)
                                    fig.add_annotation(
                                        dict(x=groundstart['KEYTIME'] + (groundend['KEYTIME'] - groundstart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                             y=(qcmstask_min+qcmstask_max)/2,  # y 位置为矩形高度的一半
                                             text=f'{durationforground}',
                                             showarrow=False, xref=f"x1", yref=f"y1"))#x2表示第2个x坐标轴，第2个y坐标轴
                            ###############画出QCMS发送的ground指令的填充图


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

                                matching_rows_for_stepstart = filtered_df[
                                    filtered_df['DATA_FROM'] == f'KPIDB.kpi_mt_step_log.{step}.start_time']
                                # 从匹配的行中提取索引值
                                stepStart_kpiindexs = matching_rows_for_stepstart.index.tolist()#[1,3]查询出来的是个列表

                                # 使用布尔索引来查找 DATA_FROM 字段等于特定值'kpiDB.QC_TROLLEY_INSTRUCTION.step.END_TIME'的行
                                matching_rows_for_stepend = filtered_df[
                                    filtered_df['DATA_FROM'] == f'KPIDB.kpi_mt_step_log.{step}.end_time']
                                # 从匹配的行中提取索引值
                                stepEnd_kpiindexs = matching_rows_for_stepend.index.tolist()#[1,3]查询出来的是个列表

                                if len(stepStart_kpiindexs) == len(stepEnd_kpiindexs):  # 判断只有当2个长度相等时才画

                                    #得到stepstart的index的长度，假设endstep跟startstep是同样的长度
                                    length_stepStart_kpiindexs = len(stepStart_kpiindexs)
                                    for step_index in range(length_stepStart_kpiindexs):#遍历查询出来的step的start的对应几条数据，有多少条数据就得画几个长方形
                                        stepstart = filtered_df.iloc[stepStart_kpiindexs[step_index]]#第一行数据
                                        stepend = filtered_df.iloc[stepEnd_kpiindexs[step_index]]
                                        #同时需要知道更新到哪个子图中
                                        figure_location_list = ast.literal_eval('[1,1]')
                                        rowcount=figure_location_list[0]
                                        colcount = figure_location_list[1]
                                        fig.add_trace(go.Scatter(x=[stepstart['KEYTIME'],stepstart['KEYTIME'],stepend['KEYTIME'],stepend['KEYTIME'],stepstart['KEYTIME']],y=[kpistep_min,kpistep_max,kpistep_max,kpistep_min,kpistep_min],fill='toself',fillcolor=color_for_kpi,line=dict(color='black'),name=f"kpi发送step={step}指令{stepTransToLanguage(step)}到完成的时间"),row=1,col=1)
                                        # # 注意：这里的位置是手动设置的，可能需要根据实际情况调整
                                        # 计算持续时间
                                        durationforstep = stepend['KEYTIME'] - stepstart['KEYTIME']
                                        durationforstep=timeChange(durationforstep)
                                        fig.add_annotation(
                                            dict(x=stepstart['KEYTIME'] + (stepend['KEYTIME'] - stepstart['KEYTIME']) / 2,  # x 位置为两个 KEYTIME 的中点
                                                 y=(kpistep_min+kpistep_max)/2,  # y 位置为矩形高度的一半
                                                 text=f"""{durationforstep}""",
                                                 showarrow=False, xref=f"x1", yref=f"y1"))

                                ###############画出KPI发送的各种step指令的填充图
                                #######################################创建QCMS单个指令和单机KPI指令单个完成时间的逻辑




                        filenameFortime = str(wholeHourTimeforSts).replace('-', '').replace(' ', '').replace(':', '').replace('.', '')

                        #打入文件夹的开始时间
                        filenameFortime1 = str(wholeTimeTasksQueryResults.iloc[0]['KEYTIME']).replace('-', '').replace(' ', '').replace(':', '').replace('.', '')
                        filenameFortime1=filenameFortime1[:8]
                        titleforsingle=f'''岸桥{stsid}的{filenameFortime}整点时间内所有任务明细'''
                        fig.update_layout(title=titleforsingle)#更新整张表的标题

                        output_path=f"./{filenameFortime1}documents_vbt_id_{wholeTimeTasksQueryResults.iloc[0]['VBT_ID']}/{stsid}/岸桥{stsid}的整点时间内{filenameFortime}任务明细.html"
                        # 提取目录路径
                        directory = os.path.dirname(output_path)
                        # 检查目录是否存在，如果不存在则创建
                        if not os.path.exists(directory):
                            os.makedirs(directory)

                        #保存图形到html格式
                        fig.write_html(output_path, full_html=False)

