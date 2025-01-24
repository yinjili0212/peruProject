from basicFunctionDefine import *
import sqliteHandle
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import os
import yaml
#########
envlist = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
#########
'''
1.根据船舶作业的开始时间和结束时间，画出每个岸桥作业的毛效率图
'''
#########
o=sqliteHandle.sqliteHandler(envlist['sqllitedbaddress'])
# stsNos=[103,104,105,106,107,108]
# 212427
# 212507
qcms_kpi_for_container_transfer=envlist['qcms_kpi_for_container_transfer_tablename']
qc_tos_task=envlist['qc_tos_task_tablename']
VBT_ID=envlist['VBT_ID']


querySqlStsNos = f"""select DISTINCT STS_NO from {qc_tos_task}  where VBT_ID={VBT_ID} and RESPONSE_TIME!='' order by STS_NO asc"""
# querySqlStsNos = f"""select DISTINCT STS_NO from {qc_tos_task}  where RESPONSE_TIME!='' order by STS_NO asc"""
stsNosQueryResults = o.query(querySqlStsNos,t='df')
if isinstance(stsNosQueryResults,pd.DataFrame):
    stsNos = stsNosQueryResults['STS_NO'].tolist()####打印出来设备编号
    subplot_for_titles=[f'{i}岸桥效率' for i in stsNos]
    # 创建一个包含N行1列子图的图表
    # fig = make_subplots(rows=len(stsNos), cols=1,subplot_titles=(f'{stsNo}' for stsNo in stsNos))
    fig = make_subplots(rows=len(stsNos), cols=1, subplot_titles=(subplot_for_titles))
    #设置一个此船舶作业量的初始值
    stsnosloadcounts =0
    stsnosdschcounts = 0
    stsnosotherscounts = 0
    #查询某个岸桥的qc_tos_Task，从而得到整条船作业的时间
    for stsNo_index,stsNo in enumerate(stsNos):#遍历岸桥编号
        #############步骤1：查询当前岸桥当前船舶VBT_ID下作业的开始时间和结束时间
        querySqlForQcTosTasks = f"""select * from {qc_tos_task}  where STS_NO={stsNo} and VBT_ID={VBT_ID} and RESPONSE_TIME!='' order by RESPONSE_TIME asc"""
        # querySqlForQcTosTasks = f"""select * from {qc_tos_task}  where STS_NO={stsNo} and RESPONSE_TIME!='' order by RESPONSE_TIME asc"""
        QcTosTaskQueryResult_dfs = o.query(querySqlForQcTosTasks,t='df')
        if isinstance(QcTosTaskQueryResult_dfs, pd.DataFrame):#能查询出来数据并且是pan.DataFrame类型
            minTimeQcTosTask=QcTosTaskQueryResult_dfs['RESPONSE_TIME'].min()
            maxTimeQcTosTask=QcTosTaskQueryResult_dfs['RESPONSE_TIME'].max()
            # print(stsNo)
            # print(minTimeQcTosTask,maxTimeQcTosTask)


            #########查询qcms_kpi_for_container_transfer
            querySqlForQcmsKpiForCtnTransfer = f"""select * from {qcms_kpi_for_container_transfer} where GROUND_TIME>='{minTimeQcTosTask}' and GROUND_TIME<='{maxTimeQcTosTask}' and QC_ID={stsNo} order by GROUND_TIME asc"""
            qcmsKpiForCtnTransferQueryResult_dfs = o.query(querySqlForQcmsKpiForCtnTransfer,t='df')



            if isinstance(qcmsKpiForCtnTransferQueryResult_dfs,pd.DataFrame):#能查询出来数据并且是pan.DataFrame类型
                # 将 GROUND_TIME 列转换为日期时间格式（如果它还不是）
                qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME'] = pd.to_datetime(qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME'])#将str改为时间格式
                qcmsKpiForCtnTransferQueryResult_dfs['PICKUP_TIME'] = pd.to_datetime(qcmsKpiForCtnTransferQueryResult_dfs['PICKUP_TIME'])  # 将str改为时间格式
                # 计算时间差，并过滤出时间差大于30秒的记录抓放箱记录
                time_difference = qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME'] - qcmsKpiForCtnTransferQueryResult_dfs['PICKUP_TIME']
                qcmsKpiForCtnTransferQueryResult_dfs = qcmsKpiForCtnTransferQueryResult_dfs[time_difference > timedelta(seconds=30)]

                # print(minTimeQcTosTask,maxTimeQcTosTask)
                stsnoWholeTimes = wholeHourTimeEnds(minTimeQcTosTask,maxTimeQcTosTask)#['2025-01-02 11:00:00', '2025-01-02 12:00:00']

                taskcounts=[i for i in range(len(stsnoWholeTimes))]#先弄一个初始空的list，后面会替换掉
                if len(stsnoWholeTimes)!=0:#只要能查询来数据就要将对应的stsno的数据画出来
                    for index_time,stsnoWholeTime in enumerate(stsnoWholeTimes):#遍历整点时间
                        #每个时间节点上任务类型的字典定义
                        dictForTaskCounts={'LOAD':0,'DSCH':0,'Others':0}

                        ####对move筛选条件做统一管理
                        if index_time==0:
                            loadFilterCondition= (qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME']<f"""{pd.to_datetime(stsnoWholeTimes[index_time])}""") & (qcmsKpiForCtnTransferQueryResult_dfs['TASK_TYPE']=='LOAD')
                            dschFilterCondition= (qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME']<f"""{pd.to_datetime(stsnoWholeTimes[index_time])}""") & (qcmsKpiForCtnTransferQueryResult_dfs['TASK_TYPE']=='DSCH')
                            othersFilterCondition= (qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME']<f"""{pd.to_datetime(stsnoWholeTimes[index_time])}""") & (qcmsKpiForCtnTransferQueryResult_dfs['TASK_TYPE']=='')
                        else:
                            loadFilterCondition= (qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME']>=f"""{pd.to_datetime(stsnoWholeTimes[index_time-1])}""") &(qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME']<f"""{pd.to_datetime(stsnoWholeTimes[index_time])}""")& (qcmsKpiForCtnTransferQueryResult_dfs['TASK_TYPE']=='LOAD')
                            dschFilterCondition= (qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME']>=f"""{pd.to_datetime(stsnoWholeTimes[index_time-1])}""") &(qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME']<f"""{pd.to_datetime(stsnoWholeTimes[index_time])}""")& (qcmsKpiForCtnTransferQueryResult_dfs['TASK_TYPE']=='DSCH')
                            othersFilterCondition= (qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME']>=f"""{pd.to_datetime(stsnoWholeTimes[index_time-1])}""") &(qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME']<f"""{pd.to_datetime(stsnoWholeTimes[index_time])}""")& (qcmsKpiForCtnTransferQueryResult_dfs['TASK_TYPE']=='')
                        ####对筛选条件做统一管理


                        #对装船move数量做的筛选filter_load_dfs.shape[0]对筛选数据做条数统计
                        filter_load_dfs=qcmsKpiForCtnTransferQueryResult_dfs[loadFilterCondition]
                        dictForTaskCounts['LOAD']=dictForTaskCounts['LOAD']+filter_load_dfs.shape[0]

                        filter_dsch_dfs=qcmsKpiForCtnTransferQueryResult_dfs[dschFilterCondition]
                        dictForTaskCounts['DSCH']=dictForTaskCounts['DSCH']+filter_dsch_dfs.shape[0]

                        filter_others_dfs=qcmsKpiForCtnTransferQueryResult_dfs[othersFilterCondition]
                        dictForTaskCounts['Others']=dictForTaskCounts['Others']+filter_others_dfs.shape[0]
                        # print(dictForTaskCounts)


                        ###############得出每个整点的move数量，明细[{'LOAD': 0, 'DSCH': 3, 'Others': 1}, {'LOAD': 0, 'DSCH': 5, 'Others': 1}]
                        taskcounts[index_time]=dictForTaskCounts
                        ###############得出每个整点的任务，明细



                    #######################根据每个岸桥的任务画子图
                    # 提取LOAD值，并创建一个标签列表用于x轴
                    load_values = [dictForFig['LOAD'] for dictForFig in taskcounts]
                    dsch_values = [dictForFig['DSCH'] for dictForFig in taskcounts]
                    others_values = [dictForFig['Others'] for dictForFig in taskcounts]
                    # print(stsNo,stsnoWholeTimes,taskcounts)



                    # 计算相加后的 y 值
                    scatteervalues_stacked = [a + b+c for a, b,c in zip(load_values, dsch_values,others_values)]
                    print(f"{VBT_ID}岸桥效率明细")
                    # print(stsNo,stsnoWholeTimes,scatteervalues_stacked)
                    # print("************")
                    #注释text
                    load_texts=['LOAD '+str(dictForFig['LOAD']) for dictForFig in taskcounts]
                    dsch_texts = ['DSCH ' + str(dictForFig['DSCH']) for dictForFig in taskcounts]
                    others_texts = ['Others ' + str(dictForFig['Others']) for dictForFig in taskcounts]
                    alltasks_texts = ['all ' + str(dictForFig) for dictForFig in scatteervalues_stacked]




                    #单独的类型条形图，
                    fig.add_trace(go.Bar(x=stsnoWholeTimes, y=load_values,marker=dict(color='#782D32'),name='LOAD',text=load_texts,textposition='inside'), row=stsNo_index+1, col=1)#'outside';'inside';'auto';'none'
                    fig.add_trace(go.Bar(x=stsnoWholeTimes, y=dsch_values,marker=dict(color='#323A45'),name='DSCH',text=dsch_texts,textposition='inside'), row=stsNo_index + 1, col=1)
                    fig.add_trace(go.Bar(x=stsnoWholeTimes, y=others_values,marker=dict(color='#FED961'),name='OtherTask',text=others_texts,textposition='inside'), row=stsNo_index + 1, col=1)


                    #添加散点图
                    fig.add_trace(go.Scatter(
                        x=stsnoWholeTimes,
                        y=scatteervalues_stacked,
                        mode='lines+markers',  # 绘制散点并连接成线
                        name='岸桥毛效率趋势图',
                        # text=scatteervalues_stacked,  # 设置每个点的文本标签
                        # textposition='outside',  # 设置每个点的文本标签
                        line=dict(color='black', width=2, dash='solid'),
                        marker=dict(size=5,symbol='square')  # 可选：设置散点的大小
                    ), row=stsNo_index+1, col=1)


                    for indexFortext,stsnoWholeTime in enumerate(stsnoWholeTimes):
                        fig.add_annotation(
                            dict(x=stsnoWholeTime,
                                 y=scatteervalues_stacked[indexFortext]+1,  # y 位置
                                 text=scatteervalues_stacked[indexFortext],
                                 showarrow=False,font=dict(size=18,color='red'),xref=f"x{stsNo_index+1}", yref=f"y{stsNo_index+1}"))# font=dict(size=20,color='red'),

                    # 更新子图的x轴和y轴文字标注
                    fig.update_xaxes(title_text='时间整点', row=stsNo_index + 1, col=1)
                    fig.update_yaxes(title_text=f'效率 move/h', row=stsNo_index + 1, col=1)
        stsnoloadcounts=sum(load_values)#当前岸桥装船任务量
        stsnodschcounts = sum(dsch_values)#当前岸桥卸船任务量
        stsnootherscounts = sum(others_values)#当前岸桥QCMS无法统计类型但司机（抓-放）>30s的任务统计

        stsnosloadcounts = stsnosloadcounts +stsnoloadcounts#总的岸桥的总的装船任务量
        stsnosdschcounts = stsnosdschcounts + stsnodschcounts  # 总的岸桥的总的卸船任务量
        stsnosotherscounts = stsnosotherscounts + stsnootherscounts  # 总岸桥任务但是QCMS无法统计类型，且司机（抓-放）>30s的任务统计
        print(f"""{stsNo}对应时间{stsnoWholeTimes}的任务总量：LOAD={load_values},DSCH={dsch_values},others={others_values},{stsnoloadcounts},{stsnodschcounts},{stsnootherscounts}""")
    # 更新图表的布局，例如为两个子图分别设置标题
    # 设置布局以堆叠条形图
    fig.update_layout(barmode='stack')#'group'分组，stack堆叠，overlay覆盖；relative相对
    fig.update_layout(
        title_text=f"船舶{VBT_ID}岸桥毛效率图，开始作业时间={minTimeQcTosTask},结束时间={maxTimeQcTosTask}，总move量={stsnosloadcounts+stsnosdschcounts+stsnosotherscounts}，装船move量={stsnosloadcounts}，卸船move量={stsnosdschcounts}，QCMS无法统计类型但司机（from抓-to放）>30s的move数量={stsnosotherscounts}")
    # fig.show()
    output_path = f"./documents_vbt_id/船舶{VBT_ID}岸桥毛效率.html"
    # 提取目录路径
    directory = os.path.dirname(output_path)
    # 检查目录是否存在，如果不存在则创建
    if not os.path.exists(directory):
        os.makedirs(directory)

    # 保存图形到html格式
    fig.write_html(output_path, full_html=False)














