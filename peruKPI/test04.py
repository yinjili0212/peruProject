from basicFunctionDefine import *
import sqliteHandle
import numpy as np
o=sqliteHandle.sqliteHandler('kpiforQcms20250103.db')
stsNos=[103,104,105,106,107,108]

qcms_kpi_for_container_transfer='qcms_kpi_for_container_transfer'
qc_tos_task='qc_tos_task'
# stsNos=[103]
#查询某个岸桥的qc_tos_Task，从而得到整条船作业的时间
for stsNo_index,stsNo in enumerate(stsNos):#遍历岸桥编号
    #############步骤1：查询当前岸桥当前船舶VBT_ID下作业的开始时间和结束时间
    querySqlForQcTosTasks = f"""select * from {qc_tos_task}  where STS_NO={stsNo} and VBT_ID=212448 and RESPONSE_TIME!='' order by RESPONSE_TIME asc"""
    QcTosTaskQueryResult_dfs = o.query(querySqlForQcTosTasks,t='df')
    if isinstance(QcTosTaskQueryResult_dfs, pd.DataFrame):#能查询出来数据并且是pan.DataFrame类型
        minTimeQcTosTask=QcTosTaskQueryResult_dfs['RESPONSE_TIME'].min()
        maxTimeQcTosTask=QcTosTaskQueryResult_dfs['RESPONSE_TIME'].max()

        #########查询qcms_kpi_for_container_transfer
        querySqlForQcmsKpiForCtnTransfer = f"""select * from {qcms_kpi_for_container_transfer} where GROUND_TIME>='{minTimeQcTosTask}' and GROUND_TIME<='{maxTimeQcTosTask}' and QC_ID={stsNo} order by GROUND_TIME asc"""
        qcmsKpiForCtnTransferQueryResult_dfs = o.query(querySqlForQcmsKpiForCtnTransfer,t='df')
        if isinstance(qcmsKpiForCtnTransferQueryResult_dfs,pd.DataFrame):#能查询出来数据并且是pan.DataFrame类型
            # 将 GROUND_TIME 列转换为日期时间格式（如果它还不是）
            qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME'] = pd.to_datetime(qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME'])#将str改为时间格式

            stsnoWholeTimes = wholeHourTimes(minTimeQcTosTask,maxTimeQcTosTask)#['2025-01-02 11:00:00', '2025-01-02 12:00:00']
            taskcounts=[]
            if len(stsnoWholeTimes)!=0:#只要能查询来数据就要将对应的stsno的数据画出来
                for index_time,stsnoWholeTime in enumerate(stsnoWholeTimes):#遍历整点时间
                    if (index_time+1)<len(stsnoWholeTimes):#只能处理除最后一个任务的值
                        filter_load_dfs=qcmsKpiForCtnTransferQueryResult_dfs[(qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME']>=f"""{pd.to_datetime(stsnoWholeTimes[index_time])}""" &
                                                                              qcmsKpiForCtnTransferQueryResult_dfs['GROUND_TIME']<f"""{pd.to_datetime(stsnoWholeTimes[index_time+1])}""" &
                                                                              qcmsKpiForCtnTransferQueryResult_dfs['TASK_TYPE']=='LOAD')]
                        print(filter_load_dfs)








