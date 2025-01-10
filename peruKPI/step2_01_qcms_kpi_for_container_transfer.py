from basicFunctionDefine import *
import sqliteHandle

o=sqliteHandle.sqliteHandler(r'./kpiforQcms20250109.db')

qcms_kpi_for_container_transfer='qcms_kpi_for_container_transfer'
qc_tos_task='qc_tos_task'
qc_container_transfer='qc_container_transfer'
qc_trolley_task='qc_trolley_task'


# 212427
# 212507
VBT_ID=212427


stsNos=[103,104,105,106,107,108]
# stsNos=[103]
#查询某个岸桥的qc_tos_Task，从而得到整条船作业的时间
for stsNo_index,stsNo in enumerate(stsNos):#遍历岸桥编号
    #############步骤1：查询当前岸桥当前船舶VBT_ID下作业的开始时间和结束时间
    querySqlForQcTosTasks = f"""select * from {qc_tos_task}  where STS_NO={stsNo} and VBT_ID={VBT_ID} and RESPONSE_TIME!='' order by RESPONSE_TIME asc"""
    QcTosTaskQueryResults = o.query(querySqlForQcTosTasks)
    if len(QcTosTaskQueryResults)>=2:#如果检测到查询出来的数据大于=2条，才可
        minTimeQcTosTask = QcTosTaskQueryResults[0]['RESPONSE_TIME']#这条船的当前岸桥的最小时间,str类型
        maxTimeQcTosTask = QcTosTaskQueryResults[-1]['RESPONSE_TIME']#这条船的当前岸桥的最大时间str类型
        #################步骤2:根据计算出来的时间，查询qc_container_transfer表的开始时间和结束时间的TRANS_CHAIN_ID
        querySqlForQcContainerTransfer = f"""select DISTINCT TRANS_CHAIN_ID from {qc_container_transfer} where QC_ID={stsNo} and CREATE_TIME>='{minTimeQcTosTask}' and CREATE_TIME<='{maxTimeQcTosTask}' order by CREATE_TIME asc"""
        qcContainerTransferQueryResults = o.query(querySqlForQcContainerTransfer)
        if len(qcContainerTransferQueryResults)!=0:#能查询出来数据
            ##########步骤3：遍历TRANS_CHAIN_ID在qc_trolley_task能不能查到，查到继续从QC_TOS_TASK中得到TASK_ID和VBT_ID
            for qcContainerTransferQueryResult in qcContainerTransferQueryResults:
                querySqlForQcTrolleyTask = f"""select * from {qc_trolley_task} where TRANS_CHAIN_ID='{qcContainerTransferQueryResult['TRANS_CHAIN_ID']}'"""
                qcTrolleyTaskQueryResults = o.query(querySqlForQcTrolleyTask)
                # TRANS_CHAIN_ID=qcContainerTransferQueryResult['TRANS_CHAIN_ID']
                #将qc_container_transfer中这个TRANS_CHAIN_ID对应的抓箱和放箱时间都找出来
                if len(qcTrolleyTaskQueryResults)==0:#表示没有查询出来数据，QCMS没有对应的任务，说明是手动做的，但是也是需要插入的qcms_kpi_for_container_transfer
                    #QC_ID=stsNo
                    #TRANS_CHAIN_ID=qcContainerTransferQueryResult['TRANS_CHAIN_ID']
                    #TASK_ID=0
                    #VBT_ID
                    insertSqlForQcmsKpiForCtnTransfer1 = f"""INSERT INTO {qcms_kpi_for_container_transfer}(QC_ID, TRANS_CHAIN_ID, TASK_ID, TASK_TYPE,VBT_ID)VALUES({stsNo}, '{qcContainerTransferQueryResult['TRANS_CHAIN_ID']}',0,'',0)"""
                    try:
                        o.executesql(insertSqlForQcmsKpiForCtnTransfer1)
                        print(datetime.now())
                    except Exception as e:  # 涉及到唯一值的异常
                        pass
                elif len(qcTrolleyTaskQueryResults)!=0:#表示能查询出来数据，对应的qc_container_transfer在QC_trolley_task能查询出来任务
                    # QC_ID=stsNo
                    # TRANS_CHAIN_ID=qcContainerTransferQueryResult['TRANS_CHAIN_ID']
                    # TASK_ID=qcTrolleyTaskQueryResults[0]['TASK_REF_ID']
                    # VBT_ID={qcTosTask2EQueryResults[0]['VBT_ID']}
                    querySqlForQcTosTask2s = f"""select * from {qc_tos_task}  where TASK_ID={qcTrolleyTaskQueryResults[0]['TASK_REF_ID']} order by RESPONSE_TIME asc"""
                    qcTosTask2EQueryResults = o.query(querySqlForQcTosTask2s)
                    if len(qcTosTask2EQueryResults)!=0:#能查询出来数据
                        # VBT_ID=qcTosTask2EQueryResults[0]['VBT_ID']
                        insertSqlForQcmsKpiForCtnTransfer =f"""INSERT INTO {qcms_kpi_for_container_transfer}(QC_ID, TRANS_CHAIN_ID, TASK_ID, TASK_TYPE,VBT_ID)VALUES({stsNo}, '{qcContainerTransferQueryResult['TRANS_CHAIN_ID']}', {qcTrolleyTaskQueryResults[0]['TASK_REF_ID']}, '{qcTosTask2EQueryResults[0]['TASK_TYPE']}',{qcTosTask2EQueryResults[0]['VBT_ID']})"""
                        try:
                            o.executesql(insertSqlForQcmsKpiForCtnTransfer)
                        except Exception as e:#涉及到唯一值的异常
                            pass
#步骤3，更新qcms_kpi_for_container_transfer剩下的字段PICKUP_LOCATION/GROUND_LOCATION/PICKUP_TIME/GROUND_TIME/Pickup_OPERATE_MODE/Ground_OPERATE_MODE/SPREADER_SIZE
for stsNo_index,stsNo in enumerate(stsNos):#遍历岸桥编号
    querySqlQcmsKpiForContainerTransfer=f"""select DISTINCT TRANS_CHAIN_ID from {qcms_kpi_for_container_transfer} where QC_ID={stsNo}"""
    qcmsKpiForContainerTransferQueryResults=o.query(querySqlQcmsKpiForContainerTransfer)
    if len(qcmsKpiForContainerTransferQueryResults)!=0:#能查询出来数据
        for qcmsKpiForContainerTransferQueryResult in qcmsKpiForContainerTransferQueryResults:#遍历TRANS_CHAIN_ID
            #TRANS_CHAIN_ID=qcmsKpiForContainerTransferQueryResult['TRANS_CHAIN_ID']
            querySqlQcContainerTransfer=f"""select * from {qc_container_transfer} where TRANS_CHAIN_ID='{qcmsKpiForContainerTransferQueryResult['TRANS_CHAIN_ID']}'"""
            qcContainerTransferQueryResults = o.query(querySqlQcContainerTransfer)
            if len(qcContainerTransferQueryResults)!=0:#能查询出来数据，说明能找到INSTR_TYPE=Pickup和INSTR_TYPE=Ground的任务
                ########在此设置更新的脚本
                #######
                for qcContainerTransferQueryResult in qcContainerTransferQueryResults:#遍历查询到的qc_container_transfer的数据
                    if qcContainerTransferQueryResult['INSTR_TYPE']=='Pickup' and qcContainerTransferQueryResult['GANTRY_POSITION']!=0:#更新到数据库qcms_kpi_for_contaienr_transfer中
                        updateSqlForQcmsKpiForCtnTransfer = f"""update {qcms_kpi_for_container_transfer} set
                        PICKUP_LOCATION='{qcContainerTransferQueryResult['WORK_LOCATION']}',
                        PICKUP_TIME='{qcContainerTransferQueryResult['CREATE_TIME']}',
                        Pickup_OPERATE_MODE='{qcContainerTransferQueryResult['OPERATE_MODE']}',
                        SPREADER_SIZE='{qcContainerTransferQueryResult['SPREADER_SIZE']}' where TRANS_CHAIN_ID='{qcmsKpiForContainerTransferQueryResult['TRANS_CHAIN_ID']}'"""
                        o.executesql(updateSqlForQcmsKpiForCtnTransfer)
                        print(datetime.now())
                    if qcContainerTransferQueryResult['INSTR_TYPE'] == 'Ground':  # 更新到数据库qcms_kpi_for_contaienr_transfer中
                        updateSqlForQcmsKpiForCtnTransfer = f"""update {qcms_kpi_for_container_transfer} set
                        GROUND_LOCATION='{qcContainerTransferQueryResult['WORK_LOCATION']}',
                        GROUND_TIME='{qcContainerTransferQueryResult['CREATE_TIME']}',
                        Ground_OPERATE_MODE='{qcContainerTransferQueryResult['OPERATE_MODE']}',
                        SPREADER_SIZE='{qcContainerTransferQueryResult['SPREADER_SIZE']}' where TRANS_CHAIN_ID='{qcmsKpiForContainerTransferQueryResult['TRANS_CHAIN_ID']}'"""
                        o.executesql(updateSqlForQcmsKpiForCtnTransfer)
                        print(datetime.now())









