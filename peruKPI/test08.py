import sqliteHandle
import pandas as pd


o=sqliteHandle.sqliteHandler('kpiforQcms20250109.db')

qc_tp_interaction_his = 'qc_tp_interaction_his'
tablename_for_kpi='kpi_for_qcms'


###############
startTime='2025-01-08 12:57:15'
endTime='2025-01-08 18:17:41'



#############################################################################对QC_TP_INTERACTION_HIS的处理
qcnos = [103,104,105,106,107,108]
lanenos = [1,2,3,4,5,6,7]
for qcno in qcnos:
    for laneno in lanenos:
        querySqlForQcTpInteractionHis = f'''select * from {qc_tp_interaction_his} where QC_ID={qcno} and LANE_ID={laneno} and (TRIG_CREATED>'{startTime}' and  TRIG_CREATED<'{endTime}') ORDER BY TRIG_CREATED asc'''
        qcTpInteractionHisQuryResults = o.query(querySqlForQcTpInteractionHis,t='df')
        if isinstance(qcTpInteractionHisQuryResults,pd.DataFrame):
            # 遍历行并比较相邻行的字段值
            for indexForQcTp,qcTpInteractionHisQuryResult in qcTpInteractionHisQuryResults.iterrows():#####遍历每一行数据
                ############从第2行数据开始
                if indexForQcTp >= 1:  # 表示从第2行开始计算数据
                    last_row = qcTpInteractionHisQuryResults.iloc[indexForQcTp-1]#上一行数据
                    current_row = qcTpInteractionHisQuryResults.iloc[indexForQcTp]#当前行数据
                    # print(type(current_val))
                    # 遍历上一行的每个字段和对应的值
                    changedata = {}
                    for column in ['QC_ID','LANE_ID','FMS_JOB_POS','FMS_AHT_ID','FMS_MOVE_KIND','FMS_AHT_STATUS','QC_REF1','QC_REF2','QC_STATUS']:#遍历这些字段乳沟有变化记录
                        last_val = last_row[column]
                        current_val = current_row[column]

                        if last_val!=current_val:
                            changedata[column]=current_val
                    # print(changedata)

                    if changedata!={}:#如果当前行的有变化值
                        for key,valueForChange in changedata.items():
                            # 将字典的值转换为逗号分隔的字符串
                            changedatavalues = ','.join(changedata.values())
                            # print(changedatavalues)

                            DATA_FROM = f"""CMSDB.QC_TP_INTERACTION_HIS.{current_row['QC_ID']}.{current_row['LANE_ID']}.{changedatavalues}"""  # 需要改的部分
                            DATA_FROM_TYPE = 'QCMS'
                            NOTES = f'''交互状态{changedata}'''  # 需要改的部分
                            insertsql = f'''insert into {tablename_for_kpi}(\
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
                            print(insertsql)
                        o.executesql(insertsql)
#########################################################对QC_TP_INTERACTION_HIS的处理

