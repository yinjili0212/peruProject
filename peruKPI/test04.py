from basicFunctionDefine import *
import sqliteHandle

o=sqliteHandle.sqliteHandler('kpiforQcms20250103.db')
stsNos=[103,104,105,106,107,108]
# stsNos=[103]
#查询某个岸桥的qc_tos_Task，从而得到整条船作业的时间
for stsNo_index,stsNo in enumerate(stsNos):#遍历岸桥编号
    #############步骤1：查询当前岸桥当前船舶VBT_ID下作业的开始时间和结束时间
    querySqlForQcTosTasks = f"""select * from qc_tos_task  where STS_NO={stsNo} and VBT_ID=212407 order by RESPONSE_TIME asc"""
    QcTosTaskQueryResults = o.query(querySqlForQcTosTasks)
    if len(QcTosTaskQueryResults)>=2:#如果检测到查询出来的数据大于=2条，才可
        minTimeQcTosTask = QcTosTaskQueryResults[0]['RESPONSE_TIME']#这条船的当前岸桥的最小时间,str类型
        maxTimeQcTosTask = QcTosTaskQueryResults[-1]['RESPONSE_TIME']#这条船的当前岸桥的最大时间str类型
        print(f"#####{minTimeQcTosTask}")
        print(f"#####{maxTimeQcTosTask}")


        # xvalues=wholeHourTimes(minTimeQcTosTask,maxTimeQcTosTask)
        # print(xvalues)
