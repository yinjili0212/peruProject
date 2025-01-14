import sqliteHandle
import pandas as pd


#########
'预处理OPCUA中的数据，将MtWorkMode/MtInstructionStatus/MhAboveSafeHeight三张表的数据预先处理' \
'1.加了字段ID自增'
'2.加了NOTES字段用来解释Value值的枚举值'
'3.加了QC_ID字段用来标注是哪个岸桥的数据'
#########
o=sqliteHandle.sqliteHandler('kpiforQcms20250109.db')

MtWorkMode='MtWorkMode'
MtInstructionStatus='MtInstructionStatus'
MhAboveSafeHeight='MhAboveSafeHeight'
###############更新MtWorkMode表###############
querySqlForMtWorkMode = f'''select * from {MtWorkMode} order by SourceTime asc'''
mtWorkModeQueryResults = o.query(querySqlForMtWorkMode,t='df')
if isinstance(mtWorkModeQueryResults,pd.DataFrame):
    for index,mtWorkModeQueryResult in mtWorkModeQueryResults.iterrows():
        QC_ID=mtWorkModeQueryResult['TagName'][10:13]

        updateMtWorkModeSql=f'''update {MtWorkMode} set QC_ID='{QC_ID}' where ID={mtWorkModeQueryResult['ID']}'''
        o.executesql(updateMtWorkModeSql)
###############更新MtWorkMode表###############



###############MtInstructionStatus###############
querySqlForInstructionStatus = f'''select * from {MtInstructionStatus} order by SourceTime asc'''
mtInstructionStatusQueryResults = o.query(querySqlForInstructionStatus,t='df')
if isinstance(mtInstructionStatusQueryResults,pd.DataFrame):
    for index,mtInstructionStatusQueryResult in mtInstructionStatusQueryResults.iterrows():
        QC_ID=mtInstructionStatusQueryResult['TagName'][10:13]

        updateMtInstructionStatusSql=f'''update {MtInstructionStatus} set QC_ID='{QC_ID}' where ID={mtInstructionStatusQueryResult['ID']}'''
        o.executesql(updateMtInstructionStatusSql)
###############更新MtInstructionStatus表###############

###############更新MhAboveSafeHeight###############
querySqlForMhAboveSafeHeight = f'''select * from {MhAboveSafeHeight} order by SourceTime asc'''
mtAboveSafeHeightQueryResults = o.query(querySqlForMhAboveSafeHeight,t='df')
if isinstance(mtAboveSafeHeightQueryResults,pd.DataFrame):
    for index,mtAboveSafeHeightQueryResult in mtAboveSafeHeightQueryResults.iterrows():
        QC_ID=mtAboveSafeHeightQueryResult['TagName'][10:13]

        updateMhAboveSafeHeightSql=f'''update {MhAboveSafeHeight} set QC_ID='{QC_ID}' where ID={mtAboveSafeHeightQueryResult['ID']}'''
        o.executesql(updateMhAboveSafeHeightSql)
###############更新MhAboveSafeHeight表###############