matching_rows_for_qcmseventstart = filtered_dfs[
    (filtered_dfs['DATA_FROM'] == 'QCMSDB.qc_event_recorder_his.CREATE_TIME')]
matching_rows_for_qcmseventstart = matching_rows_for_qcmseventstart.sort_values(by='ID', ascending=True)  # 对字段进行排序


for listforindex, qcmseventstartindex in enumerate(
        qcmseventStart_Qcmsindexs):  # 查询QCMSqcmsevent的值，listforindex对应列表的index，而qcmseventstartindex则是pandas数据对应的唯一值
    if qcmseventstartindex < (len(qcmseventStart_Qcmsindexs) - 1):  # 只计算到列表倒数第2条数据
        qcqcmseventstart = filtered_dfs.iloc[qcmseventstartindex]  # 开始画图的第一条数据
        # if qcqcmseventstart['EVENT_CODE'] not in (10001,10002,10003,10010,10011,10012,10021,10022,10031,10032):
        qcqcmseventend = filtered_dfs.iloc[
            qcmseventStart_Qcmsindexs[listforindex + 1]]  # 结束的条件应该是pandas数据的index是list列表index的下一个index