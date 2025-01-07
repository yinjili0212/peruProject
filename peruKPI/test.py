# import plotly.graph_objects as go
#
#
#
#
# xvalues=['2025-01-02 10:00:00','2025-01-02 11:00:00','2025-01-02 12:00:00','2025-01-02 13:00:00','2025-01-02 14:00:00']
# yvalues=[3,4,13,0,4]
# tests=[f'岸桥毛效率 {yvalues[0]}move/h',f'岸桥毛效率 {yvalues[1]}move/h',f'岸桥毛效率 {yvalues[2]}move/h',f'岸桥毛效率 {yvalues[3]}move/h',f'岸桥毛效率 {yvalues[4]}move/h']
# texts = [f'HAha {i}' for i in yvalues]
# # print(tests)
# fig= go.Figure()
# fig.add_trace(go.Bar(x=xvalues,y=yvalues,width=0.1))
# fig.add_trace(go.Scatter(x=xvalues,y=yvalues,mode='lines+markers',text=texts,line=dict(color='blue',width=2,dash='solid')))
#
# # 设置文本标签的位置，这里设置为 'top center' 表示在点的上方居中显示
# fig.update_traces(textposition='outside')
#
# # fig = go.Figure(data=[bar1])
# # 更新布局，设置 x 轴为类别类型
# fig.update_layout(
#     title='岸桥毛效率条形图',
#     xaxis_title='时间',
#     yaxis_title='效率 (move/h)',
#     xaxis_type='category'  # 确保 x 轴被解释为类别标签
# )
# fig.show()

# import sqliteHandle
# o=sqliteHandle.sqliteHandler('kpiforQcms20250103.db')
#
# querysqlForContainerTransferIds=f"""select * from qc_container_transfer where CREATE_TIME>='2025-01-02 09:59:16' and QC_ID=103"""
# containerTransferQueryResults = o.query(querysqlForContainerTransferIds)
# for containerTransferQueryResult in containerTransferQueryResults:
#     querysqlforQcTrolleyTask =f"""select * from qc_trolley_task where TRANS_CHAIN_ID='{containerTransferQueryResult['TRANS_CHAIN_ID']}'"""
#     qcTrolleyTaskQueryResults=o.query(querysqlforQcTrolleyTask)
#     if len(qcTrolleyTaskQueryResults)==0:
#         print(f"没有查询到数据{containerTransferQueryResult['TRANS_CHAIN_ID']}")


import plotly.graph_objects as go

xvalues = ['2025-01-02 10:00:00', '2025-01-02 11:00:00', '2025-01-02 12:00:00', '2025-01-02 13:00:00', '2025-01-02 14:00:00']
yvalues = [3, 4, 13, 0, 4]
texts = [f'HAha {i}' for i in yvalues]  # 为每个点创建文本标签

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=xvalues,
    y=yvalues,
    mode='lines',  # 绘制散点并连接成线
    name='毛效率趋势图',
    text=texts,  # 设置每个点的文本标签
    line=dict(color='black', width=2, dash='solid'),
    marker=dict(size=10)  # 可选：设置散点的大小
))
fig.add_trace(go.Bar(x=xvalues,y=yvalues,marker=dict(color='blue', line=dict(width=0, color='black')),width=0.3,name='毛效率条形图'))

# # 设置文本标签的位置，这里设置为 'top center' 表示在点的上方居中显示
# fig.update_traces(textposition='top center')
# 添加注释（此注释不会随数据点移动）
fig.add_annotation(
    x=xvalues[0],  # 注释的 x 位置（这里以第一个数据点的 x 坐标为例）
    y=yvalues[0]+0.5,  # 注释的 y 位置（这里以第一个数据点的 y 坐标为例）
    text='这是一个注释',  # 注释的文本内容
    showarrow=False # 是否显示箭头
)

# 更新布局
fig.update_layout(
    title='散点图示例',
    xaxis_title='时间',
    yaxis_title='值',
    xaxis_type='category'  # 确保 x 轴被解释为类别标签
)

fig.show()
