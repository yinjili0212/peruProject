# import sqliteHandle
# import pandas as pd
# o=sqliteHandle.sqliteHandler('kpiforQcms20250103.db')
#
# qcms_kpi_for_container_transfer='qcms_kpi_for_container_transfer'
#
# querysql = f"""select * from {qcms_kpi_for_container_transfer} order by PICKUP_TIME asc"""
# queryResults=o.query(querysql)
# for queryResult in queryResults:
#     deltatime = pd.to_datetime(queryResult['GROUND_TIME'])-pd.to_datetime(queryResult['PICKUP_TIME'])
#
#     if deltatime<pd.Timedelta(seconds=20):
#         print(deltatime)
#
#
# # a = [1, 1, 0, 1, 0]
# # for i in a:
# #     print(i)


# import sqliteHandle
# import pandas as pd
# o=sqliteHandle.sqliteHandler('kpiforQcms20250109.db')

import plotly.graph_objs as go

# 假设您有一些数据
x_data = [1, 2, 3, 4, 5]
y_data = [10, 15, 13, 17, 16]
texts = ['all 0', 'all 12', 'all 12', 'all 10', 'all 7']  # 注释文本列表

# 创建数据痕迹
trace = go.Scatter(x=x_data, y=y_data, mode='markers')

# 创建注释列表
annotations = []
for i, text in enumerate(texts):
    annotations.append(dict(text=text, x=x_data[i], y=y_data[i], xref='x', yref='y', showarrow=True,font=dict(size=24)))

# 创建图表布局并添加注释
layout = go.Layout(annotations=annotations)

# 创建图表对象并添加痕迹和布局
fig = go.Figure(data=[trace], layout=layout)

# 显示图表
fig.show()