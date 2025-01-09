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


import sqliteHandle
import pandas as pd
o=sqliteHandle.sqliteHandler('kpiforQcms20250109.db')