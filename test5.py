# import pandas as pd
# from pandas import Timestamp
# import imp
#
# # 创建一个包含日期时间的Series
# dates = pd.Series(['2023-01-01', '2023-02-01', '2023-03-01'])
# dates = pd.to_datetime(dates)  # 确保Series中的元素是Timestamp对象
#
# # 创建一个单独的Timestamp对象
# single_date = Timestamp('2023-01-31')
#
# # 进行比较
# comparison = dates >= single_date
#
# print(comparison)

from basicFunctionDefine import *
##实际时间
startTime='2025-01-08 13:30:30'
endTime='2025-01-09 08:10:44'


# startTime=strChangeTime(startTime)
# startTimeforKpi='2025-01-09 07:00:14'
# endTimeforKpi='2025-01-09 08:00:14'

# timea=timedelta(hours=5)
startTimeforKpi=timeChangeStr(strChangeTime(startTime)+timedelta(hours=5))
endTimeforKpi=timeChangeStr(strChangeTime(endTime)+timedelta(hours=5))
print(startTimeforKpi,endTimeforKpi)

