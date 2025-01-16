import pandas as pd
from pandas import Timestamp
import imp

# 创建一个包含日期时间的Series
dates = pd.Series(['2023-01-01', '2023-02-01', '2023-03-01'])
dates = pd.to_datetime(dates)  # 确保Series中的元素是Timestamp对象

# 创建一个单独的Timestamp对象
single_date = Timestamp('2023-01-31')

# 进行比较
comparison = dates >= single_date

print(comparison)