import xlrd
import xlwt
from xlutils.copy import copy





# ####################################################################################################################################读取xls数据函数
url = r'./导出.xls'
#打开指定的工作薄
workbook = xlrd.open_workbook(url)
###############获取工作表格的三种方法
# ## 获取工作表格的3种方法，第1种方法
# sheet1 = workbook.sheets()[0]  #workbook.sheets()[索引]：索引从 0 开始，对应表格1，表格2，表格3
# print(sheet1)
# ## 获取工作表格的3种方法，第2种方法
# sheet2 = workbook.sheet_by_index(0)  #workbook.sheet_by_index(索引)：索引从 0 开始，对应sheet1、sheet2、sheet3
###############

## 获取工作表格的3种方法，第3种方法
sheet1 = workbook.sheet_by_name("导出工作表")   #workbook.sheet_by_name(sheet名称)：此前提是知道表格中的sheet名称
print(sheet1)
# # 获取工作表格的所有sheet名称
# sheet_names = workbook.sheet_names()    #打印所有sheets页的名称，返回list
# print(sheet_names)
# ###############获取某个sheet页工作表格的行数nrows和列数cols
# for i in range(len(sheet_names)):
#     sheet = workbook.sheets()[i]  # 获取表格 sheet 对象
#     rows = sheet.nrows       # 获取工作表格的行数
#     cols = sheet.ncols       # 获取工作表格的列数
#     print('表格 "{}" 总共有 {} 行，{} 列'.format(sheet_names[i],rows,cols)) # 打印输出
# ###############读取某个sheet页行数据的函数
# data = sheet1.row(1) # 获取第 1 行的数据,output=[text:'第一行第一列', text:'第一行第二列']
# print(data)   #sheet.row(n)：获取第 n+1 行的数据，其中 n 为行数，从 0 开始，返回该行所有单元格数据组成的列表
# print('*'*20)
#
# data = sheet1.row_slice(1) # 获取第 2 行的数据
# print(data)  #sheet.row_slice(n)：获取第 n+1 行的数据，其中 n 为行数，从 0 开始，返回该行所有单元格数据组成的列表
# print('*'*20)
#
# data = sheet1.row_types(rowx = 6,start_colx = 0,end_colx = 5) # 获取第 7 行，第 1-5 列的数据是否填充，填充显示1，无填充显示0
# print(data)  #sheet.row_types(rowx = n, start_colx = x, end_colx = y)：获取第 n 行的数据类型，其中 start_colx 和 end_colx（若为None则获取到结束） 为列的范围
# print('*'*20)
#
# data = sheet1.row_values(rowx = 1,start_colx = 0,end_colx = 3) # 获取第 2 行，第 1-3 列的数据
# print(data)  #sheet.row_values(rowx = n, start_colx = x, end_colx = y)：获取第 n 行的数据，其中 start_colx 和 end_colx（若为None则获取到结束） 为列的范围
# print('*'*20)
#
# num = sheet1.row_len(0)# 获取第 1 行的有效长度
# print(num)  #sheet.row_len(n)：获取第 n 行的有效长度。注：列没有此函数
# ###############读取某个sheet页列数据的函数
# data = sheet1.col(0) # 获取第 1 列的数据
# print("第一列的数据为：",data)#sheet.cols(n)：获取第 n 列的数据，其中 n 为列数，从 0 开始，返回该列所有单元格数据组成的列表
# print('*'*20)
#
# data = sheet1.col_slice(1) # 获取第 2 列的数据
# print("第二列的数据为：",data)#sheet.col_slice(n)：获取第 n 列的数据，其中 n 为列数，从 0 开始，返回该列所有单元格数据组成的列表
# print('*'*20)
#
# data = sheet1.col_types(colx = 1,start_rowx = 0,end_rowx = 10) # 获取第 2 列，第 1-10 行的数据
# print("第一列，第1-5行的数据为：",data)#sheet.col_types(colx = n, start_rowx = x, end_rowx = y)：获取第 n 列的数据类型，其中 start_rowx 和 end_rowx（若为None则获取到结束） 为行的范围
# print('*'*20)
#
# data = sheet1.col_values(colx=1,start_rowx=0,end_rowx=3) # 获取第 2 列，第 1-3 行的数据
# print("第二列，第1-3行的数据为：",data)#sheet.col_values(rowx = n, start_rowx = x, end_rowx = y)：获取第 n 列的数据，其中 start_rowx 和 end_rowx（若为None则获取到结束） 为行的范围

# ###############读取某个sheet页某个单元格数据的函数
# data = sheet1.cell(rowx = 0, colx = 1)  # 获取第 3 行，第 3 列对应的单元格数据
# print("第 3 行，第 3 列对应的单元格数据为：",data)#sheet.cell(rowx = n, colx = m)：获取第 n+1 行，第 m+1 列对应的单元格数据（返回的是单元格类型数据，要想获取数据本身，可以使用sheet.cell(n, m).value）
# print("第 3 行，第 3 列对应的单元格数据为：",data.value)
# print('*'*20)
#
# # data = sheet1.cell_type(2,2)  # 获取第 3 行，第 3 列对应的单元格是否填充，填充内容=1，无填充=0
# # print("第 3 行，第 3 列对应的单元格数据类型为：",data)#sheet.cell_type(rowx = n, colx = m)：获取第 n+1 行，第 m+1 列对应的单元格数据类型
# # print('*'*20)
#
# data = sheet1.cell_value(rowx=2,colx=1)  # 获取第 3 行，第 2 列对应的单元格数据
# print("第 3 行，第 2 列对应的单元格数据为：",data)#sheet.cell_value(rowx = n, colx = m)：获取第 n+1 行，第 m+1 列对应的单元格数据。

#获取第一行数据
##获取第一行数据
# for i in range(sheet1.row_len(0)):
#     row1=sheet1.cell_value(rowx=0, colx=i)#第1行数据
#     row2=sheet1.cell_value(rowx=1, colx=i)#第2行数据
#     print(sheet1.cell_value(rowx=0,colx=i)+'='+str(row2))
#     # print(i)
# #比较数据
for i in range(sheet1.row_len(0)):
    row2=sheet1.cell_value(rowx=1, colx=i)#第2行数据
    row3=sheet1.cell_value(rowx=2, colx=i)#第3行数据
    if row3!=row2:
        print(sheet1.cell_value(rowx=0,colx=i)+'='+str(row3))












