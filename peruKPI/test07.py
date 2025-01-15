import sqlite3

# 连接到SQLite数据库
conn = sqlite3.connect('kpiforQcms20250109.db')
cursor = conn.cursor()

# 查询历史表的数据
cursor.execute('SELECT QC_ID,LANE_ID,FMS_JOB_POS,FMS_AHT_ID,FMS_MOVE_KIND,FMS_AHT_STATUS,QC_REF1,QC_REF2,QC_STATUS,TRIG_CREATED FROM qc_tp_interaction_his where QC_ID=103 and LANE_ID=1 ORDER BY TRIG_CREATED asc')  # 替换为你的表和排序列
rows = cursor.fetchall()
column_names = [description[0] for description in cursor.description]  # 获取列名

# 遍历行并比较相邻行的字段值
for i in range(len(rows) - 1):
    current_row = rows[i]
    next_row = rows[i + 1]
    print(current_row)
    print(type(current_row))

    # changes = {}
    # for col_name, current_val, next_val in zip(column_names, current_row, next_row):
    #     if current_val != next_val:
    #         # changes[col_name] = (current_val, next_val)
    #         changes[col_name] = (next_val)
    #         # print(changes)
    # # print(changes)
    # # # 打印变化
    # # if changes:
    # #     print(f"Row {i + 1} to Row {i + 2} changes:")
    # #     for col, (old_val, new_val) in changes.items():
    # #         print(f"  {col}: {old_val} -> {new_val}")
    #
    # # 打印变化
    # if changes:
    #     print(f"Row {i + 1} to Row {i + 2} changes:")
    #     for col, ( new_val) in changes.items():
    #         print(f"  {col}: {new_val}")

# 关闭数据库连接
conn.close()