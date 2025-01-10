import sqlite3
import pandas as pd

# 注：设置环境编码方式，可解决读取数据库乱码问题
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)
#设置value的显示长度为100，默认为50
pd.set_option('max_colwidth',100)
pd.set_option('display.width', 5000)

class sqliteHandler:
    def __init__(self,databasepath):#databasepath=sqlite数据库地址路径
        self.conn = sqlite3.connect(databasepath)
        self.cur = self.conn.cursor()
        # 可选：启用 WAL 模式
        self.cur.execute("PRAGMA journal_mode=WAL;")


    def query(self, sql, t='d', source=False):
        self.cur.execute(sql)
        result = self.cur.fetchall()
        col = [col[0] for col in self.cur.description]
        if len(result) == 0:
            return []
        else:
            if t == 'o':
                return result
            elif t == 'df':
                results = pd.DataFrame(result)
                results.columns = col
                if source:
                    return results, result
                else:
                    return results
            elif t == 'd':
                rd = []
                for r in result:
                    rd.append({col[n]: r[n] for n in range(len(r))})
                return rd

    def getData(self, table, fields, conditions=None):
        if conditions:
            sql = "select " + fields + " from " + table + " where " + conditions + ""
        else:
            sql = "select " + fields + " from " + table
        self.cur.execute(sql)
        result = self.cur.fetchall()
        col = [col[0] for col in self.cur.description]
        rd = []
        for r in result:
            rd.append({col[n]: r[n] for n in range(len(r))})
        return rd

    def delData(self, table, conditions=None):
        if conditions:
            sql = "delete from " + table + " where " + conditions + ""
        else:
            sql = "delete from " + table
        self.cur.execute(sql)
        self.conn.commit()

    def createKpiForQcmsTable(self, tableName):#kpi_for_qcms
        try:
            # 注意：这个查询在不同的 SQLite 版本和/或不同的 SQL 模式（如标准 SQL 模式与 SQLite 的特定扩展）中可能有所不同。
            # 下面的查询是为了获取创建表的 SQL 语句，但这通常不是标准 SQL 的一部分。SQLite 提供了 .schema 命令，但它不是通过 SQL 查询访问的。
            # 因此，这里我们使用一个变通方法：创建一个临时表，然后使用 .schema 命令获取其创建语句，然后解析或修改它以用于新表。
            # 但是，这种方法比较复杂且容易出错。更简单的方法是预先知道 kpi_for_qcms 表的结构，并手动编写创建 test 表的 SQL 语句。

            # 由于直接获取创建语句的方法不可行，我们将采用手动编写 SQL 语句的方法。
            # 假设你知道 kpi_for_qcms 表的结构，这里是一个示例创建语句（你需要根据实际情况修改它）：
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {tableName} (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                STS_NO TEXT,
                TASK_ID INTEGER,
                VBT_ID INTEGER,
                TASK_TYPE TEXT,
                TASK_STATUS TEXT,
                ORIG_WSLOC TEXT,
                DEST_WS_LOC TEXT,
                KEYTIME TEXT,
                DATA_FROM TEXT,
                DATA_FROM_TYPE TEXT,
                NOTES TEXT)
            """

            # 执行创建表的 SQL 语句（但实际上由于使用了 IF NOT EXISTS，如果表已存在则不会创建）
            self.cur.execute(create_table_sql)

            # 提交事务
            self.conn.commit()

            print(f"Table {tableName} created (or already existed).")

        except sqlite3.OperationalError as e:
            # 处理任何操作错误，比如表不存在于数据库中时尝试查询其结构
            if "no such table" in str(e):
                # 在这里，我们实际上已经处理了表不存在的情况，因为我们使用了 IF NOT EXISTS。
                # 但是，如果是因为其他原因导致的错误（比如语法错误），我们需要在这里处理它。
                print(f"Error occurred while checking or creating table: {e}")
            else:
                # 其他类型的操作错误
                print(f"Unexpected error: {e}")


    def executesql(self, sql):
        self.cur.execute(sql)
        self.conn.commit()


    def close(self):
        self.cur.close()
        self.conn.close()
