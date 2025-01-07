import yaml
import sqliteHandle
import func
import os
import time
import oracleDBHandle


env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)


###杀掉进程
func.killWinCMD('10.28.243.103','Zpmc@243.103','''taskkill /IM ZECS.BMS.Services.exe /F''')
#
###清空BMS数据库信息
dsn = conf.get('bmsoracledbhost')+":1521/"+conf.get('bmsoracledbdatabase')#10.128.231.88:1521/PERU_PRODUCT_DB
o= oracleDBHandle.OracleDBHandler(user=conf.get('bmsoracledbuser'),password=conf.get("bmsoracledbpassword"),dsn=dsn)
# o.executesql("truncate table T_BMS_ASC_COMMAND")
o.executesql("delete from T_BMS_ASC_COMMAND where status not in ('COMPLETED','ABORTED','CANCELED')")
o.executesql("truncate table T_BMS_ASC_ORDER")
o.executesql('''UPDATE T_BMS_ASC_STATUS SET ORDER_ID=NULL,CMD_ID=NULL''')
o.executesql("update T_BMS_DEVICE_STATUS set ORDER_ID=0,COMMAND_ID=0")
# o.executesql('''TRUNCATE TABLE T_BMS_EVENT_RECORDS''')
# o.executesql('''TRUNCATE TABLE T_BMS_EXCEPTION_RECORDS''')
o.executesql('''delete from T_BMS_EXCEPTION_RECORDS where SOLVE_DATETIME is NULL''')
# o.executesql('''TRUNCATE TABLE T_BMS_EXECUTION_RECORDS''')
o.executesql('''delete from T_BMS_EXECUTION_RECORDS where ORDER_STATUS not in ('COMPLETED','ABORT','CANCEL')''')
# o.executesql('''TRUNCATE TABLE T_BMS_MICROCOMMAND''')
o.executesql('''delete from T_BMS_MICROCOMMAND where INSTRUCTION_STATUS not in ('Finish','Closed','CANCEL')''')
# o.executesql('''TRUNCATE TABLE T_BMS_ORDER_RECORDS''')
o.executesql('''delete from T_BMS_ORDER_RECORDS where ORDER_STATUS not in ('COMPLETED','ABORT','CANCEL')''')



##清空SQlite本地数据
odb_o=sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
odb_o.executesql("delete from tosbmstask")
odb_o.executesql("update ocrbms set whethersendctnno='0',whethersendctndoor='0',whethersendtruck='0'")
odb_o.executesql("update fmsbms set bay=null,ascId=null,ascCtnId=null,ascCtnType=null,posOnAht=null,ascStatus=null,ahtIdFromEcs=null,ahtId=null,ahtCtnId=null,ahtStatus=null,ahtTaskType=null,init_fail_sendcount=0,locked_fail_sendcount=0,cancel_fail_sendcount=0,leavelane_fail_sendcount=0")

#打印清除环境时间
print(func.get_current_time())
