import mysqldbHandle

import yaml
import sqliteHandle
import func
import os
import oracleDBHandle

env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)

###杀掉进程
func.killWinCMD('10.28.243.102','Zpmc@243.102','''taskkill /IM ZECS.QCMS.Services.exe /F''')

#
###清空QCMS数据库信息
dsn = conf.get('qcmsoracledbhost')+":1521/"+conf.get('qcmsoracledbdatabase')#10.128.231.88:1521/PERU_PRODUCT_DB
o= oracleDBHandle.OracleDBHandler(user=conf.get('qcmsoracledbuser'),password=conf.get("qcmsoracledbpassword"),dsn=dsn)
o.executesql("TRUNCATE TABLE QC_EXCEPTION_RECORDER")
o.executesql("TRUNCATE TABLE QC_FMS_INTERACTION_RECORD")
o.executesql('''DELETE FROM QC_COMMAND qc WHERE STATUS NOT IN ('COMPLETE','ABORTED')''')
o.executesql("TRUNCATE TABLE QC_COMMAND_HIS")
o.executesql('''TRUNCATE TABLE QC_EVENT_RECORDER_HIS''')
o.executesql('''DELETE FROM QC_TOS_TASK qc WHERE TASK_STATUS NOT IN ('COMPLETE','ABORTED')''')
o.executesql('''TRUNCATE TABLE QC_TP_INTERACTION_HIS''')
o.executesql('''TRUNCATE TABLE QC_VESSEL_VISIT_STRUCTURE''')

##清空SQlite本地数据
odb_o=sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
odb_o.executesql("delete from tosqcmstask")
# odb_o.executesql("delete from fmsqcms_his")

executesql_insqlite = '''update fmsqcms set qcRef1='',qcRef2='',qcStatus="",qcV=0,ahtIdFromEcs="",jobPos='',ahtId='',moveKind='',\
ahtStatus='',ahtRespV=0,init_fail_sendcount=0,arrived_fail_sendcount=0,locked_fail_sendcount=0,cancel_fail_sendcount=0,\
leavelane_fail_sendcount=0'''
odb_o.executesql(executesql_insqlite)

#打印清除环境时间
print(func.get_current_time())
