import time

import  logservice
import func
from oracleDBHandle import OracleDBHandler
log = logservice.logfunc('./log/QC_TOS_TASK.txt')
#建立数据库连接
o = OracleDBHandler(user='PERU_QCMS', password='Zpmc#3261', dsn='10.128.231.88:1521/PERU_PRODUCT_DB')

id = 0
while True:
    time.sleep(1)
    # # 测试query函数
    # results = o.query("select * from QC_TOS_TASK where TASK_STATUS not in ('COMPLETED','ABORTED') order by OPERATE_TIME asc")
    # if len(results)!=0:
    #     id= results[0]['TASK_ID']
    # else:
    #     id = 0

    # 测试query函数
    results = o.query("select * from QC_TOS_TASK where TASK_ID={TASK_ID}".format(TASK_ID=id))
    for result in results:
        # func.append_dict_to_csv('./log/QC_TOS_TASK.csv',result)
        log.info("QC_TOS_TASK")
        log.info(result)

    results = o.query("select * from QC_TROLLEY_TASK where JOB_ID={JOB_ID}".format(JOB_ID=id))#找到对应TASK_ID
    if len(results)!=0:
        for result in results:
            log.info("QC_TROLLEY_TASK")
            log.info(result)

            querysql ="""select * from QC_TROLLEY_INSTRUCTION where TASK_ID='{TASK_ID}'""".format(TASK_ID=result['TASK_ID'])
            # print(querysql)
            instructionresults = o.query(querysql)

            for instructionresult in results:
                log.info("QC_TROLLEY_INSTRUCTION")
                log.info(instructionresult)
    # #
    results = o.query("select * from QC_COMMAND where TOS_TASK_ID={TOS_TASK_ID}".format(TOS_TASK_ID=id))
    for result in results:
        log.info("QC_COMMAND")
        log.info(result)

    results = o.query("SELECT * FROM QC_EXCEPTION_RECORDER where END_TIME IS NULL ORDER BY CREATE_TIME desc")
    for result in results:
        log.info("QC_EXCEPTION_RECORDER")
        log.info(result)
    print("\n")


# #关闭数据库和游标连接
# o.close()











