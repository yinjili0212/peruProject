# from oracleDBHandle import OracleDBHandler
# o = OracleDBHandler(host='10.128.254.200',port='1521',service_name='nuct_product_db',user='nuct_ecs',password='ecs123')
# results= o.query("select * from testqctp")
# print(results)
# o.close()




VMS = ['AHT前往QCTP过程中，交互表给ARRVING','AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=LOCK_REQ','AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=空','AHT跟随过程中']
QCMS = ['岸桥第1次移动中','岸桥第1次换贝结束','岸桥第2次移动中','岸桥第2次到位','岸桥第3次移动中','岸桥第3次到位']
for i_vms in VMS:
    for j_qcms in QCMS:
        print('VMS状态：'+i_vms+';'+'QCMS状态：'+j_qcms)

# VMS状态：AHT前往QCTP过程中，交互表给ARRVING;QCMS状态：岸桥第1次移动中（啥也不干）
# VMS状态：AHT前往QCTP过程中，交互表给ARRVING;QCMS状态：岸桥第1次换贝结束（OK）
# VMS状态：AHT前往QCTP过程中，交互表给ARRVING;QCMS状态：岸桥第2次移动中（OK）
# VMS状态：AHT前往QCTP过程中，交互表给ARRVING;QCMS状态：岸桥第2次到位（OK）
# VMS状态：AHT前往QCTP过程中，交互表给ARRVING;QCMS状态：岸桥第3次移动中（OK）
# VMS状态：AHT前往QCTP过程中，交互表给ARRVING;QCMS状态：岸桥第3次到位（OK）
# VMS状态：AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=LOCK_REQ;QCMS状态：岸桥第1次移动中(不允许存在)
# VMS状态：AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=LOCK_REQ;QCMS状态：岸桥第1次换贝结束（OK）
# VMS状态：AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=LOCK_REQ;QCMS状态：岸桥第2次移动中(不允许存在)
# VMS状态：AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=LOCK_REQ;QCMS状态：岸桥第2次到位（重复）
# VMS状态：AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=LOCK_REQ;QCMS状态：岸桥第3次移动中(不允许存在)
# VMS状态：AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=LOCK_REQ;QCMS状态：岸桥第3次到位（重复）
# VMS状态：AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=空;QCMS状态：岸桥第1次移动中（OK）
# VMS状态：AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=空;QCMS状态：岸桥第1次换贝结束（OK）
# VMS状态：AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=空;QCMS状态：岸桥第2次移动中（OK）
# VMS状态：AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=空;QCMS状态：岸桥第2次到位（OK）
# VMS状态：AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=空;QCMS状态：岸桥第3次移动中（重复）
# VMS状态：AHT到达QCTP，交互表AHT_STATUS=ARRIVED,QC_STATUS=空;QCMS状态：岸桥第3次到位（重复）
# VMS状态：AHT跟随过程中;QCMS状态：岸桥第1次换贝结束（不存在）
# VMS状态：AHT跟随过程中;QCMS状态：岸桥第2次移动中（重复）
# VMS状态：AHT跟随过程中;QCMS状态：岸桥第2次到位（重复）
# VMS状态：AHT跟随过程中;QCMS状态：岸桥第3次移动中
# VMS状态：AHT跟随过程中;QCMS状态：岸桥第3次到位

