import time
import func
while True:
    time.sleep(1)
    qcmMtinstru = func.read_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.OPC_PLC.MtInstr')
    if len(qcmMtinstru)!=0:#能查询出来数据list
        # print(qcmMtinstru)
        # qcmMtinstru[3]
        if qcmMtinstru[1]==0:
            print("主小车指令类型={HTType}".format(HTType="未知"))
        elif qcmMtinstru[1]==1:
            print("主小车指令类型={HTType}".format(HTType="停车指令"))
        elif qcmMtinstru[1]==2:
            print("主小车指令类型={HTType}".format(HTType="抓箱指令"))
        elif qcmMtinstru[1]==3:
            print("主小车指令类型={HTType}".format(HTType="放箱指令"))



        if qcmMtinstru[3]==0:
            print("主小车指令操作方式={HTType}".format(HTType="未知"))
        elif qcmMtinstru[3]==1:
            print("主小车指令操作方式={HTType}".format(HTType="全自动"))
        elif qcmMtinstru[3]==2:
            print("主小车指令操作方式={HTType}".format(HTType="半自动"))
        elif qcmMtinstru[3]==3:
            print("主小车指令操作方式={HTType}".format(HTType="手动"))


    # 主小车指令类型:
    # 1:Park，停车指令
    # 2:Pickup，抓箱指令
    # 3:Setdown，放箱指令
    # 主小车指令操作方式
    # 1:Auto，全自动
    # 2:SemiAuto，半自动
    # 3:Manual，手动