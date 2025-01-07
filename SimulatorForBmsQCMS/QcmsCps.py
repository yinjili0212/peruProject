import time
import func
while True:
    time.sleep(1)
    qcmsCps = func.read_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.OPC_PLC.HtGuiding1')
    if len(qcmsCps)!=0:#能查询出来数据list
        print(qcmsCps)

        if qcmsCps[0]==0:
            print("水平运输（集卡）类型={HTType}".format(HTType="未知"))
        elif qcmsCps[0]==1:
            print("水平运输（集卡）类型={HTType}".format(HTType="AGV/AHT车道"))
        elif qcmsCps[0]==2:
            print("水平运输（集卡）类型={HTType}".format(HTType="内集卡车道"))
        elif qcmsCps[0]==3:
            print("水平运输（集卡）类型={HTType}".format(HTType="外集卡车道"))

        if qcmsCps[1]==1:
            print("水平运输（集卡）驶入方向={HTDirection}".format(HTDirection="左进"))
        elif qcmsCps[1]==2:
            print("水平运输（集卡）驶入方向={HTDirection}".format(HTDirection="右进"))

        if qcmsCps[4]==0:
            print("水平运输引导位置={HTGuideTarget}".format(HTGuideTarget="null"))
        elif qcmsCps[4]==1:
            print("水平运输引导位置={HTGuideTarget}".format(HTGuideTarget="Not used"))
        elif qcmsCps[4]==2:
            print("水平运输引导位置={HTGuideTarget}".format(HTGuideTarget="Center20"))
        elif qcmsCps[4]==3:
            print("水平运输引导位置={HTGuideTarget}".format(HTGuideTarget="40尺"))
        elif qcmsCps[4]==4:
            print("水平运输引导位置={HTGuideTarget}".format(HTGuideTarget="45尺"))
        elif qcmsCps[4]==5:
            print("水平运输引导位置={HTGuideTarget}".format(HTGuideTarget="Twin20"))
        elif qcmsCps[4]==8:
            print("水平运输引导位置={HTGuideTarget}".format(HTGuideTarget="Left20/前20"))
        elif qcmsCps[4]==9:
            print("水平运输引导位置={HTGuideTarget}".format(HTGuideTarget="9:Right20/后20"))


        if qcmsCps[5]==1:
            print("水平运输引导类型={HTGuideType}".format(HTGuideType="装船"))
        elif qcmsCps[5]==2:
            print("水平运输引导类型={HTGuideType}".format(HTGuideType="卸船"))

        print("水平运输引导车道={HTGuideLane}".format(HTGuideLane=qcmsCps[6]))

# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.104.PLC_OPC_UI.CpsStatus1',value=3)
    # time.sleep(5)
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.104.PLC_OPC_UI.CpsStatus1',value=0)

