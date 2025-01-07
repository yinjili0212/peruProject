import time
import func
while True:
    time.sleep(1)
    bmsCps = func.read_opcua(ip='opc.tcp://10.28.243.103:5600/',tag='ns=1;s=BMS_ACCS.202.OPC_PLC.HTSWorkInfo')
    # print(bmsCps)


    binary_str = bin(bmsCps)#十进制转二进制
    # 去掉前缀'0b'
    binary_str_no_prefix = binary_str[2:]
    bmsCpsob = binary_str_no_prefix.zfill(28)#补全28位数字

    htsWorkType=bmsCpsob[-8:]
    htsWorkType=int(htsWorkType, 2)#转化为10进制
    if htsWorkType==1:
        print("装卸任务类型=装船 / 提箱")
    elif htsWorkType==2:
        print("装卸任务类型=卸船 / 收箱")
    elif htsWorkType==0:
        print("装卸任务类型=未知")
        # 0: Null
        # 1: 往集卡上放箱（堆场出箱：装船 / 提箱）
        # 2: 从集卡上抓箱（堆场进箱：卸船 / 收箱）

    workContainerSize=bmsCpsob[-16:-8]
    workContainerSize=int(workContainerSize, 2)#转化为10进制
    if workContainerSize==0:
        print("作业集装箱尺寸=未知")
    elif workContainerSize==1:
        print("作业集装箱尺寸=20尺")
    elif workContainerSize == 2:
        print("作业集装箱尺寸=40尺")
    elif workContainerSize == 3:
        print("作业集装箱尺寸=45尺")
    elif workContainerSize == 4:
        print("作业集装箱尺寸=双20尺")
        # 0：Null
        # 1：20尺
        # 2：40尺
        # 3：45尺
        # 4：双20尺

    htsWorkLane=bmsCpsob[-24:-16]
    htsWorkLane=int(htsWorkLane, 2)#转化为10进制
    print("作业车道={htsWorkLane}".format(htsWorkLane=htsWorkLane))

    htsType=bmsCpsob[:-24]
    htsType=int(htsType, 2)#转化为10进制
    if htsType==0:
        print("水平运输设备(集卡)类型=未知")
    elif htsType==4:
        print("水平运输设备(集卡)类型=内集卡")
    elif htsType==5:
        print("水平运输设备(集卡)类型=外集卡")
    elif htsType == 16:
        print("水平运输设备(集卡)类型=IGV")
    # 0:Null；
    # 4:InternalTruck；(Pickup and Ground)
    # 5:ExternalTruck；(Pickup and Ground)
    # 16:IGV(无人集卡)(Pickup and Ground）


# func.write_opcua(ip='opc.tcp://10.28.243.103:5600/',tag='ns=1;s=BMS_ACCS.202.PLC_OPC.HTSStatus',value=0)
    # time.sleep(5)
# func.write_opcua(ip='opc.tcp://10.28.243.103:5600/',tag='ns=1;s=BMS_ACCS.202.PLC_OPC.HTSStatus',value=3)

