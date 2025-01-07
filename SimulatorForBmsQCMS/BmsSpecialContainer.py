import time
import func
while True:

    # ascIdname = '201'
    ascIdnames = ['201']
    # ascIdnames = ['201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215']
    for ascIdname in ascIdnames:
        commands = func.read_opcua(ip='opc.tcp://10.28.243.103:5600/',tag='ns=1;s=BMS_ACCS.{ascIdname}.OPC_PLC.Command'.format(ascIdname=ascIdname))
        trolleyCurPos = func.read_opcua(ip='opc.tcp://10.28.243.103:5600/', tag='ns=1;s=BMS_ACCS.{ascIdname}.PLC_OPC.TrolleyCurPos'.format(ascIdname=ascIdname))
        hoistCurPos = func.read_opcua(ip='opc.tcp://10.28.243.103:5600/', tag='ns=1;s=BMS_ACCS.{ascIdname}.PLC_OPC.HoistCurPos'.format(ascIdname=ascIdname))
        spreaderTwistStatus = func.read_opcua(ip='opc.tcp://10.28.243.103:5600/', tag='ns=1;s=BMS_ACCS.{ascIdname}.PLC_OPC.SpreaderTwistStatus'.format(ascIdname=ascIdname))
        print(f'PLC返回{ascIdname}的小车当前位置={trolleyCurPos}mm')
        print(f'PLC返回{ascIdname}的起升当前位置={hoistCurPos}mm')
        print(f'PLC返回{ascIdname}的吊具开闭锁状态={spreaderTwistStatus}，1闭锁，2开锁')
        #####################################################################################CommandTypeObject
        commandIndex = commands[1]

        # 将十进制数值转换为二进制表示（去掉 '0b' 前缀）
        commandIndex = bin(commandIndex)[2:]
        commandIndex = commandIndex.zfill(26)  # 补全26位数字

        # 后8位=CommandIndex，再8位=CommandType，再8位=CommandTypeObject,最后一位=CommandOperation
        commandIndex = commandIndex[-8:]
        commandIndex = int(commandIndex, 2)  # 转化为10进制
        print(f"BMS发送{ascIdname}的commandIndex={commandIndex}")

        #####################################################################################CommandType
        commandType = commands[1]
        # print(f"取出#CommandType对应的BMS_ACCS.XXX.OPC_PLC.Command第1位值是 {commandType}")
        # 将十进制数值转换为二进制表示（去掉 '0b' 前缀）
        if commandType!='':
            commandType = bin(commandType)[2:]
            commandType = commandType.zfill(26)  # 补全26位数字
            # print(f"取出commandType转化为二进制值是 {commandType}")
            # print(f"取出commandType转化为二进制位数是 {len(commandType)}")
            #后8位=CommandIndex，再8位=CommandType，再8位=CommandTypeObject,最后一位=CommandOperation
            commandType1=commandType[-16:-8]
            commandType=int(commandType1, 2)#转化为10进制
            if commandType==0:
                print(f"BMS发送{ascIdname}的指令类型：Null（Park:unlock,Calibration,Anchor）")
            elif commandType==1:
                print(f"BMS发送{ascIdname}的指令类型：Park")
            elif commandType==2:
                print(f"BMS发送{ascIdname}的指令类型：Pickup")
            elif commandType == 3:
                print(f"BMS发送{ascIdname}的指令类型：Ground")
            elif commandType == 4:
                print(f"BMS发送{ascIdname}的指令类型：Calibration")
            elif commandType == 5:
                print(f"BMS发送{ascIdname}的指令类型：Anchor")
            elif commandType == 6:
                print(f"BMS发送{ascIdname}的指令类型：LearnTrain（在指定扫描小车车道向目标大车位置方向精准搜寻火车停靠位置）")
            # 0:Null；
            # 1:Park；
            # 2:Pickup；
            # 3:Ground；
            # 4:Calibration；
            # 5:Anchor；
            # 6.LearnTrain（在指定扫描小车车道向目标大车位置方向精准搜寻火车停靠位置）；
            #####################################################################################CommandTypeObject
            commandTypeObject=commands[1]

            # 将十进制数值转换为二进制表示（去掉 '0b' 前缀）
            commandTypeObject = bin(commandTypeObject)[2:]
            commandTypeObject = commandTypeObject.zfill(26)#补全26位数字

            #后8位=CommandIndex，再8位=CommandType，再8位=CommandTypeObject,最后一位=CommandOperation
            commandTypeObject1=commandTypeObject[-24:-16]

            commandTypeObject=int(commandTypeObject1, 2)#转化为10进制
            if commandTypeObject==0:
                print(f"BMS发送{ascIdname}的指令类型对象：Null（Park:unlock,Calibration,Anchor）")
            elif commandTypeObject==1:
                print(f"BMS发送{ascIdname}的指令类型对象：StackBlock；(Pickup and Ground)")
            elif commandTypeObject==2:
                print(f"BMS发送{ascIdname}的指令类型对象：AGV；(Pickup and Ground)")
            elif commandTypeObject == 3:
                print(f"BMS发送{ascIdname}的指令类型对象：AGVMate；(Pickup and Ground)")
            elif commandTypeObject == 4:
                print(f"BMS发送{ascIdname}的指令类型对象：InternalTruck；(Pickup and Ground)")
            elif commandTypeObject == 5:
                print(f"BMS发送{ascIdname}的指令类型对象：ExternalTruck；(Pickup and Ground)")
            elif commandTypeObject == 6:
                print(f"BMS发送{ascIdname}的指令类型对象：TransferPoint；(Pickup and Ground)")
            elif commandTypeObject == 7:
                print(f"BMS发送{ascIdname}的指令类型对象：TransferRack；(Pickup and Ground)")
            elif commandTypeObject == 8:
                print(f"BMS发送{ascIdname}的指令类型对象：ReeferRack；(Pickup and Ground)")
            elif commandTypeObject == 9:
                print(f"BMS发送{ascIdname}的指令类型对象：CrossStreet；(Only Park)")
            elif commandTypeObject == 10:
                print(f"BMS发送{ascIdname}的指令类型对象：Anchor；(Only Anchor)")
            elif commandTypeObject == 11:
                print(f"BMS发送{ascIdname}的指令类型对象：Disanchor；(Only Anchor)")
            elif commandTypeObject == 13:
                print(f"BMS发送{ascIdname}的指令类型对象：AutoCalibrate(自动标定)(Only Calibration)")
            elif commandTypeObject == 15:
                print(f"BMS发送{ascIdname}的指令类型对象：OverShoot(Only Park)")
            elif commandTypeObject == 16:
                print(f"BMS发送{ascIdname}的指令类型对象：IGV(无人集卡)(Pickup and Ground）")
            elif commandTypeObject == 17:
                print(f"BMS发送{ascIdname}的指令类型对象：Train（火车）(Pickup and Ground vs LearnTrain）")
            # 0:Null；
            # 1:StackBlock；(Pickup and Ground)
            # 2:AGV；(Pickup and Ground)
            # 3:AGVMate；(Pickup and Ground)
            # 4:InternalTruck；(Pickup and Ground)
            # 5:ExternalTruck；(Pickup and Ground)
            # 6:TransferPoint；(Pickup and Ground)
            # 7:TransferRack；(Pickup and Ground)
            # 8:ReeferRack；(Pickup and Ground)
            # 9:CrossStreet；(Only Park)
            # 10:Anchor；(Only Anchor)
            # 11:Disanchor；(Only Anchor)
            # 13:AutoCalibrate(自动标定)(Only Calibration)
            # 15:OverShoot(Only Park)
            # 16.IGV(无人集卡)(Pickup and Ground）
            # 17.Train（火车）(Pickup and Ground vs LearnTrain）


        #########################################################ContainerType
        # print(f"BMS_ACCS.XXX.OPC_PLC.Command的原值是 {commands}")
        #ContainerType提取
        containerTypes=commands[16]
        # print(f"取出ContainerType对应的BMS_ACCS.XXX.OPC_PLC.Command第16位值是 {containerTypes}")
        # 将十进制数值转换为二进制表示（去掉 '0b' 前缀）
        if containerTypes!='':
            binary_representation = bin(containerTypes)[2:]
            #补全20位；后8位=ContainerType，再8位=SpreaderDirection，再4位=BlockDesIndex
            containerTypesob = binary_representation.zfill(20)#补全20位数字
            # print(f"取出ContainerType转化为二进制值是 {binary_representation}")
            #containerType解析
            containerType1=containerTypesob[-8:]
            containerType=int(containerType1, 2)#转化为10进制
            if containerType==0:
                print(f"BMS发送{ascIdname}的箱子类型：Null（Park:unlock,Calibration,Anchor）")
            elif containerType==1:
                print(f"BMS发送{ascIdname}的箱子类型：General")
            elif containerType==2:
                print(f"BMS发送{ascIdname}的箱子类型：Empty(Park:lock,pickup,ground;对有密堆要求的空箱)-半自动靠箱")
            elif containerType == 3:
                print(f"BMS发送{ascIdname}的箱子类型：Dangerous（Park:lock,pickup,ground;危险品箱）-半自动")
            elif containerType == 4:
                print(f"BMS发送{ascIdname}的箱子类型：Reefer（Park:lock,pickup,ground;冷藏箱）-监控自动")
            elif containerType == 5:
                print(f"BMS发送{ascIdname}的箱子类型：（Park:lock,pickup,ground;危险品箱）-半自动")
            elif containerType == 6:
                print(f"BMS发送{ascIdname}的箱子类型：Others(只能手动作业，无法自动抓放，如开顶箱，箱高度不确定)")
                # 0: Null
                # 1: 往集卡上放箱（堆场出箱：装船 / 提箱）
                # 2: 从集卡上抓箱（堆场进箱：卸船 / 收箱）
            # 0：Null（Park:unlock,Calibration,Anchor）;
            # 1：General（Park:lock,pickup,ground;重箱或者对堆放没有要求的作业箱）；
            # 2：Empty(Park:lock,pickup,ground;对有密堆要求的空箱)-半自动靠箱；
            # 3：Dangerous（Park:lock,pickup,ground;危险品箱）-半自动;
            # 4：Reefer（Park:lock,pickup,ground;冷藏箱）-监控自动
            # 5：Tank-同3
            # 6：Others(只能手动作业，无法自动抓放，如开顶箱，箱高度不确定)
        #########################################################TrolleyDesPos
        # print(f"BMS_ACCS.XXX.OPC_PLC.Command的原值是 {commands}")
        #TrolleyDesPos提取
        trolleyDesPos=commands[4]
        print(f'BMS下发{ascIdname}的小车目的位置={trolleyDesPos}mm')

        #########################################################HoistDesPos
        # print(f"BMS_ACCS.XXX.OPC_PLC.Command的原值是 {commands}")
        #HoistDesPos提取
        hoistDesPos=commands[5]
        # 将十进制数值转换为二进制表示（去掉 '0b' 前缀）
        if hoistDesPos != '':
            # 将十进制数值转换为二进制表示（去掉 '0b' 前缀）
            binary_representation = bin(hoistDesPos)[2:]
            # 后8位=
            hoistDesPos1 = binary_representation[-14:]
            hoistDesPos = int(hoistDesPos1, 2)  # 转化为10进制
            print(f'BMS下发{ascIdname}的起升目的位置={hoistDesPos}mm')
        #########################################################HoistDesPos


