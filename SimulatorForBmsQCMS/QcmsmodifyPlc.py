import time
import func

qcmsCps = func.read_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.OcrRef1')
print(qcmsCps)
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.OcrRef1',value=[0, 0, 0, 0, 0, 0, 0, 0])#[14, 92, 6, 129, 3, 228, 129, 121]


# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MtInstrType',value=3)
# # 主小车指令类型:
# # 1:Park，停车指令
# # 2:Pickup，抓箱指令
# # 3:Setdown，放箱指令
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MtInstrTypeObject',value=1)
# # 主小车指令类型对象(仅抓放箱指令时使用):
# # 0:Null
# # 1:Vessel，船
# # 2:Platform，平台
# # 3:Agv/IGV，AGV或IGV
# # 4:Truck，集卡
# # 5:Apron，地面
# # 6:HatchCover，舱盖板
func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MtInstrStatus',value=8)
# # 主小车指令执行状态
# # 1:Working，指令执行中
# # 2:Finish，指令执行成功
# # 3:Pause，指令执行暂停
# # 4:Fail，指令执行失败
# # 5:Receive，收到指令
# # 7:WaitOcrConfirm，等待OCR给出继续执行的信号
# # 8:ExceptionComplete，异常完成
#
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MtWsSprdTwist',value=2)
# # 主小车海侧吊具旋锁状态
# # 1:Lock
# # 2:Unlock
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MtWsSprdLanded',value=2)
# # Primary Waterside Spreader Landing Status
# # 1:Landed
# # 2:UnLanded
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MhAboveSafeHeight',value=1)
# # 0: Null
# # 1:主起升在AGV安全高度以上或不在AGV区域
# # 2:主起升在AGV安全高度以下
#
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MtHoistPos',value=630)
# # 630: 1车道AGV下
# # 1:主起升在AGV安全高度以上或不在AGV区域
# # 2:主起升在AGV安全高度以下
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MtTrolleyPos',value=10000)
# # 4000: 1车道AGV下
#10000船
#
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MtWsSpdPosId',value=0)
# # 主小车海侧吊具当前所处位置号
# # 二进制字符串
# binary_str = "10100"
#
# # 使用int()函数转换
# decimal_num = int(binary_str, 2)
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MtPickupOperationCode',value=decimal_num)
# # 二进制字符串
# binary_str = "1010"
#
# # 使用int()函数转换
# decimal_num = int(binary_str, 2)

# # 主小车最后一次抓箱操作代码
# # BIT2、BIT1、BIT0:海侧吊具箱类型
# # 0     0     0   :未使用海侧吊具
# # 1     0     0   :单20尺
# # 1     0     1   :单40尺
# # 1     1     0   :单45尺
# # 1     1     1   :双20尺
# # BIT5、BIT4、BIT3:陆侧吊具箱类型
# # 0     0     0   :未使用陆侧吊具（双吊具模式）
# # 0     0     1   :未使用陆侧吊具（单吊具模式）
# # 1     0     0   :单20尺
# # 1     0     1   :单40尺
# # 1     1     0   :单45尺
# # 1     1     1   :双20尺
# # BIT6 : 抓、放状态
# # 0    : 抓箱
# # 1    : 放箱
# # BIT7 : 运行模式
# # 0    : 指令状态下
# # 1    : 非指令状态下
#

# 二进制字符串
binary_str = "11001100"

# 使用int()函数转换
decimal_num = int(binary_str, 2)
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MtSetdownOperationCode',value=decimal_num)
# # 主小车最后一次放箱操作代码
# # BIT2、BIT1、BIT0:海侧吊具箱类型
# # 0     0     0   :未使用海侧吊具
# # 1     0     0   :单20尺
# # 1     0     1   :单40尺
# # 1     1     0   :单45尺
# # 1     1     1   :双20尺
# # BIT5、BIT4、BIT3:陆侧吊具箱类型
# # 0     0     0   :未使用陆侧吊具（双吊具模式）
# # 0     0     1   :未使用陆侧吊具（单吊具模式）
# # 1     0     0   :单20尺
# # 1     0     1   :单40尺
# # 1     1     0   :单45尺
# # 1     1     1   :双20尺
# # BIT6 : 抓、放状态
# # 0    : 抓箱
# # 1    : 放箱
# # BIT7 : 运行模式
# # 0    : 指令状态下
# # 1    : 非指令状态下
#
#
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MtPickupWsSpdPosId',value=0)
# #主小车最后一次抓箱海侧吊具对应操作位置代号
# func.write_opcua(ip='opc.tcp://10.28.243.102:5600/',tag='ns=1;s=QCMS_ACCS.103.PLC_OPC_UI.MtSetdownWsSpdPosId',value=0)
# #主小车最后一次放箱海侧吊具对应操作位置代号



