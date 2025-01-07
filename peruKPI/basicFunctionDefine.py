from datetime import datetime,timedelta
import pandas as pd

def strChangeTime(inputStrTime):#定义字符串改为时间格式，字符串可以是'2024-12-21 16:46:39.151'格式或者'2024-12-21 16:46:39',
    # 定义时间字符串的格式
    time_format = "%Y-%m-%d %H:%M:%S.%f"
    # 定义时间字符串的格式
    time_format2 = "%Y-%m-%d %H:%M:%S"
    # 使用strptime()将字符串转换为datetime对象
    try:
        inputStrTime = datetime.strptime(inputStrTime, time_format)
    except ValueError:
        inputStrTime = datetime.strptime(inputStrTime, time_format2)
    return inputStrTime#inputStrTime=datetime.datetime类型数据


def timeChangeStr(inputTime):#定义时间格式改字符串，时间格式可以是'2024-12-21 16:46:39.151'格式或者'2024-12-21 16:46:39',
    # 定义时间字符串的格式
    time_format = "%Y-%m-%d %H:%M:%S.%f"
    # 定义时间字符串的格式
    time_format2 = "%Y-%m-%d %H:%M:%S"
    # 使用strptime()将字符串转换为datetime对象
    try:
        inputStrTime = inputTime.strftime(time_format2)
    except ValueError:
        inputStrTime = inputTime.strftime(time_format)
    return inputStrTime#inputStrTime=datetime.datetime类型数据


def wholeHourTime(inputTime):#inputTime可以是str可以是datetime.datetime类型,得到整点时间如"2024-12-20 20:09:38.607000"
    if isinstance(inputTime,str):#如果判断输入的类型是str类型，则按照下列逻辑计算出来这个时间对应的整点时间
        inputTime = strChangeTime(inputTime)
        previous_whole_hour = inputTime.replace(hour=inputTime.hour, minute=0, second=0, microsecond=0)
    if isinstance(inputTime,datetime):#如果判断输入的类型是str类型，则按照下列逻辑计算出来这个时间对应的整点时间
        previous_whole_hour = inputTime.replace(hour=inputTime.hour, minute=0, second=0, microsecond=0)

    return previous_whole_hour#得到的结果是整点时间2024-12-20 20:00:00，类型为<class 'datetime.datetime'>

def wholeHourTimes(startTime, endTime):#输入的可以是str也可以是datetime类型：2024-12-20 20:00:00;
    startWholeTime=wholeHourTime(startTime)
    endWholeTime=wholeHourTime(endTime)
    # 存储整点时间的列表
    whole_hours = []
    # 遍历直到当前时间超过结束时间
    while startWholeTime <= endWholeTime:
        # # 获取当前时间的整点时间（将分钟、秒、微秒部分置零）
        # whole_hour = start_time.replace(minute=0, second=0, microsecond=0)
        whole_hours.append(startWholeTime.strftime('%Y-%m-%d %H:%M:%S'))
        # 将当前时间增加一小时
        startWholeTime = startWholeTime+timedelta(hours=1)
    return whole_hours#得到的结果是一个lists，但是lists里面是str类型['2024-12-20 20:00:00', '2024-12-20 21:00:00', '2024-12-20 22:00:00']

def convertUtc_5(inputUtcTime):#将当前时间UTC减去5个小时作为秘鲁利马本地时间
    """
    将UTC时间减去5小时，返回新的datetime对象。

    :param utc_time: datetime.datetime对象，假设是UTC时间
    :return: 转换后的datetime.datetime对象，表示UTC-5时间
    """
    # 减去5小时
    utc_minus_5_time = inputUtcTime - timedelta(hours=5)
    return utc_minus_5_time#utc_minus_5_time=datetime.datetime类型数据

def stepTransToLanguage(step):#定义step对应的，转化为其它中文描述
    if step==11:
        outputLanguage = '到抓箱目标位置的阶段(全自动)'
    elif step==12:
        outputLanguage = '空吊具对箱阶段(全自动)'
    elif step==13:
        outputLanguage = '空吊具着箱阶段(全自动)'
    elif step==14:
        outputLanguage = '闭锁拉升阶段(全自动)'
    elif step==15:
        outputLanguage = '到放箱目标位置的阶段(全自动)'
    elif step==16:
        outputLanguage = '闭锁对箱阶段(全自动)'
    elif step==17:
        outputLanguage = '闭锁着箱阶段(全自动)'
    elif step==18:
        outputLanguage = '开锁拉升阶段(全自动)'
    elif step==21:
        outputLanguage = '到抓箱目标位置的阶段(手动带指令)'
    elif step==22:
        outputLanguage = '空吊具对箱阶段(手动带指令)'
    elif step==23:
        outputLanguage = '空吊具着箱阶段(手动带指令)'
    elif step==24:
        outputLanguage = '闭锁拉升阶段(手动带指令)'
    elif step==25:
        outputLanguage = '到放箱目标位置的阶段(手动带指令)'
    elif step==26:
        outputLanguage = '闭锁对箱阶段(手动带指令)'
    elif step==27:
        outputLanguage = '闭锁着箱阶段(手动带指令)'
    elif step==28:
        outputLanguage = '开锁拉升阶段(手动带指令)'
    elif step==31:
        outputLanguage = '到抓箱目标位置的阶段(全手动)'
    elif step==32:
        outputLanguage = '空吊具对箱阶段(全手动)'
    elif step==33:
        outputLanguage = '空吊具着箱阶段(全手动)'
    elif step==34:
        outputLanguage = '闭锁拉升阶段(全手动)'
    elif step==35:
        outputLanguage = '到放箱目标位置的阶段(全手动)'
    elif step==36:
        outputLanguage = '闭锁对箱阶段(全手动)'
    elif step==37:
        outputLanguage = '闭锁着箱阶段(全手动)'
    elif step==38:
        outputLanguage = '开锁拉升阶段(全手动)'
    elif step not in [11,12,13,14,15,16,17,18,21,22,23,24,25,26,27,28,31,32,33,34,35,36,37,38]:
        outputLanguage = f'{step}'
    return outputLanguage#输出的是一个str描述

def stateTransToLanguage(state):#定义state对应的，转化为其它中文描述
    if state==1:
        outputLanguage = '主小车处于等待指令状态，此时Step=11/21，是一条cycle的第一步'
    elif state==2:
        outputLanguage = '主起升到达安全高度后若起升小车停止，但允许启动半自动的状态，此时处于Step11/21/15/25'
    elif state==3:
        outputLanguage = '主起升到达安全高度后若起升小车停止，但允许启动半自动的状态，此时处于Step11/21/15/25'
    elif state==4:
        outputLanguage = '等待autolanding条件满足 1等待不能下降，此时处于Step12/16'
    elif state==5:
        outputLanguage = '主小车等待去集卡或者平台作业（条件满足，但司机未启动半自动或者推动手柄进行操作）'
    elif state==6:
        outputLanguage = '主小车等待去集卡作业（未收到QCMS对集卡指令）'
    elif state==7:
        outputLanguage = '主小车等待去集卡作业（集卡未到位）'
    elif state==8:
        outputLanguage = '主小车等待进入平台（门架小车占用平台)'
    elif state==9:
        outputLanguage = '主小车等待进入平台（锁扭工占用平台）'
    elif state==10:
        outputLanguage = '主小车等待进入平台（其他情况）'
    elif state==11:
        outputLanguage = '动大车 备注：包括换贝，指令内区分开 点动'
    elif state == 12:
        outputLanguage = '主小车允许人工介入(司机介入后，该点清零)，通常处于Step11/15'
    elif state == 13:
        outputLanguage = '主小车等待人工介入(司机介入后，该点清零)，通常处于Step12/16'
    elif state == 14:
        outputLanguage = '备用'
    elif state == 15:
        outputLanguage = '主小车等待人工介入(司机介入后，该点清零)，通常处于Step12/16'
    elif state == 16:
        outputLanguage = '放箱阶段起升上升（重放），通常处于Step16/26'
    elif state == 17:
        outputLanguage = '故障复位'
    elif state == 18:
        outputLanguage = '不正常减速，设备连锁或故障'
    elif state not in [11,12,13,14,15,16,17,18]:
        outputLanguage = f'{state}'
    return outputLanguage#输出的是一个str描述

def timeChange(td):#将'0 days 00:00:01.749000'格式化为00:00:01格式,td=# 假设你有一个 Timedelta 对象
    # 假设你有一个 Timedelta 对象
    # td = pd.Timedelta('0 days 00:00:01.749000')

    # 获取 Timedelta 对象中的总秒数（这里忽略了天数，因为我们只关心时间部分）
    total_seconds = td.total_seconds()

    # 计算小时、分钟和秒
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)

    # 格式化小时、分钟和秒，确保它们都是两位数
    formatted_hours = f"{hours:02d}"
    formatted_minutes = f"{minutes:02d}"
    formatted_seconds = f"{seconds:02d}"  # 注意：这里秒数已经自动截断了小数部分

    # 拼接成 HH:MM:SS 格式的字符串
    formatted_time = f"{formatted_hours}:{formatted_minutes}:{formatted_seconds}"

    return formatted_time
