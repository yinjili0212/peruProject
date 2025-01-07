import yaml
import datetime
from opcua import Client
# from logservice import logfunc
import winrm
import requests
import time
import re
import csv
import json
import hashlib
import sqliteHandle
import mysqldbHandle
import base64
import os
#读取配置文件
env_name = yaml.full_load(open('./config.yml', 'r', encoding='UTF-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='UTF-8').read()).get(env_name)
# log = logfunc(conf.get('logaddress'))#指定log打印的文件


def killWinCMD(ip,passwd,cmd):
    winConn = winrm.Session('http://'+ip+':5985/wsman',auth=('administrator',passwd))
    # ret = winConn.run_cmd('TASKKILL /F /IM '+cmd)
    ret = winConn.run_cmd(cmd)
    if ret.status_code!=0:
        print(ret.std_err)
    else:
        print(ret.std_out)

def get_current_time() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_current_timese() -> str:#带有毫秒的时间戳
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.433")

def get_current_timestr() -> str:#不带空格和-的字符串
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S") + f"{datetime.datetime.now().microsecond // 1000:03d}"
    # return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

def getTimeStamp() -> str:#时间戳1689172688843
    # 获取当前时间的时间戳（秒级）
    current_timestamp_seconds = time.time()

    # 将秒级时间戳转换为毫秒级时间戳
    current_timestamp_milliseconds = int(current_timestamp_seconds * 1000)
    current_timestamp_milliseconds=str(current_timestamp_milliseconds)
    return current_timestamp_milliseconds

def encryptionMD5(data):#秘鲁项目特有MD5签名加密，输入data为dict类型
    if isinstance(data, str):
        sign = hashlib.md5((data + '&' + conf.get('key')).encode('UTF-8')).hexdigest().upper()
    elif isinstance(data, dict):
        # 字典转字符串
        dict_str1 = json.dumps(data,ensure_ascii=False)
        sign = hashlib.md5((dict_str1 + '&' + conf.get('key')).encode('UTF-8')).hexdigest().upper()

    return sign


def encode_to_base64(input_string):
    # 将字符串编码为Base64
    encoded_string = base64.b64encode(input_string.encode('utf-8')).decode('utf-8')
    return encoded_string


def append_dict_to_csv(filename, data, fieldnames=None):#追加字典写入csv,{'age2': 'David', 'age': 28, 'city': 'Boston'}
    # 如果未提供字段名，则尝试从数据中的第一个字典获取
    if fieldnames is None:
        fieldnames = data.keys()

        # 检查文件是否存在，并确定是否需要写入表头
    header_written = os.path.isfile(filename) and os.path.getsize(filename) > 0

    # 打开文件以追加，如果文件不存在则自动创建
    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        # 如果文件是空的或者我们之前没有写入过表头，则写入表头
        if not header_written:
            writer.writeheader()

            # 写入数据行
        # for row in data:
        writer.writerow(data)
def csv_to_dict_list(csv_path):#[{'block': 'A1', 'bay': '59'},{'block': 'A1', 'bay': '59'}]
    """
    读取CSV文件,将每行数据转换为字典,并返回这些字典的列表。

    参数:
    csv_path (str): CSV文件的路径。

    返回:
    list of dict: 每行数据作为字典的列表。
    """
    data_dicts = []
    with open(csv_path, mode='r', encoding='UTF-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            data_dicts.append(row)
    return data_dicts

def write_opcua(ip,tag,value):#定义些OPCUA数值
    client = Client(ip)
    client.connect()
    var = client.get_node(tag)#OPC点的地址
    var.set_value(value)
    # 断开OPC UA客户端连接
    client.disconnect()

def read_opcua(ip,tag):#定义读OPCUA数值
    client = Client(ip)
    client.connect()
    var = client.get_node(tag)#OPC点的地址
    value = var.get_value()
    # 断开OPC UA客户端连接
    client.disconnect()
    return value
def read_opcua2(ip,tag):#定义读OPCUA数值opcuaip: 'opc.tcp://10.28.243.103:5600/'#OPCUA的地址
    try:
        client = Client(ip)
        client.connect()
        var = client.get_node(tag)#OPC点的地址
        value = var.get_value()
        # 断开OPC UA客户端连接
        client.disconnect()
    except CancelledError:
        print("OPC UA 连接被取消")
        value = 0
        # 可以在这里记录日志或执行其他错误处理逻辑
    except Exception as e:
        print(f"OPC UA 连接失败: {e}")
        value =0
    return value
def generateBmsTask(ascId,orderType,bay,equipType,cntrSize,count,writeType='w'):#生成BMS任务信息
    #ascId='201'  #orderType='DSCH' orderType='LOAD'  orderType='RECV'  orderType='DLVR' orderType='SHFI' orderType='SHFO'  orderType='PREP'
    #bay='046' equipType='AGV'  equipType='TRUCK'  cntrSize='20'#写入文件类型,'a'添加数据，'w'删除数据重新写入
    #判断blockId
    if ascId=='201':
        blockId='A1'
    elif ascId=='202':
        blockId = 'B1'
    elif ascId=='203':
        blockId = 'C1'
    elif ascId=='204':
        blockId = 'A2'
    elif ascId=='205':
        blockId = 'B2'
    elif ascId=='206':
        blockId = 'C2'
    elif ascId=='207':
        blockId = 'A3'
    elif ascId=='208':
        blockId = 'B3'
    elif ascId=='209':
        blockId = 'C3'
    elif ascId=='210':
        blockId = 'A4'
    elif ascId=='211':
        blockId = 'B4'
    elif ascId=='212':
        blockId = 'C4'
    elif ascId=='213':
        blockId = 'A5'
    elif ascId=='214':
        blockId = 'B5'
    elif ascId=='215':
        blockId = 'C5'



    #判断车道类型
    if equipType=='AGV':
        if blockId in ('A1','B1','C1','A3','B3','C3','A5','B5','C5'):
            laneType ='CW'
        elif blockId in ('A2','B2','C2','A4','B4','C4'):
            laneType = 'CL'
    elif equipType=='TRUCK':
        if blockId in ('A1','B1','C1','A3','B3','C3','A5','B5','C5'):
            laneType ='CL'
        elif blockId in ('A2','B2','C2','A4','B4','C4'):
            laneType = 'CW'


    # 任务列表
    tasks = []
    for i in range(count):
        singletask = {}  # 定义单个任务列表
        singletask['aorId'] = '1234'#固定值即可
        singletask['taskId'] = '1234'#固定值即可
        singletask['ascId'] =ascId
        singletask['orderType'] =orderType
        singletask['status'] ='SENT'
        singletask['operationProcess'] ='S'
        singletask['prePickUpFlag'] ='0'

        singletask['cntrId'] ='1'
        singletask['cntrNo'] ='ZPMC'+str(i).zfill(7)#ZPMC0000028#前面补全0,形成7位数
        singletask['cntrWeight'] ='1000'
        singletask['cntrHeight'] ='PQ'
        singletask['cntrIso'] =cntrSize+'GP'
        singletask['cntrSize'] =cntrSize
        singletask['cntrType'] ='GP'

        singletask['cntrId2'] =''
        singletask['cntrNo2'] =''
        singletask['cntrWeight2'] =''
        singletask['cntrIso2'] =''
        singletask['cntrSize2'] =''
        singletask['cntrType2'] =''
        singletask['origin2'] = ''
        singletask['destination2'] = ''
        singletask['posOnTruck2'] = ''

        singletask['origin'] =''
        # orderType='DSCH' orderType='LOAD'  orderType='RECV'  orderType='DLVR' orderType='SHFI' orderType='SHFO'  orderType='PREP'
        if orderType in ('DSCH','RECV','SHFI'):#初始位置车道#A1-053-CW-F
            if cntrSize=='20':
                singletask['origin']=blockId+'-'+bay+'-'+laneType+'-'+'F'
            elif cntrSize in ('40','45'):
                singletask['origin'] = blockId + '-' + bay + '-' + laneType + '-' + 'M'
        elif orderType in ('LOAD','DLVR','SHFO','PREP'):#初始位置堆场内A1-010-03-2
            if cntrSize=='20':
                singletask['origin']=blockId+'-'+bay+'-'+'01'+'-'+'1'
            elif cntrSize in ('40','45'):
                singletask['origin'] = blockId + '-' + bay + '-' + '01'+'-'+'1'


        singletask['originType'] =''
        if orderType in ('DSCH','RECV','SHFI'):#
            singletask['originType']='TRUCK'
        elif orderType in ('LOAD','DLVR','SHFO','PREP'):
            singletask['originType'] = 'YARD'


        singletask['destination'] =''
        # orderType='DSCH' orderType='LOAD'  orderType='RECV'  orderType='DLVR' orderType='SHFI' orderType='SHFO'  orderType='PREP'
        if orderType in ('LOAD','DLVR','SHFO'):#目的位置车道#A1-053-CW-F
            if cntrSize=='20':
                singletask['destination']=blockId+'-'+bay+'-'+laneType+'-'+'F'
            elif cntrSize in ('40','45'):
                singletask['destination'] = blockId + '-' + bay + '-' + laneType + '-' + 'M'
        elif orderType in ('DSCH','RECV','SHFI','PREP'):#目的位置堆场内A1-010-03-2
            if cntrSize=='20':
                singletask['destination']=blockId+'-'+bay+'-'+'02'+'-'+'1'
            elif cntrSize in ('40','45'):
                singletask['destination'] = blockId + '-' + bay + '-' + '02'+'-'+'1'


        singletask['destinationType'] =''
        if orderType in ('LOAD','DLVR','SHFO'):#
            singletask['destinationType']='TRUCK'
        elif orderType in ('DSCH','RECV','SHFI','PREP'):
            singletask['destinationType'] = 'YARD'

        singletask['posOnTruck'] =''
        if orderType in ('LOAD', 'DLVR', 'SHFO','DSCH','RECV','SHFI'):
            if cntrSize=='20':
                singletask['posOnTruck']='F'
            elif cntrSize in ('40', '45'):
                singletask['posOnTruck'] = 'M'



        singletask['truckNo'] =''
        if orderType in ('LOAD', 'DLVR', 'SHFO', 'DSCH', 'RECV', 'SHFI'):
            singletask['truckNo']='V'+str(i).zfill(3)#前面补全0，形成3位数

        singletask['truckType'] =''
        if equipType=='AGV':
            singletask['truckType']='A'
        elif equipType=='TRUCK':
            singletask['truckType'] = 'O'
        if orderType=='PREP':
            singletask['truckType'] = ''


        singletask['laneNo']=''
        if orderType in ('LOAD', 'DLVR', 'SHFO', 'DSCH', 'RECV', 'SHFI'):
            singletask['laneNo'] =blockId+'-'+laneType

        singletask['laneDirection'] ='0'


        tasks.append(singletask)  # 任务列表中加入对应的列表

    # 定义CSV文件名
    filename = conf.get('tosbmstaskcsv')
    # filename = 'test.csv'
    if writeType=='w':#直接写入原文件
        # 写入CSV文件
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            # 创建一个csv.DictWriter对象，传入文件对象和字段名列表
            writer = csv.DictWriter(file,fieldnames=['aorId','taskId','ascId','orderType','status','operationProcess','prePickUpFlag','cntrId','cntrNo','cntrWeight','cntrHeight','cntrIso','cntrSize','cntrType','cntrId2','cntrNo2','cntrWeight2','cntrHeight2','cntrIso2','cntrSize2','cntrType2','origin','origin2','originType','destination','destination2','destinationType','posOnTruck','posOnTruck2','truckNo','truckType','laneNo','laneDirection'
    ])

            # 写入表头
            writer.writeheader()

            # 遍历任务列表，将每个任务写入CSV文件
            for task in tasks:
                writer.writerow(task)

        print(f"CSV文件已写入：{filename}")
    elif  writeType=='a':#需要读取原文件行数且保留在后面添加
        # 读取CSV文件到字典列表
        rows = []
        fieldnames = []  # 用于存储列名（可选，如果CSV文件有表头）

        with open(filename, mode='r', newline='', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            fieldnames = csv_reader.fieldnames  # 获取列名（可选，如果你需要它们）
            for row in csv_reader:
                rows.append(row)

        # 在现有数据后添加一行新数据
        # 注意：这里需要确保新数据的键与CSV文件中的列名相匹配
        for task in tasks:
            rows.append(task)

        # 将更新后的数据写回CSV文件
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            csv_writer = csv.DictWriter(file, fieldnames=['aorId','taskId','ascId','orderType','status','operationProcess','prePickUpFlag','cntrId','cntrNo','cntrWeight','cntrHeight','cntrIso','cntrSize','cntrType','cntrId2','cntrNo2','cntrWeight2','cntrHeight2','cntrIso2','cntrSize2','cntrType2','origin','origin2','originType','destination','destination2','destinationType','posOnTruck','posOnTruck2','truckNo','truckType','laneNo','laneDirection'
    ])
            csv_writer.writeheader()  # 如果需要写入表头（CSV文件原本没有表头或你想覆盖表头则不需要这行）
            for row in rows:
                csv_writer.writerow(row)
        print('数据已成功更新！')

def generateQcmsTask(stsNo,taskType,ctnType,truckType,bay,count,writeType='w'):#生成QCMS任务信息
    # taskType='LOAD'/taskType='DSCH';# ctnType='20'/ctnType='40'/ctnType='45'/ctnType='twin20'#truckType='AGV'/truckType='TRUCK'#bay='10'#count=1
    # #writeType='w'删除数据重新写入,#writeType='a'在当前文件中追加数据
    # 任务列表
    tasks = []
    for i in range(count):
        singletask = {}#定义单个任务列表
        singletask['taskId']='20240716'#固定值即可
        singletask['taskVersion'] = '1'#固定值即可
        singletask['stsNo'] = stsNo
        singletask['vbtId'] = '2001'#固定值即可
        singletask['taskType'] = taskType
        singletask['equipmentType'] = 'M'#固定值即可

        singletask['wsHbCntrno'] = ''
        singletask['wsLbCntrno'] = ''
        singletask['wsHbOverLimitFlag'] = ''
        singletask['wsLbOverLimitFlag'] = ''
        singletask['wsHbISO'] = ''
        singletask['wsLbISO'] = ''
        if ctnType in ('20','40','45'):#输入20尺寸箱/40尺寸箱/45尺寸箱
            singletask['wsHbCntrno'] = 'ZPMC' + str(i).zfill(7)
            singletask['wsLbCntrno'] = ''
            singletask['wsHbOverLimitFlag']='N'
            singletask['wsHbISO'] = ctnType + 'GP'
        elif ctnType=='twin20':#输入双箱20尺寸箱
            singletask['wsHbCntrno'] = 'ZPMC' + str(i).zfill(7)
            singletask['wsLbCntrno'] = 'ABCD' + str(i).zfill(7)
            singletask['wsHbOverLimitFlag'] = 'N'
            singletask['wsLbOverLimitFlag'] = 'N'
            singletask['wsHbISO'] = '20GP'
            singletask['wsLbISO'] = '20GP'


        singletask['wsHbDangerFlag'] = ''#固定值即可
        singletask['wsLbDangerFlag'] = ''#固定值即可
        singletask['spreaderStatus'] = '1'#固定值即可

        if ctnType=='20':#20尺任务
            singletask['wsSpreaderSize'] = '2'#海侧吊具尺寸;0:该吊具不作业;1:未知吊具尺寸;2:20 尺寸;3:两个 20 尺寸;4:40 尺寸;5:45 尺寸;6:过高架、 超高、 超限箱;A:24 尺寸
        elif ctnType=='40':#40尺任务
            singletask['wsSpreaderSize'] = '4'
        elif ctnType=='45':#45尺任务
            singletask['wsSpreaderSize'] = '5'
        elif ctnType=='twin20':#双20尺尺任务
            singletask['wsSpreaderSize'] = '3'


        singletask['pickupPermissionFlag'] = '1'#固定值即可
        singletask['dropoffPermissionFlag'] = '1'#固定值即可

        singletask['origWsLoc'] = ''#//位置类型=SHIP时:船倍(2位或3位,)+排(2位)+层(2位),从后面开始解析,最后两位为层号,倒数第3和第4位为排号,倒数第5位之前为倍位号；
#位置类型=AGV或者TRUCK时:倒数第1位为车道号,倒数第2位为L或者T,L表示AGV,T表示集卡,倒数第3位之前为桥吊编号；
        if taskType=='LOAD':#装船任务
            if truckType == 'AGV':  # AGV任务
                singletask['origWsLoc'] = stsNo + 'L' + '3'  # 103L3
            elif truckType == 'TRUCK':  # TRUCK任务
                singletask['origWsLoc'] = stsNo + 'T' + '1'  # 103T1
        elif taskType=='DSCH':#卸船任务
            singletask['origWsLoc'] = bay + '0082'  # 100082



        singletask['origType'] = ''  #开始位置类型,SHIP:船上位置 SHIP;AGV:AGV（AGV）;TRUCK:集卡 TRUCK;PLATFORM:平台 PLATFORM;BAY:倍位 BAY;GROUND:地面（开关舱作业位置）;STSMT:主小车;STSPT:门架小车;SIDE:岸边缓冲区
        if taskType=='LOAD':#装船任务
            if truckType == 'AGV':  # AGV任务
                singletask['origType'] = 'AGV'
            elif truckType == 'TRUCK':  # TRUCK任务
                singletask['origType'] = 'TRUCK'
        elif taskType == 'DSCH':  # 卸船任务
            singletask['origType'] = 'SHIP'



        singletask['destWsLoc'] = ''  #
        if taskType=='DSCH':#卸船任务
            if truckType == 'AGV':  # AGV任务
                singletask['destWsLoc'] = stsNo + 'L' + '3'  # 103L3
            elif truckType == 'TRUCK':  # TRUCK任务
                singletask['destWsLoc'] = stsNo + 'T' + '1'  # 103T1
        elif taskType=='LOAD':#装船任务
            singletask['destWsLoc'] = bay + '0082'  # 100082

        singletask['destType'] = ''  #
        if taskType=='DSCH':#卸船任务
            if truckType == 'AGV':  # AGV任务
                singletask['destType'] = 'AGV'
            elif truckType == 'TRUCK':  # TRUCK任务
                singletask['destType'] = 'TRUCK'
        elif taskType == 'LOAD':  # 装船任务
            singletask['destType'] = 'SHIP'

        singletask['origHtsNoWs'] = ''  #开始位置海侧水平运输工具编号;
        singletask['origHtsPosWs'] = ''  #开始位置海侧水平运输工具作业位置;CENTER:中心位置;HB:泊位高缆桩方向;LB:泊位低缆桩方向
        if taskType=='LOAD':#装船任务
            singletask['origHtsNoWs']='V'+str(i).zfill(4)
            if ctnType == '20':  # 20尺任务
                singletask['origHtsPosWs'] = 'HB'
            elif ctnType == '40':  # 40尺任务
                singletask['origHtsPosWs'] = 'CENTER'
            elif ctnType == '45':  # 45尺任务
                singletask['origHtsPosWs'] = 'CENTER'
            elif ctnType == 'twin20':  # 双20尺尺任务
                singletask['origHtsPosWs'] = 'CENTER'




        singletask['destHtsNoWs'] = ''  #
        singletask['destHtsPosWs'] = ''  #
        if taskType=='DSCH':#卸船任务
            singletask['destHtsNoWs']='V'+str(i).zfill(4)
            if ctnType == '20':  # 20尺任务
                singletask['destHtsPosWs'] = 'HB'
            elif ctnType == '40':  # 40尺任务
                singletask['destHtsPosWs'] = 'CENTER'
            elif ctnType == '45':  # 45尺任务
                singletask['destHtsPosWs'] = 'CENTER'
            elif ctnType == 'twin20':  # 双20尺尺任务
                singletask['destHtsPosWs'] = 'CENTER'


        singletask['taskStatus'] = 'ENTERED'  #固定值即可
        singletask['errorCode'] = ''  #固定值即可
        singletask['errorMsg'] = ''  #固定值即可
        singletask['operateTime'] = 'sysdate'  #固定值即可
        singletask['responseTime'] = 'sysdate'  #固定值即可

        tasks.append(singletask)#任务列表中加入对应的列表

    # 定义CSV文件名
    filename = conf.get('tosqcmstaskcsv')
    if writeType=='w':#直接覆盖数据
        # 写入CSV文件
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            # 创建一个csv.DictWriter对象，传入文件对象和字段名列表
            writer = csv.DictWriter(file, fieldnames=['taskId','taskVersion','stsNo','vbtId','taskType','equipmentType','wsHbCntrno','wsLbCntrno','wsHbOverLimitFlag','wsLbOverLimitFlag','wsHbDangerFlag','wsLbDangerFlag','spreaderStatus','wsSpreaderSize','wsHbISO','wsLbISO','pickupPermissionFlag','dropoffPermissionFlag','origWsLoc','origType','destWsLoc','destType','origHtsNoWs','origHtsPosWs','destHtsNoWs','destHtsPosWs','taskStatus','errorCode','errorMsg','operateTime','responseTime'])

            # 写入表头
            writer.writeheader()

            # 遍历任务列表，将每个任务写入CSV文件
            for task in tasks:
                writer.writerow(task)

        print(f"CSV文件已写入：{filename}")
    elif writeType=='a':#追加数据
        # 读取CSV文件到字典列表
        rows = []
        fieldnames = []  # 用于存储列名（可选，如果CSV文件有表头）

        with open(filename, mode='r', newline='', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            fieldnames = csv_reader.fieldnames  # 获取列名（可选，如果你需要它们）
            for row in csv_reader:
                rows.append(row)

        # 在现有数据后添加一行新数据
        # 注意：这里需要确保新数据的键与CSV文件中的列名相匹配
        for task in tasks:
            rows.append(task)

        # 将更新后的数据写回CSV文件
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            csv_writer = csv.DictWriter(file, fieldnames=['taskId','taskVersion','stsNo','vbtId','taskType','equipmentType','wsHbCntrno','wsLbCntrno','wsHbOverLimitFlag','wsLbOverLimitFlag','wsHbDangerFlag','wsLbDangerFlag','spreaderStatus','wsSpreaderSize','wsHbISO','wsLbISO','pickupPermissionFlag','dropoffPermissionFlag','origWsLoc','origType','destWsLoc','destType','origHtsNoWs','origHtsPosWs','destHtsNoWs','destHtsPosWs','taskStatus','errorCode','errorMsg','operateTime','responseTime'])
            csv_writer.writeheader()  # 如果需要写入表头（CSV文件原本没有表头或你想覆盖表头则不需要这行）
            for row in rows:
                csv_writer.writerow(row)
        print('数据已成功更新！')

def insertdatatodb():
    # 数据库文件路径
    db_path = conf.get('localSqllitedb')

    # 连接到SQLite数据库
    # 如果数据库不存在,它将被自动创建
    o = sqliteHandle.sqliteHandler(db_path)

    # CSV文件路径
    csv_file_path = conf.get('tosbmstaskcsv')

    # 如果想要将行数据转换为字典,可以使用csv.DictReader
    # 注意：这种方法自动跳过了标题行,并将其用作字典的键
    with open(csv_file_path, mode='r', newline='', encoding='UTF-8') as csvfile:
        csvreader = csv.DictReader(csvfile)
        # 逐行读取数据（已经是字典格式,且自动跳过了标题行）
        for row in csvreader:
            # 现在你可以通过标题名来访问数据了
            # print(row)
            if row['aorId'] != '':  # 查询出来的csv有数据才操作下面的步骤
                try:
                    aorId = int(get_current_timestr()[-9:])
                    taskId = aorId#int(get_current_timestr()[-9:])
                    ascId = row['ascId']
                    orderType = row['orderType']
                    status = row['status']
                    operationProcess = row['operationProcess']
                    prePickUpFlag = row['prePickUpFlag']
                    cntrId = int(get_current_timestr()[-3:])
                    cntrNo = row['cntrNo']
                    if row['cntrWeight']=='':
                        cntrWeight=0
                    else:
                        cntrWeight = int(row['cntrWeight'])
                    cntrHeight = row['cntrHeight']
                    cntrIso = row['cntrIso']
                    cntrSize = row['cntrSize']
                    cntrType = row['cntrType']
                    # cntrId2 = int(row['cntrId2'])
                    # cntrNo2 = row['cntrNo2']
                    # cntrWeight2 = int(row['cntrWeight2'])
                    # cntrHeight2 = row['cntrHeight2']
                    # cntrIso2 = row['cntrIso2']
                    # cntrSize2 = row['cntrSize2']
                    # cntrType2 = row['cntrType2']
                    origin = row['origin']
                    # origin2 = row['origin2']
                    originType = row['originType']
                    destination = row['destination']
                    destination2 = row['destination2']
                    destinationType = row['destinationType']
                    posOnTruck = row['posOnTruck']
                    # posOnTruck2 = row['posOnTruck2']
                    truckNo = row['truckNo']
                    truckType = row['truckType']
                    laneNo = row['laneNo']
                    laneDirection = row['laneDirection']

                    # # 插入数据
                    insert_query = '''INSERT INTO tosbmstask (aorId,taskId,ascId,orderType,status,operationProcess,
                                prePickUpFlag,cntrId,cntrNo,cntrWeight,cntrHeight,cntrIso,cntrSize,cntrType,cntrId2,
                                cntrNo2,cntrWeight2,cntrHeight2,cntrIso2,cntrSize2,cntrType2,origin,origin2,originType,destination,
                                destination2,destinationType,posOnTruck,posOnTruck2,truckNo,truckType,laneNo,laneDirection) VALUES
                                ({aorIdreplace1},{taskIdreplace},"{ascIdreplace}","{orderTypereplace}","{statusreplace}","{operationProcessreplace}",
                                "{prePickUpFlagreplace}",{cntrIdreplace},"{cntrNoreplace}",{cntrWeightreplace},"{cntrHeightreplace}",
                                "{cntrIsoreplace}","{cntrSizereplace}","{cntrTypereplace}",null,null,
                                null,null,null,null,null,
                                "{originreplace}",null,"{originTypereplace}","{destinationreplace}",null,
                                "{destinationTypereplace}","{posOnTruckreplace}",null,"{truckNoreplace}","{truckTypereplace}","{laneNoreplace}","{laneDirectionreplace}")''' \
                        .format(aorIdreplace1=aorId, taskIdreplace=taskId, ascIdreplace=ascId,
                                orderTypereplace=orderType,
                                statusreplace=status, operationProcessreplace=operationProcess,
                                prePickUpFlagreplace=prePickUpFlag, cntrIdreplace=cntrId, cntrNoreplace=cntrNo,
                                cntrWeightreplace=cntrWeight, cntrHeightreplace=cntrHeight,
                                cntrIsoreplace=cntrIso, cntrSizereplace=cntrSize, cntrTypereplace=cntrType,
                                originreplace=origin, originTypereplace=originType, destinationreplace=destination,
                                destinationTypereplace=destinationType, posOnTruckreplace=posOnTruck,
                                truckNoreplace=truckNo, truckTypereplace=truckType, laneNoreplace=laneNo,
                                laneDirectionreplace=laneDirection)

                    o.executesql(insert_query)
                    # print(insert_query)
                    time.sleep(0.1)

                except ValueError:
                    # 处理数据类型转换错误（例如，如果年龄不是整数）
                    print(f"无法将行插入数据库，因为数据类型不匹配。")

def insertQcmsdatatodb():
    # 数据库文件路径
    db_path = conf.get('localSqllitedb')

    # 连接到SQLite数据库
    # 如果数据库不存在,它将被自动创建
    o = sqliteHandle.sqliteHandler(db_path)


    # CSV文件路径
    csv_file_path = conf.get('tosqcmstaskcsv')

    # 如果想要将行数据转换为字典,可以使用csv.DictReader
    # 注意：这种方法自动跳过了标题行,并将其用作字典的键
    with open(csv_file_path, mode='r', newline='', encoding='UTF-8') as csvfile:
        csvreader = csv.DictReader(csvfile)
        # 逐行读取数据（已经是字典格式,且自动跳过了标题行）
        for row in csvreader:
            # 现在你可以通过标题名来访问数据了
            if row['taskId'] != '':  # 查询出来的csv有数据才操作下面的步骤
                try:
                    taskId = int(get_current_timestr())
                    taskVersion = int('1')
                    stsNo = row['stsNo']
                    vbtId = int(row['vbtId'])
                    taskType = row['taskType']
                    equipmentType = row['equipmentType']
                    spreaderStatus = int(row['spreaderStatus'])
                    wsSpreaderSize = row['wsSpreaderSize']
                    wsHbISO = row['wsHbISO']
                    wsLbISO = row['wsLbISO']
                    pickupPermissionFlag = int(row['pickupPermissionFlag'])
                    dropoffPermissionFlag = int(row['dropoffPermissionFlag'])
                    origWsLoc = row['origWsLoc']
                    origType = row['origType']
                    destWsLoc = row['destWsLoc']
                    destType = row['destType']
                    origHtsNoWs = row['origHtsNoWs']
                    origHtsPosWs = row['origHtsPosWs']
                    destHtsNoWs = row['destHtsNoWs']
                    destHtsPosWs = row['destHtsPosWs']
                    taskStatus = row['taskStatus']
                    operateTime = get_current_timese()
                    wsHbCntrno = row['wsHbCntrno']
                    wsLbCntrno = row['wsLbCntrno']
                    wsHbOverLimitFlag = row['wsHbOverLimitFlag']
                    wsLbOverLimitFlag = row['wsLbOverLimitFlag']
                    wsHbDangerFlag = row['wsHbDangerFlag']
                    wsLbDangerFlag = row['wsLbDangerFlag']

                    if taskType == 'LOAD':
                        trucktypestr = origWsLoc[-2:-1]
                        if trucktypestr == 'L':
                            truckType = 'AGV'
                        elif trucktypestr == 'T':
                            truckType = 'TRUCK'
                        else:
                            truckType = ''#从任务中判断水平运输设备是AGV还是有人集卡

                        lane = origWsLoc[-1:]#作业集卡车道

                        if origHtsPosWs=='HB':#assert container on truck pos
                            posOnTruck = 'H'
                        elif origHtsPosWs=='LB':
                            posOnTruck='L'
                        elif origHtsPosWs=='CENTER':
                            posOnTruck='C'
                        else:
                            posOnTruck=''


                        ahtId = origHtsNoWs#提取ahtId

                        #提取作业贝位后更新到此条任务中
                        currentBayId = int(destWsLoc[:-4])




                    if taskType == 'DSCH':
                        trucktypestr = destWsLoc[-2:-1]
                        if trucktypestr == 'L':
                            truckType = 'AGV'
                        elif trucktypestr == 'T':
                            truckType = 'TRUCK'
                        else:
                            truckType = ''

                        lane = destWsLoc[-1:]#作业集卡车道

                        if destHtsPosWs == 'HB':  # assert container on truck pos
                            posOnTruck = 'H'
                        elif destHtsPosWs == 'LB':
                            posOnTruck = 'L'
                        elif destHtsPosWs == 'CENTER':
                            posOnTruck = 'C'
                        else:
                            posOnTruck = ''

                        ahtId = destHtsNoWs

                        # 提取作业贝位后更新到此条任务中
                        currentBayId = int(origWsLoc[:-4])

                    # 插入数据
                    insert_sql = '''INSERT INTO tosqcmstask (taskId,taskVersion,stsNo,vbtId,taskType,equipmentType,
                                spreaderStatus,wsSpreaderSize,wsHbISO,wsLbISO,pickupPermissionFlag,dropoffPermissionFlag,origWsLoc,origType,
                                destWsLoc,destType,origHtsNoWs,origHtsPosWs,destHtsNoWs,destHtsPosWs,taskStatus,operateTime,truckType,lane,posOnTruck,ahtId,currentBayId,\
                                wsHbCntrno,wsLbCntrno,wsHbOverLimitFlag,wsLbOverLimitFlag,wsHbDangerFlag,wsLbDangerFlag) VALUES
                                ({taskId},{taskVersion},"{stsNo}",{vbtId},"{taskType}","{equipmentType}",{spreaderStatus},"{wsSpreaderSize}","{wsHbISO}","{wsLbISO}",\
                                {pickupPermissionFlag},{dropoffPermissionFlag},"{origWsLoc}","{origType}","{destWsLoc}","{destType}","{origHtsNoWs}","{origHtsPosWs}",\
                                "{destHtsNoWs}","{destHtsPosWs}","{taskStatus}","{operateTime}","{truckType}","{lane}","{posOnTruck}","{ahtId}",{currentBayId},"{wsHbCntrno}",\
                                "{wsLbCntrno}","{wsHbOverLimitFlag}","{wsLbOverLimitFlag}","{wsHbDangerFlag}","{wsLbDangerFlag}")'''\
                        .format(taskId=taskId,taskVersion=taskVersion, stsNo=stsNo,vbtId=vbtId, taskType=taskType, equipmentType=equipmentType,\
                                spreaderStatus=spreaderStatus, wsSpreaderSize=wsSpreaderSize,wsHbISO=wsHbISO, wsLbISO=wsLbISO,pickupPermissionFlag=pickupPermissionFlag,\
                                dropoffPermissionFlag=dropoffPermissionFlag, origWsLoc=origWsLoc,origType=origType, destWsLoc=destWsLoc,destType=destType, \
                                origHtsNoWs=origHtsNoWs, origHtsPosWs=origHtsPosWs,destHtsNoWs=destHtsNoWs,destHtsPosWs=destHtsPosWs,taskStatus=taskStatus,\
                                operateTime=operateTime,truckType=truckType,lane=lane,posOnTruck=posOnTruck,ahtId=ahtId,currentBayId=currentBayId,\
                                wsHbCntrno=wsHbCntrno,wsLbCntrno=wsLbCntrno,wsHbOverLimitFlag=wsHbOverLimitFlag,wsLbOverLimitFlag=wsLbOverLimitFlag,wsHbDangerFlag=wsHbDangerFlag,wsLbDangerFlag=wsLbDangerFlag)
                    # print(insert_sql)
                    o.executesql(insert_sql)
                    # print(o.executesql("select * from tosqcmstask"))
                    time.sleep(0.1)

                except ValueError:
                    # 处理数据类型转换错误（例如，如果年龄不是整数）
                    print(f"无法将行插入数据库，因为数据类型不匹配。")

def postOcrTruckMsg():#OCR -》BMS 2024/08/26发送truck信号
    o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
    dataaboutctnno = o.query('''select * from tosbmstask where isSendOcrTruck=0 and status="SENT"''')
    if len(dataaboutctnno)!=0:#查询出来数据库中不为空
        for dataaboutctnno_dict in dataaboutctnno:
            if dataaboutctnno_dict['isSendOcrTruck']==0:#查询没有发送OCR的数据
                if dataaboutctnno_dict['bmssendstep'] in ('0','1','2','3','4'):#需要发送OCR的条件
                    url = conf.get('bmshttpip')+conf.get('ocrbmsurls')[3] #'http://10.28.243.103:8003/WellOcean/PostOcrInfo'
                    #对work_type进行判断
                    work_type=''
                    if dataaboutctnno_dict['orderType'] in ('DSCH','RECV','SHFI'):#任务类型: DSCH("卸船");LOAD("装船")RECV("道口进箱")DLVR("道口提箱")SHFI("堆场转入")SHFO("堆场转出"PREP("堆场归并
                        work_type='in'
                        send_point=4#4：进箱发送⻋号、箱号和ISO识别信息
                    elif dataaboutctnno_dict['orderType'] in ('LOAD','DLVR','SHFO'):
                        work_type='out'
                        send_point = 3# 3：出箱发送⻋号信息
                    else:#理箱场景，不发送集卡
                        send_point = 0


                    if dataaboutctnno_dict['laneNo'][-1:]=='W':#查询出来tosbmstask表laneNo字段如果最后一个字符是W表示是海侧，L表示陆侧
                        side='water'
                    elif dataaboutctnno_dict['laneNo'][-1:]=='L':
                        side = 'land'
                    else:
                        side = ''

                    data = {
"rmg": dataaboutctnno_dict['ascId'],
"side": side,
"token": get_current_timestr(),
"truck_plate": dataaboutctnno_dict['truckNo'],
"truck_plate_img": "http://127.0.0.1/imagesave/detect.jpg",
"truck_roof_num": "T01",
"truck_roof_num_img": "http://127.0.0.1/imagesave/detect.jpg",
"ctn_num": dataaboutctnno_dict['cntrNo'],
"iso": dataaboutctnno_dict['cntrIso'],
"ctn_num_img": "http://127.0.0.1/imagesave/detect.jpg",
"ctn_door_is_open": False,
"ctn_door_is_open_img": "http://127.0.0.1/imagesave/detect.jpg",
"ctn_door_face": "lowrow",
"ctn_door_face_img": "http://127.0.0.1/imagesave/detect.jpg",
"work_type": work_type,
"send_point": send_point,#不同的发送时机发送不同的数据，下⾯是详细说明：
# 1：出箱堆区闭锁发送箱⻔朝向、箱⻔异常打开和箱号识别信息（#不包括截图）
# 2：出箱吊具在⻋道上⽅下降到⼀定⾼度发送ISO识别信息和箱⻔朝向截图、箱⻔异常打开截图
# 3：出箱发送⻋号信息
# 4：进箱发送⻋号、箱号和ISO识别信息
# 5：进箱吊具在⻋道上⽅下降到⼀定⾼度发送和箱⻔朝向和箱⻔异常打开（包括截图）'''
"timestamp": getTimeStamp()
}

                    try:
                        headers = {
                            'equipId': encode_to_base64(dataaboutctnno_dict['ascId']),
                            'Content-Type':'application/json; charset=utf-8',
                            'authorization': 'serial_num'

                        }
                        response = requests.post(url, json=data, headers=headers)
                        print(response.status_code)
                        if response.status_code == 200:  # 发送成功将csv中那行数据进行更新
                            o.executesql('''update tosbmstask set isSendOcrTruck=1 where aorId={0}'''.format(
                                dataaboutctnno_dict['aorId']))
                        print(get_current_time())
                        print("OCR->BMS")
                        print(data)  # 打印发送的消息
                        print("postOcrTruckInfo-response" + response.text)
                    except requests.exceptions.ConnectionError as e:
                        print("Failed to connect to the server:", e)

def postOcrCtnMsg():#OCR -》BMS 2024/08/26发送CTN信号
    o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
    dataaboutctnno = o.query('''select * from tosbmstask where isSendOcrCtn=0 and status="SENT"''')
    if len(dataaboutctnno)!=0:#查询出来数据库中不为空
        for dataaboutctnno_dict in dataaboutctnno:
            if dataaboutctnno_dict['isSendOcrCtn']==0:#查询没有发送OCR的数据
                if dataaboutctnno_dict['bmssendstep'] in ('0','1','2','3','4'):#需要发送OCR的条件
                    url = conf.get('bmshttpip')+conf.get('ocrbmsurls')[3] #'http://10.28.243.103:8003/WellOcean/PostOcrInfo'
                    #对work_type进行判断
                    work_type=''
                    if dataaboutctnno_dict['orderType'] in ('DSCH','RECV','SHFI'):#任务类型: DSCH("卸船");LOAD("装船")RECV("道口进箱")DLVR("道口提箱")SHFI("堆场转入")SHFO("堆场转出"PREP("堆场归并
                        work_type='in'
                        send_point=4#4：进箱发送⻋号、箱号和ISO识别信息
                    elif dataaboutctnno_dict['orderType'] in ('LOAD','DLVR','SHFO'):
                        work_type='out'
                        send_point = 1# 1：出箱堆区闭锁发送箱⻔朝向、箱⻔异常打开和箱号识别信息（#不包括截图）
                    else:#理箱场景，不发送集卡
                        send_point = 0


                    if dataaboutctnno_dict['laneNo'][-1:]=='W':#查询出来tosbmstask表laneNo字段如果最后一个字符是W表示是海侧，L表示陆侧
                        side='water'
                    elif dataaboutctnno_dict['laneNo'][-1:]=='L':
                        side = 'land'
                    else:
                        side = ''

                    data = {
"rmg": dataaboutctnno_dict['ascId'],
"side": side,
"token": get_current_timestr(),
"truck_plate": dataaboutctnno_dict['truckNo'],
"truck_plate_img": "http://127.0.0.1/imagesave/detect.jpg",
"truck_roof_num": "T01",
"truck_roof_num_img": "http://127.0.0.1/imagesave/detect.jpg",
"ctn_num": dataaboutctnno_dict['cntrNo'],
"iso": dataaboutctnno_dict['cntrIso'],
"ctn_num_img": "http://127.0.0.1/imagesave/detect.jpg",
"ctn_door_is_open": False,
"ctn_door_is_open_img": "http://127.0.0.1/imagesave/detect.jpg",
"ctn_door_face": "lowrow",
"ctn_door_face_img": "http://127.0.0.1/imagesave/detect.jpg",
"work_type": work_type,
"send_point": send_point,#不同的发送时机发送不同的数据，下⾯是详细说明：
# 1：出箱堆区闭锁发送箱⻔朝向、箱⻔异常打开和箱号识别信息（#不包括截图）
# 2：出箱吊具在⻋道上⽅下降到⼀定⾼度发送ISO识别信息和箱⻔朝向截图、箱⻔异常打开截图
# 3：出箱发送⻋号信息
# 4：进箱发送⻋号、箱号和ISO识别信息
# 5：进箱吊具在⻋道上⽅下降到⼀定⾼度发送和箱⻔朝向和箱⻔异常打开（包括截图）
"timestamp": getTimeStamp()
}

                    try:
                        headers = {
                            'equipId': encode_to_base64(dataaboutctnno_dict['ascId']),
                            'Content-Type':'application/json; charset=utf-8',
                            'authorization': 'serial_num'

                        }
                        response = requests.post(url, json=data, headers=headers)
                        print(response.status_code)
                        if response.status_code == 200:  # 发送成功将csv中那行数据进行更新
                            o.executesql('''update tosbmstask set isSendOcrCtn=1 where aorId={0}'''.format(
                                dataaboutctnno_dict['aorId']))
                        print(get_current_time())
                        print("OCR->BMS")
                        print(data)  # 打印发送的消息
                        print("postOcrCtnInfo-response" + response.text)
                    except requests.exceptions.ConnectionError as e:
                        print("Failed to connect to the server:", e)

def postf2bUpdateInterajson():#/f2b/updateIntera FMS给BMS发送更新交互状态
    o=sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
    dataaboutinte = o.query('''select * from fmsbms''')
    if len(dataaboutinte) != 0:  # 查询出来数据库中不为空
        for dataaboutinte_dict in dataaboutinte:#dataaboutinte=[{},{},{}]将数据库每一条拿出来判断
            ###判断要发送的BMS所在的服务器
            if dataaboutinte_dict['block'] in ('A1','B1','C1'):
                bmshttpip=conf.get('bmshttpip_1')
            elif dataaboutinte_dict['block'] in ('A2','B2','C2'):
                bmshttpip = conf.get('bmshttpip_2')
            elif dataaboutinte_dict['block'] in ('A3', 'B3', 'C3'):
                bmshttpip = conf.get('bmshttpip_3')
            elif dataaboutinte_dict['block'] in ('A4', 'B4', 'C4'):
                bmshttpip = conf.get('bmshttpip_4')
            elif dataaboutinte_dict['block'] in ('A5', 'B5', 'C5'):
                bmshttpip = conf.get('bmshttpip_5')
            else:
                bmshttpip = conf.get('bmshttpip_5')

            url = bmshttpip + conf.get('fmsbmsurls')[0]  # 'http://10.28.243.103:8003/f2b/updateIntera'

            ##提取当前交互表信息
            block = dataaboutinte_dict['block']
            bay = dataaboutinte_dict['bay']
            ascId = dataaboutinte_dict['ascId']
            ascCtnId = dataaboutinte_dict['ascCtnId']
            ascCtnType = dataaboutinte_dict['ascCtnType']
            posOnAht = dataaboutinte_dict['posOnAht']
            ascStatus =dataaboutinte_dict['ascStatus']
            ahtIdFromEcs = dataaboutinte_dict['ahtIdFromEcs']
            ahtId = dataaboutinte_dict['ahtId']
            ahtCtnId = dataaboutinte_dict['ahtCtnId']
            ahtStatus = dataaboutinte_dict['ahtStatus']
            ahtTaskType = dataaboutinte_dict['ahtTaskType']


            if ascStatus=='INIT' and (ahtStatus=='' or ahtStatus==None):#需要发送FMS INIT状态给BMS
                dataaboutask = o.query(
                    '''select * from tosbmstask where ascId="{0}" and status="SENT" and bmssendstep is not NULL order by aorId asc'''.format(
                        dataaboutinte_dict['ascId']))
                if len(dataaboutask)!=0:

                    if dataaboutask[0]['orderType'] in ('DSCH', 'RECV','SHFI'):  # 任务类型: DSCH("卸船");LOAD("装船")RECV("道口进箱")DLVR("道口提箱")SHFI("堆场转入")SHFO("堆场转出"PREP("堆场归并
                        ahtTaskType = 'DELIVER'
                    elif dataaboutask[0]['orderType'] in ('LOAD', 'DLVR', 'SHFO'):
                        ahtTaskType = 'RECEIVE'
                    else:
                        ahtTaskType = ''


                    data = {
                    "block": block,
                    "bay": bay,
                    "ahtId": ahtIdFromEcs,
                    "ahtCtnId": ascCtnId,
                    "ahtStatus": "INIT",
                    "ahtTaskType": ahtTaskType,
                    "ahtUpdateTime": get_current_timese()}

                    if (data["ahtStatus"] == 'INIT' and dataaboutinte_dict['init_fail_sendcount'] <= 2):
                        try:
                            headers = {
                                'charset': 'UTF-8',
                                'signType': 'MD5',
                                'mchId': 'zpmc',
                                'equipId': encode_to_base64(block),
                                'sign': encryptionMD5(json.dumps(data))}

                            response = requests.post(url, json=data, headers=headers)
                            # 检查响应
                            print(response.status_code, data)
                            # 读取返回的消息内容
                            response_content = response.text
                            objt_dict = json.loads(response_content)
                            response_status = objt_dict['status']

                            if response.status_code == 200:  # 发送成功将csv中那行数据进行更新
                                if response_status=="0":#BMS反馈成功
                                    executesql_insqlite = '''update fmsbms set init_fail_sendcount=0,ahtId="{ahtId}",ahtCtnId="{ahtCtnId}",ahtStatus="{ahtStatus}",ahtTaskType="{ahtTaskType}" where block="{block}"'''\
                                        .format(ahtId=data['ahtId'], ahtCtnId=data['ahtCtnId'], ahtStatus=data['ahtStatus'],
                                        ahtTaskType=data['ahtTaskType'], block=data['block'])
                                    o.executesql(executesql_insqlite)
                                    print("更新FMS&BMS交互成功")
                                    print(executesql_insqlite)
                                else:#BMS反馈失败
                                    init_fail_sendcount = dataaboutinte_dict['init_fail_sendcount'] + 1  # 从数据库查询出来对应的字段值+1
                                    executesql_insqlite = '''update fmsbms set init_fail_sendcount={init_fail_sendcount} where block="{block}"'''\
                                        .format(block=data['block'],init_fail_sendcount=init_fail_sendcount)
                                    o.executesql(executesql_insqlite)
                                    print("更新FMS&BMS交互失败")
                                    print(executesql_insqlite)
                        except requests.exceptions.ConnectionError as e:
                            print("Failed to connect to the server:", e)
            elif ascStatus=='REQ_LOCK' and ahtStatus=='INIT':#需要发送LOCKED状态
                ahtStatus='LOCKED'
                data = {
                    "block": block,
                    "bay": bay,
                    "ahtId": ahtIdFromEcs,
                    "ahtCtnId": ascCtnId,
                    "ahtStatus": ahtStatus,
                    "ahtTaskType": ahtTaskType,
                    "ahtUpdateTime": get_current_timese()}

                if (data["ahtStatus"] == 'LOCKED' and dataaboutinte_dict['locked_fail_sendcount'] <= 2):
                    try:
                        headers = {
                            'charset': 'UTF-8',
                            'signType': 'MD5',
                            'mchId': 'zpmc',
                            'equipId': encode_to_base64(block),
                            'sign': encryptionMD5(json.dumps(data))}
                        # time.sleep(20)

                        response = requests.post(url, json=data, headers=headers)
                        # 检查响应
                        print(response.status_code, data)
                        # 读取返回的消息内容
                        response_content = response.text
                        objt_dict = json.loads(response_content)
                        response_status = objt_dict['status']

                        if response.status_code == 200:  # 发送成功将csv中那行数据进行更新
                            if response_status == "0":  # BMS反馈成功
                                executesql_insqlite = '''update fmsbms set locked_fail_sendcount=0,ahtId="{ahtId}",ahtCtnId="{ahtCtnId}",ahtStatus="{ahtStatus}",ahtTaskType="{ahtTaskType}" where block="{block}"''' \
                                    .format(ahtId=data['ahtId'], ahtCtnId=data['ahtCtnId'], ahtStatus=data['ahtStatus'],
                                            ahtTaskType=data['ahtTaskType'], block=data['block'])
                                o.executesql(executesql_insqlite)
                                print("更新FMS&BMS交互成功")
                                print(executesql_insqlite)
                            else:  # BMS反馈失败
                                locked_fail_sendcount = dataaboutinte_dict['locked_fail_sendcount'] + 1  # 从数据库查询出来对应的字段值+1
                                executesql_insqlite = '''update fmsbms set locked_fail_sendcount={locked_fail_sendcount} where block="{block}"''' \
                                    .format(locked_fail_sendcount=locked_fail_sendcount,block=block)
                                print("更新FMS&BMS交互失败")
                                print(executesql_insqlite)
                                o.executesql(executesql_insqlite)
                    except requests.exceptions.ConnectionError as e:
                        print("Failed to connect to the server:", e)
            elif ascStatus == 'COMPLETE' and ahtStatus == 'LOCKED':  # 需要发送离开车道
                ahtStatus = None
                data = {
                    "block": block,
                    "bay": bay,
                    "ahtId": '',
                    "ahtCtnId": '',
                    "ahtStatus": '',
                    "ahtTaskType": '',
                    "ahtUpdateTime": get_current_timese()}

                if dataaboutinte_dict['leavelane_fail_sendcount'] <= 2:
                    try:
                        headers = {
                            'charset': 'UTF-8',
                            'signType': 'MD5',
                            'mchId': 'zpmc',
                            'equipId': encode_to_base64(block),
                            'sign': encryptionMD5(json.dumps(data))}

                        response = requests.post(url, json=data, headers=headers)
                        # 检查响应
                        print(response.status_code, data)
                        # 读取返回的消息内容
                        response_content = response.text
                        objt_dict = json.loads(response_content)
                        response_status = objt_dict['status']

                        if response.status_code == 200:  # 发送成功将csv中那行数据进行更新
                            # executesql_insqlite = '''update fmsbms set bay='',ascId='',ascCtnId='',ascCtnType='',posOnAht='',ascStatus='',ahtIdFromEcs='',ahtId='',ahtCtnId='',ahtStatus='',ahtTaskType='',init_fail_sendcount=0,locked_fail_sendcount=0,cancel_fail_sendcount=0,leavelane_fail_sendcount=0 where block="{block}"''' \
                            #     .format(block=data['block'])
                            # print(executesql_insqlite)
                            # o.executesql(executesql_insqlite)
                            # print("更新FMS&BMS交互成功")
                            # print(executesql_insqlite)
                            print("车道离开")
                            print(objt_dict)
                            if response_status == "0":  # BMS反馈成功
                                executesql_insqlite = """update fmsbms set bay='',ascId='',ascCtnId='',ascCtnType='',posOnAht='',ascStatus='',ahtIdFromEcs='',ahtId='',ahtCtnId='',ahtStatus='',ahtTaskType='',init_fail_sendcount=0,locked_fail_sendcount=0,cancel_fail_sendcount=0,leavelane_fail_sendcount=0 where block='{block}'""" \
                                    .format(block=data['block'])
                                print(executesql_insqlite)
                                o.executesql(executesql_insqlite)
                                print("更新FMS&BMS交互成功")
                                print(executesql_insqlite)
                            else:  # BMS反馈失败
                                leavelane_fail_sendcount = dataaboutinte_dict['leavelane_fail_sendcount'] + 1  # 从数据库查询出来对应的字段值+1
                                executesql_insqlite = '''update fmsbms set leavelane_fail_sendcount={leavelane_fail_sendcount} where block="{block}"''' \
                                    .format(leavelane_fail_sendcount=leavelane_fail_sendcount,block=block)
                                print("更新FMS&BMS交互失败")
                                print(executesql_insqlite)
                                o.executesql(executesql_insqlite)
                    except requests.exceptions.ConnectionError as e:
                        print("Failed to connect to the server:", e)

def postf2qUpdateIntera():#/f2b/updateIntera FMS给QCMS发送更新交互状态
    o=sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
    # dataaboutinte = o.query('''select * from fmsqcms where whethersend2qcms=0''')#查询没有发过的交互表信息
    dataaboutinte = o.query('''select * from fmsqcms''')  # 查询没有发过的交互表信息
    if len(dataaboutinte) != 0:  # 查询出来数据不为空
        for dataaboutinte_dict in dataaboutinte:#dataaboutinte=[{},{},{}]将数据库每一条拿出来判断
            ###判断要发送的BMS所在的服务器
            if dataaboutinte_dict['qcId']=='103':
                qcmshttpip=conf.get('qcmshttpip_103')
            elif dataaboutinte_dict['qcId']=='104':
                qcmshttpip=conf.get('qcmshttpip_104')
            elif dataaboutinte_dict['qcId']=='105':
                qcmshttpip=conf.get('qcmshttpip_105')
            elif dataaboutinte_dict['qcId']=='106':
                qcmshttpip=conf.get('qcmshttpip_106')
            elif dataaboutinte_dict['qcId']=='107':
                qcmshttpip=conf.get('qcmshttpip_107')
            elif dataaboutinte_dict['qcId']=='108':
                qcmshttpip=conf.get('qcmshttpip_108')
            else:
                qcmshttpip = conf.get('qcmshttpip_101')

            url = qcmshttpip + conf.get('fmsqcmsurls')[0]  # 'http://10.28.243.102:10087/f2q/updateIntera/'

            qcId = dataaboutinte_dict['qcId']
            lane = dataaboutinte_dict['lane']
            qcRef1 = dataaboutinte_dict['qcRef1']
            qcRef2 = dataaboutinte_dict['qcRef2']
            qcStatus = dataaboutinte_dict['qcStatus']
            qcV = dataaboutinte_dict['qcV']
            ahtIdFromEcs = dataaboutinte_dict['ahtIdFromEcs']
            jobPos = dataaboutinte_dict['jobPos']
            ahtId = dataaboutinte_dict['ahtId']
            moveKind = dataaboutinte_dict['moveKind']
            ahtStatus = dataaboutinte_dict['ahtStatus']
            ahtRespV = dataaboutinte_dict['ahtRespV']
            ahtUpdateTime=get_current_timese()
            data = {
            "qcId": qcId,
            "lane": lane,
            "jobPos": jobPos,#根据任务情况进行替换
            "ahtId": ahtId,#根据任务情况进行替换
            "moveKind": moveKind,#根据任务情况进行替换
            "ahtStatus": "LOCKED",#根据任务情况进行替换
            "ahtRespV": qcV,#根据任务情况进行替换
            "ahtUpdateTime":ahtUpdateTime
        }
            ahtRespV = qcV

            if qcStatus=='' and ahtStatus=='':#需要发送INIT状态
                querysql = '''select * from tosqcmstask where taskStatus in ('ENTERED','WORKING') and whethersendtask=1 and stsNo="{qcId}" and lane="{lane}" and truckType='AGV' order by taskId asc'''.format(
                    qcId=qcId, lane=lane)
                # print(querysql)
                queryresults = o.query(querysql)
                if len(queryresults) != 0:  # 查询出来数据
                    queryresultsingle = queryresults[0]
                    jobPos = data['jobPos'] = queryresultsingle['posOnTruck']
                    ahtId = data['ahtId'] = queryresultsingle['ahtId']
                    moveKind = data['moveKind'] = queryresultsingle['taskType']
                    ahtRespV = data['ahtRespV']
                    ahtStatus = data['ahtStatus'] ='INIT'
                    if int(dataaboutinte_dict['init_fail_sendcount']) <= 2:  # 异常发送次数<=2时才发送
                        try:
                            if queryresultsingle['initWhetherSend'] == 0:  # 发送INIT的条件是此条任务没有发送过INIT状态
                                currentBayInOPC = read_opcua(ip=conf.get('qcmsopcuaip'),tag='ns=1;s=QCMS_ACCS.XXX.PLC_OPC_UI.CurrentBayId'.replace('XXX',qcId))
                                if currentBayInOPC==queryresultsingle['currentBayId']:#OPCUA大车位置与tosqcmstask中一致时才发送
                                    data = json.dumps(data)
                                    headers = {'charset': 'UTF-8','signType': 'MD5','mchId': 'zpmc','equipId': encode_to_base64(dataaboutinte_dict['qcId']),'sign': encryptionMD5(data)}

                                    time.sleep(1)
                                    response = requests.post(url, data=data, headers=headers)  # 发送消息

                                    #打印当前时间
                                    print(get_current_time())
                                    # 检查响应代码
                                    print(response.status_code, data)
                                    # 读取返回的消息内容
                                    response_content = response.text  # str类型
                                    objt_dict = json.loads(response_content)  # 转化为dict类型
                                    response_status = objt_dict['status']

                                    if response.status_code == 200:  # 发送成功将csv中那行数据进行更新
                                        print("FMS-QCMS:/f2q/updateIntera/-response " + response.text)
                                        if response_status == "0":  # QCMS反馈成功
                                            executesql_insqlite = '''update fmsqcms set init_fail_sendcount=0,jobPos="{jobPos}",ahtId="{ahtId}",moveKind="{moveKind}",ahtStatus="{ahtStatus}",ahtRespV={ahtRespV},ahtUpdateTime="{ahtUpdateTime}" where qcId="{qcId}" and lane="{lane}"'''.format(
                                                jobPos=jobPos, ahtId=ahtId, moveKind=moveKind,
                                                ahtStatus=ahtStatus, ahtRespV=ahtRespV, qcId=qcId, lane=lane,ahtUpdateTime=ahtUpdateTime)
                                            o.executesql(executesql_insqlite)
                                            print("更新到数据库{0}成功".format(executesql_insqlite))

                                            executesql_insqlite = '''update tosqcmstask set initWhetherSend=1 where taskId={taskId}'''.format(
                                                taskId=queryresultsingle['taskId'])
                                            o.executesql(executesql_insqlite)

                                        elif response_status != "0":  # QCMS反馈失败
                                            init_fail_sendcount = dataaboutinte_dict['init_fail_sendcount'] + 1  # 从数据库查询出来对应的字段值+1
                                            executesql_insqlite = '''update fmsqcms set init_fail_sendcount={init_fail_sendcount} where qcId="{qcId}" and lane="{lane}"'''.format(
                                                init_fail_sendcount=init_fail_sendcount, qcId=qcId, lane=lane)
                                            o.executesql(executesql_insqlite)
                                            print("更新到数据库{0}失败".format(executesql_insqlite))
                        except requests.exceptions.ConnectionError as e:
                            print("Failed to connect to the server:", e)

            elif qcStatus=='' and ahtStatus=='INIT':#需要发送ARRIVED状态
               ahtStatus=data['ahtStatus'] ='ARRIVED'
               if int(dataaboutinte_dict['arrived_fail_sendcount']) <= 2:  # 异常发送次数<=2时才发送
                    try:
                        data = json.dumps(data)
                        headers = {
                            'charset': 'UTF-8',
                            'signType': 'MD5',
                            'mchId': 'zpmc',
                            'equipId': encode_to_base64(dataaboutinte_dict['qcId']),
                            'sign': encryptionMD5(data)

                        }
                        # time.sleep(100)
                        response = requests.post(url, data=data, headers=headers)  # 发送消息

                        # 打印当前时间
                        print(get_current_time())
                        # 检查响应代码
                        print(response.status_code, data)
                        # 读取返回的消息内容
                        response_content = response.text  # str类型
                        objt_dict = json.loads(response_content)  # 转化为dict类型
                        response_status = objt_dict['status']
                        print(objt_dict)

                        if response.status_code == 200:  # 发送成功将csv中那行数据进行更新
                            print("FMS-QCMS:/f2q/updateIntera/-response " + response.text)
                            if response_status == "0":  # QCMS反馈成功
                                executesql_insqlite = '''update fmsqcms set arrived_fail_sendcount=0,jobPos="{jobPos}",ahtId="{ahtId}",moveKind="{moveKind}",ahtStatus="{ahtStatus}",ahtRespV={ahtRespV},ahtUpdateTime="{ahtUpdateTime}" where qcId="{qcId}" and lane="{lane}"'''.format(
                                    jobPos=jobPos, ahtId=ahtId, moveKind=moveKind,
                                    ahtStatus=ahtStatus, ahtRespV=ahtRespV, qcId=qcId, lane=lane,ahtUpdateTime=ahtUpdateTime)
                                o.executesql(executesql_insqlite)
                                print("更新到数据库{0}成功".format(executesql_insqlite))

                            elif response_status != "0":  # QCMS反馈失败
                                # 发送的FMS-》qcms是ARRIVED消息且QCMS反馈失败了
                                arrived_fail_sendcount = dataaboutinte_dict['arrived_fail_sendcount'] + 1  # 从数据库查询出来对应的字段值+1
                                executesql_insqlite = '''update fmsqcms set arrived_fail_sendcount={arrived_fail_sendcount} where qcId="{qcId}" and lane="{lane}"'''.format(
                                    arrived_fail_sendcount=arrived_fail_sendcount, qcId=qcId, lane=lane)
                                o.executesql(executesql_insqlite)
                                print("更新到数据库{0}失败".format(executesql_insqlite))
                    except requests.exceptions.ConnectionError as e:
                        print("Failed to connect to the server:", e)
            elif qcStatus=='REQ_LOCK' and ahtStatus=='ARRIVED':#需要发送LOCKED状态


                ahtStatus = data['ahtStatus'] = 'LOCKED'
                if int(dataaboutinte_dict['locked_fail_sendcount']) <= 2:  # 异常发送次数<=2时才发送
                    try:
                        data = json.dumps(data)
                        headers = {
                            'charset': 'UTF-8',
                            'signType': 'MD5',
                            'mchId': 'zpmc',
                            'equipId': encode_to_base64(dataaboutinte_dict['qcId']),
                            'sign': encryptionMD5(data)

                        }
                        time.sleep(1)
                        response = requests.post(url, data=data, headers=headers)  # 发送消息

                        # 打印当前时间
                        print(get_current_time())
                        # 检查响应代码
                        print(response.status_code, data)
                        # 读取返回的消息内容
                        response_content = response.text  # str类型
                        objt_dict = json.loads(response_content)  # 转化为dict类型
                        response_status = objt_dict['status']


                        if response.status_code == 200:  # 发送成功将csv中那行数据进行更新
                            print("FMS-QCMS:/f2q/updateIntera/-response " + response.text)
                            if response_status == "0":  # QCMS反馈成功
                                executesql_insqlite = '''update fmsqcms set locked_fail_sendcount=0,jobPos="{jobPos}",ahtId="{ahtId}",moveKind="{moveKind}",ahtStatus="{ahtStatus}",ahtRespV={ahtRespV},ahtUpdateTime="{ahtUpdateTime}" where qcId="{qcId}" and lane="{lane}"'''.format(
                                    jobPos=jobPos, ahtId=ahtId, moveKind=moveKind,
                                    ahtStatus=ahtStatus, ahtRespV=ahtRespV, qcId=qcId, lane=lane,ahtUpdateTime=ahtUpdateTime)
                                o.executesql(executesql_insqlite)
                                print("更新到数据库{0}成功".format(executesql_insqlite))

                            elif response_status != "0":  # QCMS反馈失败
                                locked_fail_sendcount = dataaboutinte_dict['locked_fail_sendcount'] + 1  # 从数据库查询出来对应的字段值+1
                                executesql_insqlite = '''update fmsqcms set locked_fail_sendcount={locked_fail_sendcount} where qcId="{qcId}" and lane="{lane}"'''.format(
                                    locked_fail_sendcount=locked_fail_sendcount, qcId=qcId, lane=lane)
                                o.executesql(executesql_insqlite)
                    except requests.exceptions.ConnectionError as e:
                        print("Failed to connect to the server:", e)
            # elif (qcStatus=='COMPLETE' or qcStatus=='RELEASE')and ahtStatus=='LOCKED':#需要发送离开车道状态
            elif qcStatus == 'COMPLETE' and ahtStatus == 'LOCKED':  # 需要发送离开车道状态

                if int(dataaboutinte_dict['leavelane_fail_sendcount']) <= 2:  # 异常发送次数<=2时才发送
                    try:
                        data = {"qcId": qcId, "lane": lane, "jobPos": None, "ahtId": None, "moveKind": None,
                                "ahtStatus": None, "AhtRespV": 0, "ahtUpdateTime": get_current_timese()}
                        data = json.dumps(data)

                        headers = {
                            'charset': 'UTF-8',
                            'signType': 'MD5',
                            'mchId': 'zpmc',
                            'equipId': encode_to_base64(dataaboutinte_dict['qcId']),
                            'sign': encryptionMD5(data)
                        }
                        time.sleep(1)
                        response = requests.post(url, data=data, headers=headers)  # 发送消息
                        # 打印当前时间
                        print(get_current_time())
                        # 检查响应代码
                        print(response.status_code, data)
                        # 读取返回的消息内容
                        response_content = response.text  # str类型
                        objt_dict = json.loads(response_content)  # 转化为dict类型
                        response_status = objt_dict['status']

                        if response.status_code == 200:  # 发送成功将csv中那行数据进行更新
                            print("FMS-QCMS:/f2q/updateIntera/-response " + response.text)
                            if response_status == "0":  # QCMS反馈车道离开成功
                                executesql_insqlite = '''update fmsqcms set qcRef1='',qcRef1='',qcStatus="",qcV=0,ahtIdFromEcs="",jobPos='',qcUpdateTime='',ahtId='',moveKind='',ahtStatus='',ahtRespV=0,ahtUpdateTime='',
                                                                    init_fail_sendcount=0,arrived_fail_sendcount=0,locked_fail_sendcount=0,cancel_fail_sendcount=0,leavelane_fail_sendcount=0
                                                                    where qcId="{qcId}" and lane="{lane}"'''.format(
                                    qcId=qcId, lane=lane)
                                o.executesql(executesql_insqlite)
                                print("更新到数据库{0}成功".format(executesql_insqlite))

                            elif response_status != "0":  # QCMS反馈失败# 发送的FMS-》qcms是离开车道反馈失败了
                                leavelane_fail_sendcount = dataaboutinte_dict['leavelane_fail_sendcount'] + 1  # 从数据库查询出来对应的字段值+1
                                executesql_insqlite = '''update fmsqcms set leavelane_fail_sendcount={leavelane_fail_sendcount} where qcId="{qcId}" and lane="{lane}"'''.format(
                                    leavelane_fail_sendcount=leavelane_fail_sendcount, qcId=qcId, lane=lane)
                                o.executesql(executesql_insqlite)
                                print("更新到数据库{0}失败".format(executesql_insqlite))
                    except requests.exceptions.ConnectionError as e:
                        print("Failed to connect to the server:", e)
            else:
                pass



























































