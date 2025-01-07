import func
import yaml
from flask import Flask, request, Response, jsonify, make_response, \
    after_this_request  # Flask是一个基于Python的轻量级Web框架，可以用于实现HTTP服务器端。下面是一个简单的使用Flask实现HTTP服务器端的示例：
import logservice
import datetime
import requests
import hashlib
import ast
import json
import csv
import sqliteHandle
#打开配置文件
env_name = yaml.full_load(open('./config.yml', 'r', encoding='UTF-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='UTF-8').read()).get(env_name)
log = logservice.logfunc(conf.get('logaddress'))



#设置http服务端
app = Flask(__name__)
# 修改默认端口号
# app.config['SERVER_NAME'] = conf.get('httpserverip')  # 建立连接
app.config['SERVER_NAME'] = '10.128.254.17:8090'  # 建立连接

@app.route('/api/stand/order/progress', methods=['POST'])#TOS模拟器处理BMS更新指令进度
def handleOrderProgress_feedbak():
    # # 1.获取并打印客户端发送的HTTP头信息
    # headers = request.headers
    # for header, value in headers.items():
    #     print(f"{header}: {value}")
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据,str类型
    print("BMS->TOS:order/progress str类型"+receive_msg)
    # 使用ast.literal_eval()将字符串转换成字典
    dict_obj = json.loads(receive_msg)
    aorId= dict_obj['aorId']#提取到的指令唯一编号
    step = dict_obj['step']  # 提取到的BMS返回的消息
    #3.设置回复的消息
    responseMessage = '''{"time_stamp": "time_stamp1","status": "200","result": 0,"data": "","message": "任务状态更新成功!","path": "api/stand/order/progress","version": "NC_V1"}'''.replace("time_stamp1",func.get_current_time())

    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)

    # 使用after_this_request装饰器来确保在响应发送后执行某些操作# #将收到的BMS消息对应的aorId更新到数据库表中
    @after_this_request
    def after_request_func(response):
        o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
        updatesql = '''update tosbmstask set bmssendstep="{0}" where aorId={1}'''.format(step, aorId)
        o.executesql(updatesql)
        return response
    return response1

@app.route('/api/stand/order/checkTruck', methods=['POST'])#TOS模拟器处理BMS换车
def handlechecktruck_feedbak():
    # # 1.获取并打印客户端发送的HTTP头信息
    # headers = request.headers
    # for header, value in headers.items():
    #     print(f"{header}: {value}")
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    print("BMS->TOS:order/checkTruck "+receive_msg)
    #3.设置回复的消息
    responseMessage = '''{"time_stamp": "time_stamp1","status": "200","result": 0,"data": {"isAble": true,"truckNo": "A332","posOnTruck": "A"},"message": "操作成功！","path": "/api/stand/order/checkTruck","version": "NC_V1"}'''.replace("time_stamp1",func.get_current_time())
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)

    return response1

@app.route('/api/stand/order/finish', methods=['POST'])#TOS模拟器处理BMS指令完成
def handleOrderFinish_feedbak():
    # # 1.获取并打印客户端发送的HTTP头信息
    # headers = request.headers
    # for header, value in headers.items():
    #     print(f"{header}: {value}")
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    print("BMS->TOS:order/finish "+receive_msg)
    # 使用ast.literal_eval()将字符串转换成字典
    # dict_obj = ast.literal_eval(receive_msg)
    dict_obj = json.loads(receive_msg)
    aorId= dict_obj['aorId']#提取到的指令唯一编号
    #3.设置回复的消息
    responseMessage = '''{"time_stamp": "time_stamp1","status": "200","result": 0,"data": "","message": "任务完成成功!","path": "/api/stand/order/finish","version": "NC_V1"}'''.replace(
        "time_stamp1", func.get_current_time())
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)

    # 使用after_this_request装饰器来确保在响应发送后执行某些操作# #将收到的BMS消息对应的aorId任务完成状态更新到数据库中
    @after_this_request
    def after_request_func(response):
        o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
        updatesql = '''update tosbmstask set status="FINISH" where aorId={0}'''.format(aorId)
        o.executesql(updatesql)
        return response

    return response1


@app.route('/api/stand/order/abort', methods=['POST'])#TOS模拟器处理BMS指令中止
def handleOrderAbort_feedbak():
    # # 1.获取并打印客户端发送的HTTP头信息
    # headers = request.headers
    # for header, value in headers.items():
    #     print(f"{header}: {value}")
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    print("BMS->TOS:/order/abort "+receive_msg)
    #将收到的消息转化为字典以提取需要的字段值
    # 使用ast.literal_eval()将字符串转换成字典
    # dict_obj = ast.literal_eval(receive_msg)
    dict_obj = json.loads(receive_msg)
    aorId= dict_obj['aorId']#提取到的指令唯一编号
    #3.设置回复的消息
    responseMessage = '''{"time_stamp": "time_stamp1","status": "200","result": 0,"data": "","message": "任务中止成功!","path": "/api/stand/order/abort","version": "NC_V1"}'''.replace(
        "time_stamp1", func.get_current_time())
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)
    # 使用after_this_request装饰器来确保在响应发送后执行某些操作# #将收到的BMS消息对应的aorId任务完成状态更新到数据库中
    @after_this_request
    def after_request_func(response):
        o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
        updatesql = '''update tosbmstask set status="ABORT" where aorId={0}'''.format(aorId)
        o.executesql(updatesql)
        return response
    return response1

@app.route('/api/stand/asc/update', methods=['POST'])#TOS模拟器处理BMS更新场桥信息
def handleAscUpdate_feedbak():
    # # 1.获取并打印客户端发送的HTTP头信息
    # headers = request.headers
    # for header, value in headers.items():
    #     print(f"{header}: {value}")
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    print("BMS->TOS:/asc/update "+receive_msg)
    #3.设置回复的消息
    responseMessage = '''{"time_stamp": "time_stamp1","status": "200","result": 0,"data": "","message": "设备状态更新成功！","path": "/api/stand/asc/update","version": "NC_V1"}'''.replace(
        "time_stamp1", func.get_current_time())
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)
    # print(responseMessage)
    return response1


@app.route('/api/stand/order/pull', methods=['POST'])#TOS模拟器处理BMS请求任务
def queryOrderPull_feedbak():
    # 数据库的连接
    o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
    # # 1.获取并打印客户端发送的HTTP头信息
    # headers = request.headers
    # for header, value in headers.items():
    #     print(f"{header}: {value}")
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    print('BMS->TOS:/order/pull '+receive_msg)
    # 使用ast.literal_eval()将字符串转换成字典
    # dict_obj = ast.literal_eval(receive_msg)
    dict_obj = json.loads(receive_msg)
    ascId= dict_obj['ascId']#提取到的轨道吊编号


    querysql='''select * from tosbmstask where (bmssendstep is NULL or bmssendstep='') and status="SENT" and ascId="{0}" order by aorId asc'''.format(ascId)
    queryresults = o.query(querysql)
    # print(queryresults)
    if len(queryresults)!=0:#查询出来的数据不为0
        if 'taskId' in queryresults[0] and queryresults[0]['taskId']:
            queryresults[0]['taskId'] = int(func.get_current_timestr())
        if 'cntrId' in queryresults[0] and queryresults[0]['cntrId']:
            # responseMessage[0]['cntrId'] = int(responseMessage[0]['cntrId'])
            queryresults[0]['cntrId'] = int(func.get_current_timestr())
        if 'cntrWeight' in queryresults[0] and queryresults[0]['cntrWeight']:
            queryresults[0]['cntrWeight'] = int(queryresults[0]['cntrWeight'])
        if 'cntrId2' in queryresults[0] and queryresults[0]['cntrId2']:
            queryresults[0]['cntrId2'] = int(queryresults[0]['cntrId2'])
        if 'cntrWeight2' in queryresults[0] and queryresults[0]['cntrWeight2']:
            queryresults[0]['cntrWeight2'] = int(queryresults[0]['cntrWeight2'])
        # 使用字典推导式重新构建字典，排除值为""的键值对
        new_dict = {k: v for k, v in queryresults[0].items() if v != ""}

        #字典转字符串
        dict_str1 = json.dumps(new_dict)
        #4.1设置回复的消息总格式,原始字符串模板，这里我们使用{data_placeholder}作为占位符
        responseMessage = '''{"time_stamp": "time_stamp1","status": "200","result": 0, "data": '''.replace("time_stamp1",func.get_current_time())+dict_str1+''',"message": "操作成功！","path": "/api/stand/order/pull","version": "NC_V1"'''+'''}'''
    else:#查询出来的数据=0
        responseMessage = '''{"time_stamp": "time_stamp1","status": "200","result": 0, "data": {}}'''.replace("time_stamp1",func.get_current_time())
    #     # # 5.创建一个响应对象
    response1 = make_response(responseMessage)
    # 6.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)
    print(responseMessage)
    print("\n")
    return response1

@app.route('/api/ocr/identify/container', methods=['POST'])#TOS模拟器处理BMS上传的箱号信息
def handleocridentidy_feedbak():
    # # 1.获取并打印客户端发送的HTTP头信息
    # headers = request.headers
    # for header, value in headers.items():
    #     print(f"{header}: {value}")
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    print('BMS->TOS:/api/ocr/identify/container '+receive_msg)
    #3.设置回复的消息
    responseMessage = '''{"time_stamp": "time_stamp1","status": "200","result": 0,"message": "接收成功！","path": "/api/ocr/identify","version": "NC_V1"}'''.replace("time_stamp1", func.get_current_time())
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)#MD5签名结果
    # print(responseMessage)
    return response1

@app.route('/WellOcean/GetTruckNum', methods=['POST'])#OCR模拟器处理BMS申请车道编号
def handlegettrucknum_feedbak():
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    print('BMS->OCR:GetTruckNum '+receive_msg)
    # 使用json.loads()将字符串转换成字典
    dict_obj = json.loads(receive_msg)
    rmg= dict_obj['rmg']#提取到的轨道调编号
    side=dict_obj['side']#提取到的轨道调编号

    #3.设置回复的消息
    responseMessage = '''{"code": "0","data": "success"}'''
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)#MD5签名结果
    # 使用after_this_request装饰器来确保在响应发送后执行某些操作# #将收到的BMS消息对应的aorId任务完成状态更新到数据库中
    @after_this_request
    def after_request_func(response):
        o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
        updatesql = '''update ocrbms set whethersendtruck="0",truckside="{0}" where rmg={1}'''.format(side,rmg)
        o.executesql(updatesql)
        return response

    return response1

@app.route('/WellOcean/GetCtnNum', methods=['POST'])#OCR模拟器处理BMS申请箱号编号
def handlegetctnnum_feedbak():
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    # print('BMS->OCR:GetCtnNum '+receive_msg)
    # 使用json.loads()将字符串转换成字典
    dict_obj = json.loads(receive_msg)
    rmg= dict_obj['rmg']#提取到的轨道调编号
    side=dict_obj['side']#提取到的轨道调编号
    # #收到的消息转化为字典，提取消息中关键字段
    # dict_obj = ast.literal_eval(receive_msg)
    # rmg = dict_obj['rmg']
    # side = dict_obj['side']
    #3.设置回复的消息
    responseMessage = '''{"code": "0","data": "success"}'''
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)#MD5签名结果
    # 使用after_this_request装饰器来确保在响应发送后执行某些操作# #将收到的BMS消息对应的aorId任务完成状态更新到数据库中
    @after_this_request
    def after_request_func(response):
        o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
        updatesql = '''update ocrbms set whethersendctnno="0",ctnnoside="{0}" where rmg={1}'''.format(side,rmg)
        o.executesql(updatesql)
        return response

    return response1

@app.route('/WellOcean/GetCtnDoorStatus', methods=['POST'])#OCR模拟器处理BMS申请箱门朝向
def handlegetctndoor_feedbak():
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    print("BMS->OCR:/GetCtnDoorStatus "+receive_msg)
    #提取收到消息关键信息
    dict_obj = json.loads(receive_msg)
    rmg = dict_obj['rmg']
    side = dict_obj['side']
    #3.设置回复的消息
    responseMessage = '''{"code": "0","data": "success"}'''
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)#MD5签名结果
    # 使用after_this_request装饰器来确保在响应发送后执行某些操作# #将收到的BMS消息对应的aorId任务完成状态更新到数据库中
    @after_this_request
    def after_request_func(response):
        o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
        updatesql = '''update ocrbms set whethersendctndoor="0",ctndoorside="{0}" where rmg={1}'''.format(side,rmg)
        o.executesql(updatesql)
        return response
    return response1

@app.route('/b2f/updateIntera', methods=['POST'])#FMS模拟器处理BMS申请交互
def handleupdateintera_feedbak():
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    print('BMS->FMS:/b2f/updateIntera '+receive_msg)
    #3.设置回复的消息
    responseMessage = '''{"responseTime": "responseTime1","status": "0","data": {}}'''.replace("responseTime1",func.get_current_timese())
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)#MD5签名结果

    ####将收到的消息写入CSV记录BMS发送过的交互记录
    # 使用json.loads()将字符串转换成字典
    dict_obj = json.loads(receive_msg)
    block= dict_obj['block']#提取到的堆场号 A1
    bay=dict_obj['bay']#提取到的堆场bay位号 59
    ascId = dict_obj['ascId']  # 提取到此消息设备号 201
    ascCtnId = dict_obj['ascCtnId']# 提取到此消息箱号 ZPMC0000001
    ascCtnType = dict_obj['ascCtnType']# 提取到此消息箱类型 20
    posOnAht=dict_obj['posOnAht']# 提取到此消息箱类在集卡上的位置 F M A
    ascStatus = dict_obj['ascStatus']# 提取到此消息ASC发送的状态 INIT/REQ_LOCK
    ahtIdFromEcs= dict_obj['ahtIdFromEcs']# 提取到此消息TOS带的ahtid
    @after_this_request
    def after_request_func(response):
        o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
        updatesql = '''update fmsbms set bay="{0}",ascId="{1}",ascCtnId="{2}",posOnAht="{3}",ascStatus="{4}",ahtIdFromEcs="{5}",whethersend2bms=0 where block="{6}"'''.format(bay,ascId,ascCtnId,posOnAht,ascStatus,ahtIdFromEcs,block)
        print(updatesql)
        o.executesql(updatesql)
        return response
    return response1

@app.route('/f2q/updateIntera/', methods=['POST'])#FMS模拟器处理BMS申请交互---
def handleupdateintera1_feedbak():
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    #3.设置回复的消息
    responseMessage = '''{"responseTime": "responseTime1","status": "0","data": {}}'''.replace("responseTime1",func.get_current_timese())
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)#MD5签名结果

    # ####将收到的消息写入CSV记录BMS发送过的交互记录
    # # 使用json.loads()将字符串转换成字典
    # dict_obj = json.loads(receive_msg)
    # block= dict_obj['block']#提取到的堆场号 A1
    # bay=dict_obj['bay']#提取到的堆场bay位号 59
    # ascId = dict_obj['ascId']  # 提取到此消息设备号 201
    # ascCtnId = dict_obj['ascCtnId']# 提取到此消息箱号 ZPMC0000001
    # ascCtnType = dict_obj['ascCtnType']# 提取到此消息箱类型 20
    # posOnAht=dict_obj['posOnAht']# 提取到此消息箱类在集卡上的位置 F M A
    # ascStatus = dict_obj['ascStatus']# 提取到此消息ASC发送的状态 INIT/REQ_LOCK
    # ahtIdFromEcs= dict_obj['ahtIdFromEcs']# 提取到此消息TOS带的ahtid
    # @after_this_request
    # def after_request_func(response):
    #     o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
    #     updatesql = '''update fmsbms set bay="{0}",ascId="{1}",ascCtnId="{2}",posOnAht="{3}",ascStatus="{4}",ahtIdFromEcs="{5}",whethersend2bms=0 where block="{6}"'''.format(bay,ascId,ascCtnId,posOnAht,ascStatus,ahtIdFromEcs,block)
    #     print(updatesql)
    #     o.executesql(updatesql)
    #     return response
    return response1



@app.route('/sts/p2m/plcStatus/', methods=['POST'])#TOS模拟器处理QCMS机械状态上报
def handlePlcStatus_feedbak():
    # # 1.获取并打印客户端发送的HTTP头信息
    # headers = request.headers
    # for header, value in headers.items():
    #     print(f"{header}: {value}")
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    # log.info(receive_msg)
    #3.设置回复的消息
    responseMessage = '''{"status": "0","data": {}}'''
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)#MD5签名结果
    return response1

@app.route('/sts/p2m/plcTaskStatus/', methods=['POST'])#TOS模拟器处理QCMS任务上报
def handleplcTaskStatus_feedbak():
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    # 使用json.loads()将字符串转换成字典
    dict_obj = json.loads(receive_msg)
    taskId = dict_obj['taskId']#指令id
    taskStatus= dict_obj['taskStatus']#任务状态

    #3.设置回复的消息
    responseMessage = '''{"status": "0","data": {}}'''
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)#MD5签名结果

    @after_this_request
    def after_request_func(response):
        o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
        updatesql = '''update tosqcmstask set taskStatus="{taskStatus}" where taskId="{taskId}"'''.format(taskStatus=taskStatus,taskId=taskId)
        # print(updatesql)
        o.executesql(updatesql)
        return response

    return response1

@app.route('/q2f/updateIntera/', methods=['POST'])#FMS模拟器处理QCMS申请交互
def handleq2fupdateintera_feedbak():
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    print('QCMS->FMS:/q2f/updateIntera/ '+receive_msg)
    #3.设置回复的消息
    responseMessage = '''{"responseTime": "responseTime1","status": "0","data": {}}'''.replace("responseTime1",func.get_current_timese())
    # 4.创建一个响应对象，将消息设置为json格式
    response1 = make_response(responseMessage)
    # 5.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)#MD5签名结果

    ####将收到的消息写入数据库记录BMS发送过的交互记录
    # 使用json.loads()将字符串转换成字典
    dict_obj = json.loads(receive_msg)
    qcId= dict_obj['qcId']#
    lane=dict_obj['lane']#
    qcRef1 = dict_obj['qcRef1']  #
    qcRef2 = dict_obj['qcRef2']#
    qcStatus = dict_obj['qcStatus']#
    qcV=dict_obj['qcV']#
    ahtIdFromEcs= dict_obj['ahtIdFromEcs']#

    @after_this_request
    def after_request_func(response):
        o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
        updatesql = '''update fmsqcms set qcRef1="{qcRef1}",qcRef2="{qcRef2}",qcStatus="{qcStatus}",qcV={qcV},ahtIdFromEcs="{ahtIdFromEcs}",whethersend2qcms=0 where qcId="{qcId}" and lane="{lane}"'''\
            .format(qcRef1=qcRef1,qcRef2=qcRef2,qcStatus=qcStatus,qcV=qcV,ahtIdFromEcs=ahtIdFromEcs,qcId=qcId,lane=lane)
        print(updatesql)
        o.executesql(updatesql)
        return response
    return response1

@app.route('/sts/p2m/queryMsTask/', methods=['POST'])#TOS模拟器处理QCMS任务查询
def queryMsTask_feedbak():
    currenttime = datetime.datetime.now()
    #2.打印客户端发送的消息
    receive_msg = request.data.decode('UTF-8')  # 获取请求的数据
    log.info(receive_msg)
    # 使用json.loads()将字符串转换成字典
    dict_obj = json.loads(receive_msg)
    stsNo=dict_obj['stsNo']

    stsNo= dict_obj['stsNo']#提取到的岸桥编号

    o= sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
    querysql='''select * from tosqcmstask where taskStatus in ("ENTERED",'WORKING','') and stsNo="{stsNo}" order by taskId asc'''.format(stsNo=stsNo)
    queryresults = o.query(querysql)
    # print(queryresults)
    if len(queryresults)!=0:#查询出来的数据不为0
        queryresults[0]['operateTime'] = currenttime.strftime("%Y-%m-%d %H:%M:%S")
        # 使用字典推导式重新构建字典，排除值为""的键值对
        new_dict = {k: v for k, v in queryresults[0].items() if v != "" and k not in ('whethersendtask','truckType','lane','posOnTruck','ahtId')}
        #字典转字符串
        dict_str1 = json.dumps(new_dict)
        #4.1设置回复的消息总格式,原始字符串模板，这里我们使用{data_placeholder}作为占位符
        responseMessage = '''{"status": "0","data": '''+dict_str1+'''}'''
        taskId=new_dict['taskId']#为了响应成功后更新sqlite此条数据准备
    else:
        responseMessage = '''{"status": "0","data": {}}'''
        taskId=0
    #     # # 5.创建一个响应对象
    response1 = make_response(responseMessage)
    # 6.添加需要回复的自定义的HTTP头
    response1.headers['charset'] = 'UTF-8'
    response1.headers['signType'] = 'MD5'
    response1.headers['mchId'] = 'zpmc'
    response1.headers['sign'] = func.encryptionMD5(responseMessage)#MD5签名结果#MD5签名结果

    @after_this_request
    def after_request_func(response):
        o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))

        updatesql = '''update tosqcmstask set whethersendtask=1 where taskId="{taskId}"'''.format(taskId=taskId)
        # print(updatesql)
        o.executesql(updatesql)
        return response
    return response1
#####
if __name__ == '__main__':
    app.run(debug=False)






