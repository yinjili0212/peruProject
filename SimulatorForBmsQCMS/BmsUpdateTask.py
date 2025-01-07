import requests
import yaml
import hashlib
import json
import func
import sqliteHandle
import datetime
env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)
url = conf.get('bmshttpip')+'/api/stand/order/event'
#TOS更新任务
aorId = 161700664
o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
querysql = "select * from tosbmstask where aorId={aorId}".format(aorId=aorId)
queryresults=o.query(querysql)
if len(queryresults)!=0:#能查询出来数据
    print("查询出来数据")

    data = {
        "eventCode": "ASC_TRUCK_CHANGE",#ASC_UPDATE_ORIG:起始位置变化;ASC_UPDATE_DEST:目的位置变化;ASC_TRUCK_CHANGE:集卡变化
        "order":{
            "aorId": aorId,
            "taskId": queryresults[0]['taskId'],
            "ascId": queryresults[0]['ascId'],
            "orderType": queryresults[0]['orderType'],
            "status": queryresults[0]['status'],
            "operationProcess": queryresults[0]['operationProcess'],
            "prePickUpFlag": queryresults[0]['prePickUpFlag'],
            "cntrId": queryresults[0]['cntrId'],
            "cntrNo": queryresults[0]['cntrNo'],
            "cntrWeight": queryresults[0]['cntrWeight'],
            "cntrHeight": queryresults[0]['cntrHeight'],
            "cntrIso": queryresults[0]['cntrIso'],
            "cntrSize": queryresults[0]['cntrSize'],
            "cntrType": queryresults[0]['cntrType'],
            "cntrId2": 0,
            "cntrNo2": '',
            "cntrWeight2": 0,
            "cntrHeight2": '',
            "cntrIso2": '',
            "cntrSize2": '',
            "cntrType2": '',
            "origin": queryresults[0]['origin'],
            "origin2": '',
            "originType": queryresults[0]['originType'],
            "destination": queryresults[0]['destination'],
            "destination2": '',
            "destinationType": queryresults[0]['destinationType'],
            "posOnTruck": queryresults[0]['posOnTruck'],
            "posOnTruck2": '',
            "truckNo": queryresults[0]['truckNo'],
            "truckType": queryresults[0]['truckType'],
            "laneNo": queryresults[0]['laneNo'],
            "laneDirection": queryresults[0]['laneDirection']

        }

    }
    # data = json.dumps(data)
    headers = {'charset': 'UTF-8','signType': 'MD5','mchId': 'zpmc','equipId': func.encode_to_base64(queryresults[0]['ascId']),'sign': func.encryptionMD5(data)}
    response = requests.post(url, json=data,headers=headers)
    print(data)
    # 检查响应
    reponseCode = response.status_code
    print(reponseCode)
    #返回信息
    responsemsg=json.loads(response.text)#{'status': '0', 'data': None}
    print(responsemsg)






