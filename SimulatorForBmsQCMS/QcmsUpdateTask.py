import requests
import yaml
import hashlib
import json
import func
import sqliteHandle
import datetime
env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)
url = conf.get('qcmshttpip')+'/sts/m2p/updateTask/'
#TOS发送取消任务
taskId = 20241118164419914
o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
querysql = "select * from tosqcmstask where taskId={taskId}".format(taskId=taskId)
queryresults=o.query(querysql)
if len(queryresults)!=0:#能查询出来数据

    data = {
        "taskId": taskId,
        "taskVersion": queryresults[0]['taskVersion'],
        "stsNo": queryresults[0]['stsNo'],
        "vbtId": queryresults[0]['vbtId'],
        "taskType": queryresults[0]['taskType'],
        "equipmentType": queryresults[0]['equipmentType'],
        "spreaderStatus": queryresults[0]['spreaderStatus'],
        "wsSpreaderSize": queryresults[0]['wsSpreaderSize'],
        "wsHbISO": queryresults[0]['wsHbISO'],
        "wsLbISO": queryresults[0]['wsLbISO'],
        "pickupPermissionFlag": queryresults[0]['pickupPermissionFlag'],
        "dropoffPermissionFlag": queryresults[0]['dropoffPermissionFlag'],
        "origWsLoc": queryresults[0]['origWsLoc'],
        "origType": queryresults[0]['origType'],
        "destWsLoc": queryresults[0]['destWsLoc'],
        "destType": queryresults[0]['destType'],
        "origHtsNoWs": queryresults[0]['origHtsNoWs'],
        "origHtsPosWs": queryresults[0]['origHtsPosWs'],
        "destHtsNoWs": queryresults[0]['destHtsNoWs'],
        "destHtsPosWs": queryresults[0]['destHtsPosWs'],
        "taskStatus": queryresults[0]['taskStatus'],
        "responseTime": func.get_current_timese()
    }
    data = json.dumps(data)
    headers = {'charset': 'UTF-8','signType': 'MD5','mchId': 'zpmc','equipId': func.encode_to_base64(queryresults[0]['stsNo']),'sign': func.encryptionMD5(data)}
    response = requests.post(url, data=data,headers=headers)

    # 检查响应
    reponseCode = response.status_code
    print(reponseCode)
    #返回信息
    responsemsg=json.loads(response.text)#{'status': '0', 'data': None}
    print(responsemsg)

    # if reponseCode==200 and responsemsg['status']=='0':#QCMS响应成功，则把对应的任务更新成CANCEl状态
    #     o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
    #     updatesql = '''update tosqcmstask set taskStatus="{taskStatus}" where taskId="{taskId}"'''.format(taskId=taskId, taskStatus="CANCELED")
    #     # print(updatesql)
    #     o.executesql(updatesql)




