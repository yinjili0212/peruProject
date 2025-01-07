import requests
import yaml
import hashlib
import json
import func
import sqliteHandle
import datetime
env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)
url = conf.get('qcmshttpip')+'/sts/m2p/cancelTask/'
#TOS发送取消任务
taskId = 20241118165907837
data = {
    "taskId": taskId,
    "taskVersion": 123,
    "stsNo": "103",
    "operateTime": func.get_current_time()
}
# data = json.dumps(data)
headers = {'charset': 'UTF-8','signType': 'MD5','mchId': 'zpmc','equipId': func.encode_to_base64('103'),'sign': func.encryptionMD5(data)}
response = requests.post(url, json=data,headers=headers)

# 检查响应
reponseCode = response.status_code
print(reponseCode)
#返回信息
responsemsg=json.loads(response.text)#{'status': '0', 'data': None}
print(responsemsg)

if reponseCode==200 and responsemsg['status']=='0':#QCMS响应成功，则把对应的任务更新成CANCEl状态
    o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
    updatesql = '''update tosqcmstask set taskStatus="{taskStatus}" where taskId="{taskId}"'''.format(taskId=taskId, taskStatus="CANCELED")
    # print(updatesql)
    o.executesql(updatesql)




