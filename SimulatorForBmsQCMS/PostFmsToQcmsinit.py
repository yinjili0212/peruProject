import requests
import yaml
import hashlib
import json

import func
import datetime
env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)
url = conf.get('qcmshttpip')+'/f2q/updateIntera/'
# url = 'http://10.128.231.132:10087/f2q/updateIntera/'
##交互
data = {
    "qcId": "103",
    "lane": "3",
    "jobPos": "H",  # 根据任务情况进行替换
    "ahtId": "V0000",  # 根据任务情况进行替换
    "moveKind": "DSCH",  # 根据任务情况进行替换
    "ahtStatus": "LOCKED",  # 根据任务情况进行替换 ARRIVED LOCKED
    "ahtRespV": 1,  # 根据任务情况进行替换 0 1
    "ahtUpdateTime": func.get_current_timese()
}


# data = {"qcId":"103","lane":"3","jobPos":None,"ahtId":None,"moveKind":None,"ahtStatus":None,"AhtRespV":0,"ahtUpdateTime":func.get_current_timese()}

data = json.dumps(data)
headers = {
    'charset': 'UTF-8',
    'signType': 'MD5',
    'mchId': 'zpmc',
    'equipId': func.encode_to_base64('103'),
    'sign': func.encryptionMD5(data)

}
# print(data)
response = requests.post(url, data=data,headers=headers)



# 检查响应
print(response.status_code)
print(response.text)
