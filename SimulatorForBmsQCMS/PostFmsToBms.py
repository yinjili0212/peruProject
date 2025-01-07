import requests
import yaml
import hashlib
import json

import func
import datetime
env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)
url = conf.get('bmshttpip')+'/f2b/updateIntera/'
# url = 'http://10.128.231.133:8003/f2b/updateIntera/'

# # # # ###INIT
# data = {
#                     "block": "B1",
#                     "bay": "19",
#                     "ahtId": "V001",
#                     "ahtCtnId": "ZPMC0000000",
#                     "ahtStatus": "INIT",
#                     "ahtTaskType": "DELIVER",
#                     "ahtUpdateTime": func.get_current_timese()}
# # # # ###LOCKED
# data = {
#                     "block": "B1",
#                     "bay": "19",
#                     "ahtId": "V001",
#                     "ahtCtnId": "ZPMC0000000",
#                     "ahtStatus": "LOCKED",
#                     "ahtTaskType": "DELIVER",
#                     "ahtUpdateTime": func.get_current_timese()}
# # # ###CANCEL
# data = {
#                     "block": "C1",
#                     "bay": "19",
#                     "ahtId": "V000",
#                     "ahtCtnId": "ZPMC0000000",
#                     "ahtStatus": "CANCEL",
#                     "ahtTaskType": "RECEIVE",
#                     "ahtUpdateTime": func.get_current_timese()}
# # # # ###leave
# data = {
#                     "block": "B1",
#                     "bay": "19",
#                     "ahtId": "",
#                     "ahtCtnId": "",
#                     "ahtStatus": "",
#                     "ahtTaskType": "",
#                     "ahtUpdateTime": func.get_current_timese()}

headers = {
    'charset': 'UTF-8',
    'signType': 'MD5',
    'mchId': 'zpmc',
    'equipId': func.encode_to_base64('C1'),
    'sign': func.encryptionMD5(json.dumps(data))

}
response = requests.post(url, json=data,headers=headers)



# 检查响应
print(response.status_code)
print(response.text)
