import requests
import yaml
import hashlib
import json
import func
import sqliteHandle
import datetime
env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)
url = conf.get('bmshttpip')+'/api/stand/area/info'
data = {
    "areaNo": "205",
    "banBays": [
        {
            "minBay": "002",
            "maxBay": "020"
        },
        {
            "minBay": "024",
            "maxBay": "045"
        }
    ]
}

headers = {'charset': 'UTF-8','signType': 'MD5','mchId': 'zpmc','equipId': func.encode_to_base64('201'),'sign': func.encryptionMD5(data)}
response = requests.post(url, json=data,headers=headers)
print(data)
# 检查响应
reponseCode = response.status_code
print(reponseCode)
#返回信息
responsemsg=json.loads(response.text)#{'status': '0', 'data': None}
print(responsemsg)






