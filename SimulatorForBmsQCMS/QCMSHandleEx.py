import requests
import yaml
import hashlib
import json

import func
import sqliteHandle
import datetime
env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)
url = 'http://10.128.231.136:5005/qcms/mi-handle'
# url = conf.get('qcmshttpip')+'/qcms/mi-handle'
###Abort
data = {
  "operator_id": "FrameUI:XXXX",
  "args": {
    "exception_record_id": 183,
    # "exception_handle_id": 0,
    "exception_handle_body": {}
  },
  "time_stamp": func.get_current_timese()
}
data = json.dumps(data)
headers = {
                                'charset': 'UTF-8',
                                'mchId': 'zpmc',
                                'equip_id': func.encode_to_base64("104")}

response = requests.post(url, json=data,headers=headers)



# 检查响应
print(response.status_code)
print(response.text)

