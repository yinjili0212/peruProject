import requests
import yaml
import hashlib
import json

import func
import sqliteHandle
import datetime
env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)
url = conf.get('bmshttpip')+'/bms/equip/update/work-range'


data = {
    "args": {
        "equip_id": "202",
        "block_no": "A1",
        "start_bay": "",
        "end_bay": "",
        "start_physical_position": 1,
        "end_physical_position": 10870001
    },
    "operator_id": "ROS01",
    "time_stamp": "2024-08-26 17:21:36 000"
}

headers = {
                                'Content-Type': 'application/json; charset=utf-8',
                                'mchId': 'zpmc',
                                'equip_id': func.encode_to_base64("202")}
# print(data)
response = requests.post(url, json=data,headers=headers)
# print(func.encode_to_base64("202"))


# 检查响应
print(response.status_code)
print(response.text)
#
