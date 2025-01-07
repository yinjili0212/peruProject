import requests
import yaml
import hashlib
import json

import func
import sqliteHandle
import datetime

env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)
# url = 'http://10.28.243.103:8003/bms/job/update/container/confirm'
url = conf.get('bmshttpip')+'/bms/job/abort'
###Abort
data = {
    "args": {
        "equip_id": "202",
        "job_id": "84104553",
        "reason_code": "101",
        "reason_description": "集卡没来"
    },
    "operator_id": "ROS1",
    "time_stamp": "2024-07-18 17:21:36 000"
}

headers = {
                                'Content-Type': 'application/json; charset=utf-8',
                                'equip_id': func.encode_to_base64("202")}
response = requests.post(url, json=data,headers=headers)

# 检查响应
print(response.status_code)
print(response.text)


# ####禁行区
# url = conf.get('bmshttpip')+'/bms/equip/update/forbidden-range'
# ###Abort
# data = {
#     "args": {
#         "block_no": "A3",
#         "operate_flag": 1,
#         "start_bay": "010",
#         "end_bay": "045"
#     },
#     "operator_id": "ROS1",
#     "time_stamp": "2024-07-18 17:21:36 000"
# }
#
# headers = {
#                                 'Content-Type': 'application/json; charset=utf-8',
#                                 'equip_id': func.encode_to_base64("A3")}
# response = requests.post(url, json=data,headers=headers)
#
# # 检查响应
# print(response.status_code)
# print(response.text)

