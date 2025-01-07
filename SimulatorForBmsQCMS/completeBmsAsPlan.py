import requests
import yaml
import hashlib
import json

import func
import sqliteHandle
import datetime
env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)
url = conf.get('bmshttpip')+'/bms/job/update/complete-exception/planed'
print(url)

data = {
    "args": {
        "equip_id": "201",
        "job_id": "162732613",
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
# responseMsg = json.loads(response.text)#BMS返回的消息转为dict格式
# if responseMsg['status']=="0":#表示BMS反馈成功，则需要将tosbmstask数据库此条信息对应的status=CANCEL
#     o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
#     updatesql = '''update tosbmstask set status="CANCEL" where aorId={0}'''.format(data['aorId'])  # 更新任务状态
#     o.executesql(updatesql)
