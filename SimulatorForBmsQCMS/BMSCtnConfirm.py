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
url = conf.get('bmshttpip')+'/bms/job/update/container/confirm'
###Abort
data = {
    "args": {
        "equip_id": "204",
        "operator_id": "ROS01",
        "job_id": "164919149",
"confirm_flag": 1,
"container_id": "V000",
"container_iso": "V000"
    },
    "time_stamp": "2024-08-26 17:21:36 000"
}
# headers = {
#                                 'charset': 'UTF-8',
#                                 'signType': 'MD5',
#                                 'mchId': 'zpmc',
#                                 'equipId': "201",
#                                 'sign': func.encryptionMD5(json.dumps(data))}
headers = {
                                'Content-Type': 'application/json; charset=utf-8',
                                'mchId': 'zpmc',
                                'equipId': "201"}
response = requests.post(url, json=data,headers=headers)



# 检查响应
print(response.status_code)
print(response.text)
#
# responseMsg = json.loads(response.text)#BMS返回的消息转为dict格式
# if responseMsg['status']=="0":#表示BMS反馈成功，则需要将tosbmstask数据库此条信息对应的status=CANCEL
#     o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
#     updatesql = '''update tosbmstask set status="CANCEL" where aorId={0}'''.format(data['aorId'])  # 更新任务状态
#     o.executesql(updatesql)
