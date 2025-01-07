import requests
import yaml
import hashlib
import json

import func
import sqliteHandle
import datetime
env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)
url = conf.get('bmshttpip')+'/api/stand/order/cancel'


###CANCEL
data = {
    "aorId": 95416717
}

headers = {
                                'charset': 'UTF-8',
                                'signType': 'MD5',
                                'mchId': 'zpmc',
                                'equipId': func.encode_to_base64("B1"),
                                'sign': func.encryptionMD5(json.dumps(data))}
response = requests.post(url, json=data,headers=headers)



# 检查响应
print(response.status_code)
print(response.text)

responseMsg = json.loads(response.text)#BMS返回的消息转为dict格式
if responseMsg['status']=="0":#表示BMS反馈成功，则需要将tosbmstask数据库此条信息对应的status=CANCEL
    o = sqliteHandle.sqliteHandler(conf.get('localSqllitedb'))
    updatesql = '''update tosbmstask set status="CANCEL" where aorId={0}'''.format(data['aorId'])  # 更新任务状态
    o.executesql(updatesql)
