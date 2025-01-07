import requests
import yaml
import hashlib
import json

import func
import sqliteHandle
import datetime
env_name = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get('current_env')
conf = yaml.full_load(open('./config.yml', 'r', encoding='utf-8').read()).get(env_name)
url = conf.get('bmshttpip')+'/bms/exception/handle'
# # # ###abort处理
# data ={
#     "time_stamp": '2024-07-18 17:21:36 000',
#     "operator_id": "GUI01",
#     "args": {
#         "exception_record_id": 241,
#         "exception_handle_id": 50101,
#         "exception_handle_body": {
#             "equip_id": "201",
#             "job_id": "144722680",
#             "reason_code": "101",
#             "reason_description": "集卡没来"
#         }
#     }
# }
###确认箱号
data ={
    "time_stamp": '2024-07-18 17:21:36 000',
    "operator_id": "GUI01",
    "args": {
        "exception_record_id": 466,
        "exception_handle_id": 50104,
        "exception_handle_body": {
            "equip_id": "201",
            "job_id": "160227073",
            "confirm_flag": 1,#0不一致，1一致
            "container_id": "ZPMC0000000",
            "container_iso": "20GP"
        }
    }
}

# #确认任务完成
# data ={
#     "time_stamp": '2024-07-18 17:21:36 000',
#     "operator_id": "GUI01",
#     "args": {
#         "exception_record_id": 198,
#         "exception_handle_id": "50102",
#         "exception_handle_body": {
#             "equip_id": "201",
#             "job_id": "145303438"
#         }
#     }
# }
# #确认任务完成-计划位
# data ={
#     "time_stamp": '2024-07-18 17:21:36 000',
#     "operator_id": "GUI01",
#     "args": {
#         "exception_record_id": 316,
#         "exception_handle_id": "50102",
#         "exception_handle_body": {
#             "equip_id": "203",
#             "job_id": "170619363",
#             "block_no": "C1",
#             "bay_no": "019",
#             "lane_no": "04",
#             "tier_no": "1",
#             "container_id": "ZPMC0000000",
#             "container_iso": "20GP"
#         }
#     }
# }
# #确认任务完成-非计划位
# data ={
#     "time_stamp": '2024-07-18 17:21:36 000',
#     "operator_id": "GUI01",
#     "args": {
#         "exception_record_id": 369,
#         "exception_handle_id": "50103",
#         "exception_handle_body": {
#             "equip_id": "201",
#             "job_id": "162449332",
#             "block_no": "A1",
#             "bay_no": "019",
#             "lane_no": "04",#堆场内必须给2位列数据
#             "tier_no": "1",
#             # "hts_no": "V001",
#             # "hts_load_pos": 3,#1front 2middle 3rear
#             "container_id": "ZPMC0000000",
#             "container_iso": "20GP"
#         }
#     }
# }
# #确认集卡到达
# data ={
#     "time_stamp": '2024-07-18 17:21:36 000',
#     "operator_id": "GUI01",
#     "args": {
#         "exception_record_id": 370,
#         "exception_handle_id": "50109",
#         "exception_handle_body": {
#             "equip_id": "201",
#             "job_id": "134857309",
#             "hts_id": "V001",
#             "hts_load_pos": 3,#1front 2middle 3rear
#             "hts_type": "6",#InternalTruck = 4,ExternalTruck = 5IGV=6,
#             "hts_work_lane": "41"#车辆实际作业车道 31海侧车道  41陆侧车道
#         }
#     }
# }
headers = {
                                'Content-Type': 'application/json; charset=utf-8',
                                'equip_id': func.encode_to_base64("203")}

response = requests.post(url, json=data,headers=headers)

# 检查响应
print(response.status_code)
print(response.text)

