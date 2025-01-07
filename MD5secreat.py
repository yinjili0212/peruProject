# import hashlib
#
#
# def md5_encrypt(input_string):
#     md5_object = hashlib.md5(input_string.encode())
#     md5_hash = md5_object.hexdigest()
#     return md5_hash
#
#
# # 使用示例,MD5加密
# input_string = '''201'''
# print(md5_encrypt(input_string))


# import psutil
# import os
#
#
# def kill_process_by_name(process_name):
#     for proc in psutil.process_iter(['name']):
#         try:
#             # 检查进程名是否包含给定的字符串
#             if process_name.lower() in proc.info['name'].lower():
#                 print(f"Killing {proc.info['name']} (PID: {proc.pid})")
#                 proc.kill()  # 杀死进程
#         except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
#             pass
#
#         # 调用函数来杀死名为 process.exe 的进程
#
#
# kill_process_by_name("chrome.exe")


import base64

# 原始数据（可以是字节串或字符串）
original_data = b"Hello, World!"

# Base64 编码
encoded_data = base64.b64encode(original_data)
print(encoded_data)  # 输出: b'SGVsbG8sIFdvcmxkIQ=='

# Base64 解码
decoded_data = base64.b64decode(encoded_data)
print(decoded_data)  # 输出: b'Hello, World!'

