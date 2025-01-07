#
personcount_site = 1#现场支持人员数量

personcount_remote = 2.5 #远程支持人员数量


person_site_value = 350#工作单价人日（单位：美元）
person_remote_value = 350#工作单价人日（单位：美元）


sitevalues = int(personcount_site*person_site_value*365*2)#
remotevalues = int(personcount_remote*person_remote_value*250*2)
print(f'现场支持人员数量{personcount_site}人；远程技术支持团队的人员配置设定为{personcount_remote}人，这一数量是基于对现场人员在非工作时间段仍需技术支持的考量，以及面对突发问题时需迅速派遣技术人员进行解决等多方面因素的全面评估后得出的合理配置')
# print(f'现场人员{personcount_site}人 2年内需要的人工成本{sitevalues}美元（不含税）')
# print(f'现场人员{personcount_site}人 2年内需要的人工成本{personcount_site}人*{person_site_value}美元*365天*2年={sitevalues}美元（不含税）')
# print(f'远程支持人员{personcount_remote}人 2年内需要的人工成本{remotevalues}美元（不含税）')
# print(f'远程支持人员{personcount_remote}人 2年内需要的人工成本{personcount_remote}人*{person_remote_value}美元*365天*2年={remotevalues}美元（不含税）')
# print(f'现场+远程技术人员2年内需要的人工成本{sitevalues+remotevalues}美元')
print(f'总需要的人员数量为{personcount_site+personcount_remote}人 2年内需要的人工成本{personcount_site}人*{person_site_value}美元*365天*2年+{personcount_remote}人*{person_remote_value}美元*250天(工作日)*2年={sitevalues+remotevalues}美元（不含税）')
print('以上报价不包含差旅、住宿及餐饮等附加支出')