import func
import time

# # # # # # #
# # ascIds = ['201','202','203','204','205','206','207','208','209','210','211','212','213','214','215']
# # for ascId in ascIds:
# func.generateBmsTask(ascId='204',orderType='LOAD',bay='031',equipType='AGV',cntrSize='20',count=1,writeType='w')
#ascId='201'  #orderType='DSCH' orderType='LOAD'  orderType='RECV'  orderType='DLVR' orderType='SHFI' orderType='SHFO'  orderType='PREP'
#bay='046'必须三位 equipType='AGV'  equipType='TRUCK'  cntrSize='20'
# time.sleep(0.5)
# #数据库文件路径
# func.insertdatatodb()#此语句将当前目录下./tosbmstask.csv文件数据导入本地Sqlite数据库


func.generateQcmsTask(stsNo='103',taskType='DSCH',ctnType='twin20',truckType='AGV',bay='22',count=1,writeType='w')
# #taskType='LOAD'/taskType='DSCH';# ctnType='20'/ctnType='40'/ctnType='45'/ctnType='twin20'#truckType='AGV'/truckType='TRUCK'#bay='10'#count=1
func.insertQcmsdatatodb()#此语句将当前目录下./tosqcmstask.csv文件数据导入本地Sqlite数据库


