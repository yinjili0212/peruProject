import func
while True:
    func.postf2bUpdateInterajson()#FMS更新BMS的交互
    func.postOcrTruckMsg()#OCR模拟器发送OCR车号
    func.postOcrCtnMsg()#OCR模拟器发送OCR箱号
    func.postf2qUpdateIntera()#FMS响应岸桥交互QCMS


