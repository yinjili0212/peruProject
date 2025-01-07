import func
import time
ascIdname='201'
heartbeat=func.read_opcua(ip='opc.tcp://10.28.243.103:5600/',tag='ns=1;s=BMS_ACCS.{ascIdname}.OPC_PLC.HeartBeat'.format(ascIdname=ascIdname))
for heartbeatid in range(256):
    # print(i)
    func.write_opcua(ip='opc.tcp://10.28.243.103:5600/',tag='ns=1;s=BMS_ACCS.{ascIdname}.PLC_OPC.HeartBeat'.format(ascIdname=ascIdname),value=heartbeatid)
    time.sleep(0.7)