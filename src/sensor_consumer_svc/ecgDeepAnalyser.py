
from concurrent import futures
import time
import math
import logging

import grpc

#from pyDeepAnalysis_proto import pyGoDeepAnalysis_pb2
#from pyDeepAnalysis_proto import pyGoDeepAnalysis_pb2_grpc
from proto_out import pyGoDeepAnalysis_pb2
from proto_out import pyGoDeepAnalysis_pb2_grpc


import numpy as np

import pathlib
import numpy as np
import math
from scipy.signal import butter, lfilter, freqz
import json
import scipy

import pyhrv.tools as tools
import biosppy
import redis

import pyhrv.time_domain as td
from alertaclient.api import Client

from google.protobuf.json_format import MessageToJson
from connection_pool import ConnectionPool
from influxdb_client import InfluxDBClient


#========================================================#
################ LP FILTER ##############
def butter_lowpass(cutoff, fs, order=5):
    nyq = 0.5 * fs
    normal_cutoff = cutoff / nyq
    b, a = butter(order, normal_cutoff, btype='low', analog=False)
    return b, a


def butter_lowpass_filter(data, cutoff, fs, order=5):
    b, a = butter_lowpass(cutoff, fs, order=order)
    y = lfilter(b, a, data)
    return y



def butter_bandpass_filter(cutoff, fs, order=5):
    nyq = 0.8 * fs
    normal_cutoff = cutoff / nyq
    low_cutoff = 0.5 / nyq

    b, a = butter(order, [low_cutoff, normal_cutoff ], btype='band')
    return b, a


def lp_filter(sig):
    # Filter requirements.
    order = 6
    fs = 128.0       # sample rate, Hz
    cutoff = 50.667  # desired cutoff frequency of the filter, Hz
    # Get the filter coefficients so we can check its frequency response.
    b, a = butter_lowpass(cutoff, fs, order)
    return butter_lowpass_filter(sig, cutoff, fs, order)
# FILTER
#========================================================#


def read_ecg(file_name):
    return genfromtxt(file_name, delimiter=',')


def lgth_transform(ecg, ws):
    lgth = ecg.shape[0]
    sqr_diff = np.zeros(lgth)
    diff = np.zeros(lgth)
    ecg = np.pad(ecg, ws, 'edge')
    for i in range(lgth):
        temp = ecg[i:i+ws+ws+1]
        left = temp[ws]-temp[0]
        right = temp[ws]-temp[-1]
        diff[i] = min(left, right)
        diff[diff < 0] = 0
    # sqr_diff=np.multiply(diff, diff)
    # diff=ecg[:-1]-ecg[1:]
    # sqr_diff[:-1]=np.multiply(diff, diff)
    # sqr_diff[-1]=sqr_diff[-2]
    return np.multiply(diff, diff)


def integrate(ecg, ws):
    lgth = ecg.shape[0]
    integrate_ecg = np.zeros(lgth)
    ecg = np.pad(ecg, math.ceil(ws/2), mode='symmetric')
    for i in range(lgth):
        integrate_ecg[i] = np.sum(ecg[i:i+ws])/ws
    return integrate_ecg


def find_peak(data, ws):
    lgth = data.shape[0]
    true_peaks = list()
    for i in range(lgth-ws+1):
        temp = data[i:i+ws]
        if np.var(temp) < 5:
            continue
        index = int((ws-1)/2)
        peak = True
        for j in range(index):
            if temp[index-j] <= temp[index-j-1] or temp[index+j] <= temp[index+j+1]:
                peak = False
                break

        if peak is True:
            true_peaks.append(int(i+(ws-1)/2))
    return np.asarray(true_peaks)


def find_R_peaks(ecg, peaks, ws):
    num_peak = peaks.shape[0]
    R_peaks = list()
    for index in range(num_peak):
        i = peaks[index]
        if i-2*ws > 0 and i < ecg.shape[0]:
            temp_ecg = ecg[i-2*ws:i]
            R_peaks.append(int(np.argmax(temp_ecg)+i-2*ws))
    return np.asarray(R_peaks)


def find_S_point(ecg, R_peaks):
    num_peak = R_peaks.shape[0]
    S_point = list()
    for index in range(num_peak):
        i = R_peaks[index]
        cnt = i
        if cnt+1 >= ecg.shape[0]:
            break
        while ecg[cnt] > ecg[cnt+1]:
            cnt += 1
            if cnt >= ecg.shape[0]:
                break
        S_point.append(cnt)
    return np.asarray(S_point)


def find_Q_point(ecg, R_peaks):
    num_peak = R_peaks.shape[0]
    Q_point = list()
    for index in range(num_peak):
        i = R_peaks[index]
        cnt = i
        if cnt-1 < 0:
            break
        while ecg[cnt] > ecg[cnt-1]:
            cnt -= 1
            if cnt < 0:
                break
        Q_point.append(cnt)
    return np.asarray(Q_point)


def EKG_QRS_detect(ecg, fs, QS, plot=False):
    sig_lgth = len(ecg)
    ecg = ecg-np.mean(ecg)
    ecg_lgth_transform = lgth_transform(ecg, int(fs/20))
    # ecg_lgth_transform=lgth_transform(ecg_lgth_transform, int(fs/40))

    ws = int(fs/8)
    ecg_integrate = integrate(ecg_lgth_transform, ws)/ws
    ws = int(fs/6)
    ecg_integrate = integrate(ecg_integrate, ws)
    ws = int(fs/36)
    ecg_integrate = integrate(ecg_integrate, ws)
    ws = int(fs/72)
    ecg_integrate = integrate(ecg_integrate, ws)

    peaks = find_peak(ecg_integrate, int(fs/10))
    R_peaks = find_R_peaks(ecg, peaks, int(fs/40))
    if QS:
        S_point = find_S_point(ecg, R_peaks)
        Q_point = find_Q_point(ecg, R_peaks)
    else:
        S_point = None
        Q_point = None
    if plot:
        index = np.arange(sig_lgth)/fs
        fig, ax = plt.subplots()
        ax.plot(index, ecg, 'b', label='EKG')
        ax.plot(R_peaks/fs, ecg[R_peaks], 'ro', label='R peaks')
        if QS:
            ax.plot(S_point/fs, ecg[S_point], 'go', label='S')
            ax.plot(Q_point/fs, ecg[Q_point], 'yo', label='Q')
        ax.set_xlim([0, sig_lgth/fs])
        ax.set_xlabel('Time [sec]')
        ax.legend()
        # ax[1].plot(ecg_integrate)
        # ax[1].set_xlim([0, ecg_integrate.shape[0]])
        # ax[2].plot(ecg_lgth_transform)
        # ax[2].set_xlim([0, ecg_lgth_transform.shape[0]])
        plt.show()
    return R_peaks, S_point, Q_point


def Ecg_RR_Detector(ecg):
    ecg_filtered = lp_filter(ecg)
    R_peaks, S_point, Q_point = EKG_QRS_detect(ecg_filtered, 128, True, False)

    # print(R_peaks)
    # print(S_point)
    # print(Q_point)

    #print("QRS-COMPLEX", S_point-Q_point)


my_ecg_stream = []


def writeToInflux(patientUUID, ecg):
    client = InfluxDBClient(url="http://10.0.0.4:8086", token="my-token", org="")
    write_api = client.write_api()
    dic = [{"measurement": "patientUUID"+"_ecg", "tags": {"ecg": list(map(str, ecg)) }, "time": int(time.time()*1000000000)}]
    write_api.write("emr_dev/year_only", "", dic)

class MyEncoder(json.JSONEncoder):
    def default(self, o):
        if type(o) is bytes:
            return o.decode("utf-8")
        return super(MyEncoder, self).default(o)

class DeepAnalyserRpcServicer(pyGoDeepAnalysis_pb2_grpc.DeepAnalyserRpcServicer):
    """Provides methods that implement functionality of route guide server."""

    def RaiseAlert(self, in_req, context):
        

        #client = Client(endpoint='http://10.0.0.4:8999/')
        #with pool.item() as client:
        client.send_alert(resource=in_req.resource, event=in_req.event, status='open', environment='Development', severity=in_req.severity, group='ecg', value=in_req.value, service=[in_req.resource], createTime=in_req.createTime)

        rsp = pyGoDeepAnalysis_pb2.ALERT_YELLOW
        return pyGoDeepAnalysis_pb2.alertCode(alertRsp=rsp)




    def ProcessEcg(self, in_req, context):
        #my_ecg_stream = []

        start_time = time.time()

        # print(in_req.patientUUID)
        # print(in_req.ecgVal)
        #if in_req.hr < 70:
        #    print("Heart rate %d less than 70, raise Alarm", in_req.hr)
       # print("Heart rate %d ", in_req.hr)
        # print(in_req.hr)


        elapsed_time = time.time() - start_time
        rsp = pyGoDeepAnalysis_pb2.ALERT_YELLOW

        filtered = list(lfilter(b, a, in_req.ecgVal))
        
        filtered1 = [round(x*100)/100 for x in filtered]

        if in_req.motion == 0 :
            motionStr = "Motion"
        elif in_req.motion == 1:
            motionStr = "Standing"
        elif in_req.motion == 2 :
            motionStr = "Bending"
        else:
            motionStr = "Sleeping"
        

        toJsonData = {}
        toJsonData['ecg'] = filtered1
        toJsonData['time'] = in_req.currTime
        toJsonData['hr'] = in_req.hr
        toJsonData['rr'] = in_req.rr
        toJsonData['spo2'] = in_req.spo2
        toJsonData['pr'] = in_req.pr
        toJsonData['pi'] = round(in_req.pi*100)/100 
        toJsonData['temperature'] =  round(in_req.temperature*100)/100 
        toJsonData['motion'] = motionStr


        #filtered = scipy.signal.filtfilt(b,a,in_req.ecgVal)
        redis_conn = redis.Redis(connection_pool=redis_pool)

        #r = redis.Redis(host='10.0.0.4', port=6379, db=0)
        redis_conn.publish(in_req.patientUUID+"-ecg", str(filtered1))
        redis_conn.publish(in_req.patientUUID+"-live",json.dumps(toJsonData, cls=MyEncoder))

        #redis_conn.publish(in_req.patientUUID+"-ecg",  str(in_req.ecgVal))

        return pyGoDeepAnalysis_pb2.alertCode(alertRsp=rsp)


    def ProcessEcgOld(self, in_req, context):
        #my_ecg_stream = []

        start_time = time.time()

        # print(in_req.patientUUID)
        # print(in_req.ecgVal)
        #if in_req.hr < 70:
        #    print("Heart rate %d less than 70, raise Alarm", in_req.hr)
        # print("Heart rate %d ", in_req.hr)
        # print(in_req.hr)


        elapsed_time = time.time() - start_time
        rsp = pyGoDeepAnalysis_pb2.ALERT_YELLOW

        filtered = lfilter(b, a, in_req.ecgVal)


        """         
        Ecg_RR_Detector(in_req.ecgVal)

        fs = 128
        detectors = Detectors(fs)
        r1_peaks = detectors.two_average_detector(in_req.ecgVal)
        #print("r1_peaks ", r1_peaks)
        r2_peaks = detectors.swt_detector(in_req.ecgVal)
        #print("r2_peaks ", r2_peaks)
        r3_peaks = detectors.engzee_detector(in_req.ecgVal)
        #print("r3_peaks ", r3_peaks)
        r4_peaks = detectors.christov_detector(in_req.ecgVal)
        #print("r4_peaks ", r4_peaks)
        r5_peaks = detectors.hamilton_detector(in_req.ecgVal)
        #print("r5_peaks ", r5_peaks)
        r6_peaks = detectors.pan_tompkins_detector(in_req.ecgVal)
        #print("r6_peaks ", r6_peaks)
        """
        # my_ecg_stream.extend(in_req.ecgVal)
        # if (len(my_ecg_stream) > 1000):
        #    print("Len of myecg ", len(my_ecg_stream))
        #    signal, rpeaks = biosppy.signals.ecg.ecg(my_ecg_stream, show=False)[1:3]

        results = {}
        signal, rpeaks = biosppy.signals.ecg.ecg(in_req.ecgVal, sampling_rate=128 ,show=False)[1:3]

        results = td.nni_parameters(rpeaks=rpeaks)
        print("mean HR %d Min : %d   Max: %d", int(60/(results["nni_mean"]/128.0)), int(60/(results["nni_min"]/128.0)), int(60/(results["nni_max"]/128.0)))
        results = td.nni_differences_parameters(rpeaks=rpeaks)
        if results["nni_diff_max"] > 15 :
            print(in_req.ecgVal)
            print ("Raising Alarm nni_diff_max >15", results["nni_diff_max"])
            #print(in_req.ecgVal)
        #print(results)
        #results = td.rmssd(rpeaks=rpeaks)
        #print(results)



        #results['sdnn'] = td.sdnn(in_req.ecgVal)
        #results['rmssd'] = td.rmssd(in_req.ecgVal)
        # Access SDNN value using the key 'sdnn'
        # print(results['sdnn'])
        # print(results['rmssd'])

        #results['sdnn'] = td.sdnn(my_ecg_stream)
        #results['rmssd'] = td.rmssd(my_ecg_stream)
        # Access SDNN value using the key 'sdnn'
        #print("Avg", results['sdnn'])
        # print("Avg",results['rmssd'])

        return pyGoDeepAnalysis_pb2.alertCode(alertRsp=rsp)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pyGoDeepAnalysis_pb2_grpc.add_DeepAnalyserRpcServicer_to_server(
        DeepAnalyserRpcServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

def create_alerta_client():
    print("creating alerta client")
    return Client(endpoint='http://alertaApp1:8999/')

if __name__ == '__main__':
    logging.basicConfig()
    
    order = 6
    fs = 128.0       # sample rate, Hz
    cutoff = 40#50.667  # desired cutoff frequency of the filter, Hz
    global b
    global a
    #b, a = butter_lowpass(cutoff, fs, order)
    b, a =  butter_bandpass_filter(cutoff, fs, order)

    global pool
    pool = ConnectionPool(create=create_alerta_client,
                      max_size=20, max_usage=1000, idle=120, ttl=240)
    print(pool)

    global client
    client = Client(endpoint='http://alertaApp1:8999/')


    global redis_pool
    redis_pool = redis.ConnectionPool(host='redisSvr1', port=6379, db=0,password="sOmE_sEcUrE_pAsS" )


    serve()
