
import wfdb
import biosppy
import pyhrv.time_domain as td
import statistics
import numpy

i = 10
while(i < 40):
    name = 'p'+ str(i)
    record = wfdb.srdsamp(name,sampfrom=0, sampto = 30000, channels=[1])
    x = record[0].tolist()
    testList=[0]
    for tmp in x:
        testList.append(tmp[0]*1000)

    print("======================")
    signal, rpeaks = biosppy.signals.ecg.ecg(testList, sampling_rate=128 ,show=False)[1:3]
    #results = pyhrv.time_domain.nni_parameters(rpeaks=rpeaks)
    #results = td.nni_differences_parameters(rpeaks=rpeaks)
    #if results["nni_diff_max"] > 15 :
    #  print("PROBLEM",results["nni_diff_max"])
    #results
    arr = numpy.array(rpeaks)
    arr2 = arr[1:len(arr)]
    arr1 = arr[0:len(arr)-1]
    delta = arr2-arr1
    output = statistics.variance(delta)
    print("VARIANCE  ",name, output )
    


    name = 'p'+ str(i)+"c"
    record = wfdb.srdsamp(name,sampfrom=0, sampto = 30000, channels=[1])
    x = record[0].tolist()
    testList=[0]
    for tmp in x:
        testList.append(tmp[0]*1000)
    print("======================")

    signal, rpeaks = biosppy.signals.ecg.ecg(testList, sampling_rate=128 ,show=False)[1:3]
    #results = pyhrv.time_domain.nni_parameters(rpeaks=rpeaks)
    #results = td.nni_differences_parameters(rpeaks=rpeaks)
    #if results["nni_diff_max"] > 15 :
    #  print("PROBLEM",results["nni_diff_max"])
    #results
    arr = numpy.array(rpeaks)
    arr2 = arr[1:len(arr)]
    arr1 = arr[0:len(arr)-1]
    delta = arr2-arr1
    output = statistics.variance(delta)
    print("VARIANCE :  ",name, output )
    


    name = 'n'+ str(i)
    record = wfdb.srdsamp(name,sampfrom=0, sampto = 30000, channels=[1])
    x = record[0].tolist()
    testList=[0]
    for tmp in x:
        testList.append(tmp[0]*1000)
    print("======================")

    signal, rpeaks = biosppy.signals.ecg.ecg(testList, sampling_rate=128 ,show=False)[1:3]
    #results = pyhrv.time_domain.nni_parameters(rpeaks=rpeaks)
    #results = td.nni_differences_parameters(rpeaks=rpeaks)
    #if results["nni_diff_max"] > 15 :
    #  print("PROBLEM",results["nni_diff_max"])
    #results
    arr = numpy.array(rpeaks)
    arr2 = arr[1:len(arr)]
    arr1 = arr[0:len(arr)-1]
    delta = arr2-arr1
    output = statistics.variance(delta)
    print("VARIANCE :  ",name, output )
    


    name = 'n'+ str(i)+"c"
    record = wfdb.srdsamp(name,sampfrom=0, sampto = 30000, channels=[1])
    x = record[0].tolist()
    testList=[0]
    for tmp in x:
        testList.append(tmp[0]*1000)
    print("======================")

    signal, rpeaks = biosppy.signals.ecg.ecg(testList, sampling_rate=128 ,show=False)[1:3]
    #results = pyhrv.time_domain.nni_parameters(rpeaks=rpeaks)
    #results = td.nni_differences_parameters(rpeaks=rpeaks)
    #if results["nni_diff_max"] > 15 :
    #  print("PROBLEM",results["nni_diff_max"])
    #results
    arr = numpy.array(rpeaks)
    arr2 = arr[1:len(arr)]
    arr1 = arr[0:len(arr)-1]
    delta = arr2-arr1
    output = statistics.variance(delta)
    print("VARIANCE :  ",name, output )
    
    i = i+ 1