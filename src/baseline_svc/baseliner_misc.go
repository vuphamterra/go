package main

import (
	"net"
	"context"
	"fmt"
	"strconv"
	"math"
	//pb "sensor_consumer_svc/proto_out/emr_consumer_proto"

	pb "baseline_svc/proto_out/emr_consumer_proto"

	"google.golang.org/grpc"
	credentials "google.golang.org/grpc/credentials"
	"os"
	"github.com/nakabonne/tstorage"
	"time"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"

)

func (bslnrCtxt *baselinerContext) startGoRoutine(f func()) {
	bslnrCtxt.wg.Add(1)
	go f()
}


func (bslnrCtxt *baselinerContext) insertMetric(storage tstorage.Storage , uuidPatient string, sensor string, currentTime int64, value float64) {
	labels := []tstorage.Label{
		{Name: "pid", Value: uuidPatient},
	}
	_ = storage.InsertRows([]tstorage.Row{
		{
			Metric: sensor,
			Labels:    labels,
			DataPoint: tstorage.DataPoint{Timestamp: currentTime, Value: value},
		},
	})
	

	value = math.Round(value*100)/100
	bslnrCtxt.log.Debugf("Inserting to influx ..time %v",time.Unix(currentTime, 0))
	//measrmnt = uuidPatient //+ "_sensors"
	writePoint := influxdb2.NewPointWithMeasurement(uuidPatient).
					//AddTag("unit", sensor).
					AddField(sensor, value).
					SetTime(time.Unix(currentTime, 0))
	err := bslnrCtxt.writeAPI.WritePoint(context.Background(), writePoint)
	if err != nil {
		bslnrCtxt.log.Errorf("Patient %s, %v",uuidPatient, err)
	}

}


func (bslnrCtxt *baselinerContext) insertMetricInflux(uuidPatient string, sensor string, currentTime int64, value float64) {
	
	value = math.Round(value*100)/100
	bslnrCtxt.log.Debugf("Inserting to influx ..time %v",time.Unix(currentTime, 0))
	//measrmnt = uuidPatient //+ "_sensors"
	writePoint := influxdb2.NewPointWithMeasurement(uuidPatient).
					//AddTag("unit", sensor).
					AddField(sensor, value).
					SetTime(time.Unix(currentTime, 0))
	err := bslnrCtxt.writeAPI.WritePoint(context.Background(), writePoint)
	if err != nil {
		bslnrCtxt.log.Errorf("Patient %s, %v",uuidPatient, err)
	}

}

func (bslnrCtxt *baselinerContext) readMetric(storage tstorage.Storage, 
											uuidPatient string, sensor string, 
											startTime int64, endTime int64, nHistory int) ([]*tstorage.DataPoint, error) {
	labels := []tstorage.Label{
		{Name: "pid", Value: uuidPatient},
	}
	points, err := storage.Select(sensor, labels, startTime, endTime)
	

	nHistoryStr := "-" + strconv.Itoa(nHistory) + "h"

	// Get query client
	queryAPI := bslnrCtxt.influxClient.QueryAPI(influxorg)

	  // Query. You need to change a bit the Query from the Query Builder
    // Otherwise it won't work
    fluxQuery := fmt.Sprintf(`from(bucket: "%s")
    |> range(start:%s)
    |> filter(fn: (r) => r["_measurement"] == "%s")
    |> yield(name: "data")`, influxdbDatabase , nHistoryStr,uuidPatient)

	bslnrCtxt.log.Debugf("Reading frm influx %v ==== %v\n %s", time.Unix(startTime, 0), time.Unix(endTime, 0), fluxQuery)

	// Get parser flux query result
	result, err := queryAPI.Query(context.Background(), fluxQuery)
	if err == nil {
		// Use Next() to iterate over query result lines
		for result.Next() {
			// Observe when there is new grouping key producing new table
			if result.TableChanged() {
				bslnrCtxt.log.Errorf("table: %s\n", result.TableMetadata().String())
			}
			// read result
			bslnrCtxt.log.Errorf("row: %s\n", result.Record().String())
		}
		if result.Err() != nil {
			bslnrCtxt.log.Errorf("Query error: %s\n", result.Err().Error())
		}
	} else {
		bslnrCtxt.log.Errorf("Reading influx err: Patient %s, %v",uuidPatient, err)
	}
		

	return points, err
}

func (bslnrCtxt *baselinerContext) tenantMapInsert (uuidTenant string) (patientMap *pmap){
	bslnrCtxt.tenantMapLock.Lock()
	defer bslnrCtxt.tenantMapLock.Unlock()

	ptmap := bslnrCtxt.tenantMapFindlocked(uuidTenant)
	if ptmap != nil {
		return ptmap
	}
	
	ptmapTmp := pmap{}
	ptmap = &ptmapTmp
	if ptmap == nil {
		bslnrCtxt.log.Errorf("Failed to allocate patient Map. Exiting")
		os.Exit(1)
	}

	ptmap.patientMap = make(map[string]*PatientSensorReadings)
	bslnrCtxt.numTenants++
	ptmap.dbHashBucket = HashBucket(uuidTenant, NUM_DB_BUCKETS)
	bslnrCtxt.tenantMap[uuidTenant] = ptmap		
	
	bslnrCtxt.startGoRoutine(func() {
		bslnrCtxt.sortewsOnTenant(uuidTenant)
	})
	
	ptmap.storage, _ = tstorage.NewStorage(
		tstorage.WithDataPath("/data/trends/"+uuidTenant),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
		tstorage.WithRetention(336*2 * time.Hour),
		tstorage.WithWALBufferedSize(-1), // WAL had freezes etc
		tstorage.WithPartitionDuration(10 * time.Minute),
		//tstorage.WithLogger(log.Println),
	)

	return ptmap
}

func (bslnrCtxt *baselinerContext) tenantMapFind (uuidTenant string) (patientMap *pmap) {
	bslnrCtxt.tenantMapLock.Lock()
	defer bslnrCtxt.tenantMapLock.Unlock()
	return bslnrCtxt.tenantMap[uuidTenant]
}


func (bslnrCtxt *baselinerContext) tenantMapFindlocked (uuidTenant string) (patientMap *pmap) {
	return bslnrCtxt.tenantMap[uuidTenant]
}

func (bslnrCtxt *baselinerContext) tenantMapDelete (uuidTenant string) {
	bslnrCtxt.tenantMapLock.Lock()
	defer bslnrCtxt.tenantMapLock.Unlock()
	//@rajsreen ---- NEED to delete all patients
	delete(bslnrCtxt.tenantMap, uuidTenant)
}


func (bslnrCtxt *baselinerContext) patientMapGetLen (ptntMap *pmap)(pLen int) {
	ptntMap.patientMapLock.Lock()
	defer ptntMap.patientMapLock.Unlock()
	return len(ptntMap.patientMap)
}

func (bslnrCtxt *baselinerContext) patientMapInsert (uuidTenant string, uuidPatient string, patientInfo *PatientSensorReadings) {
	ptntMap := bslnrCtxt.tenantMapFind(uuidTenant)
	ptntMap.patientMapLock.Lock()
	defer ptntMap.patientMapLock.Unlock()

	patientInfo.lastUpdatedLiveHr = EINVAL
	patientInfo.lastUpdatedLiveRr = EINVAL
	patientInfo.lastUpdatedLiveSpo2 = EINVAL
	patientInfo.lastUpdatedLivePr = EINVAL
	patientInfo.lastUpdatedLivePi = EINVAL
	patientInfo.lastUpdatedLiveTemperature = EINVAL
	patientInfo.lastUpdatedLiveBp = EINVAL
	patientInfo.lastUpdatedLiveWeight = EINVAL
	patientInfo.lastUpdatedLiveSugar = EINVAL

	ptntMap.patientMap[uuidPatient] = patientInfo
}

func (bslnrCtxt *baselinerContext) patientMapFind (uuidTenant string, uuidPatient string) (patientInfo *PatientSensorReadings) {
	ptntMap := bslnrCtxt.tenantMapFind(uuidTenant)
	if ptntMap == nil {
		ptntMap = bslnrCtxt.tenantMapInsert(uuidTenant)
	}
	ptntMap.patientMapLock.Lock()
	defer ptntMap.patientMapLock.Unlock()
	return ptntMap.patientMap[uuidPatient]
}


func (bslnrCtxt *baselinerContext) patientMapFindlocked (ptntMap *pmap, uuidPatient string) (patientInfo *PatientSensorReadings) {
	return ptntMap.patientMap[uuidPatient]
}

func (bslnrCtxt *baselinerContext) patientMapDelete (uuidTenant string, uuidPatient string) {
	pmap := bslnrCtxt.tenantMapFind(uuidTenant)
	pmap.patientMapLock.Lock()
	defer pmap.patientMapLock.Unlock()
	delete(pmap.patientMap, uuidPatient)
}


func (bslnrCtxt *baselinerContext) addNewPatientToSortList(uuidTenant string, uuidPatient string) {
	pmap := bslnrCtxt.tenantMapFind(uuidTenant)
	pmap.patientMapLock.Lock()
	defer pmap.patientMapLock.Unlock()
	pmap.sortedEwsList = append(pmap.sortedEwsList, uuidPatient)
}

func (bslnrCtxt *baselinerContext) grpcServerSetup(lstngIPPort string, secure bool) error {
	var s *grpc.Server

	l, err := net.Listen("tcp", lstngIPPort)
	if err != nil {
		bslnrCtxt.log.Errorf("failed to listen: %v", err)
		return err
	}

	if secure {
		creds, err := credentials.NewServerTLSFromFile(bslnrCtxt.cfg.grpcSrvrCert, bslnrCtxt.cfg.grpcSrvrKey)
		if err != nil {
			bslnrCtxt.log.Errorf("could not load TLS keys: %s", err)
			l = nil
			return err
		}

		bslnrCtxt.log.Infof("Creating secure server %s", lstngIPPort)
		s = grpc.NewServer(grpc.Creds(creds), grpc.KeepaliveParams(bslnrCtxt.std.grpcProfile.keepAliveSrvr))
	} else {
		ctrlLstngIPPort := "127.0.0.1:" + bslnrCtxt.std.ctrlPort
		bslnrCtxt.log.Infof("Creating insecure server: %s", lstngIPPort)
		if lstngIPPort == ctrlLstngIPPort {
			// Lets keep Localhost channel without keepalive for now.
			s = grpc.NewServer()
			//s = grpc.NewServer(grpc.KeepaliveParams(bslnrCtxt.std.grpcProfile.localKeepAliveSrvr))
		} else {
			s = grpc.NewServer(grpc.KeepaliveParams(bslnrCtxt.std.grpcProfile.keepAliveSrvr))
		}
	}

	
	osSrvr := NewBaselinerServer()
	osSrvr.bslnrCtxt = bslnrCtxt
	pb.RegisterEmrT1RpcServer(s, osSrvr)

	//pb.RegisterBaselinerRpcServer(s, osSrvr)

	if err := s.Serve(l); err != nil {
		bslnrCtxt.log.Errorf("failed to serve: %v", err)
		s = nil
		l = nil
		/* fallthrough */
	}

	return err
}



func (bslnrCtxt *baselinerContext) processBaselineWorkerChannel(i int) {
	bslnrCtxt.startGoRoutine(func() {


		for {
			recvData := <-bslnrCtxt.baselineWorkerChannel[i]
			bslnrCtxt.log.Error("In worker channel - %s",recvData)
			recvData = nil			
		}
	})
}

func HashBucket(word string, buckets int) int32 {
	return 3
	var sum int
	for _, v := range word{
		sum += int(v)
	}
	return int32((sum % buckets ) + 1)
}



func (bslnrCtxt *baselinerContext)  find(source []string, value string) bool {
    for _, item := range source {
        if item == value {
            return true
        }
    }
    return false
}