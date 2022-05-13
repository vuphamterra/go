package main

import (
	"context"
	"time"
	"math/rand"
	"github.com/segmentio/kafka-go"
	"sort"
	//"encoding/json"
	//"net/http"
	//"net"
	//influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	//pb "baseline_svc/proto_out/emr_consumer_proto"

)

const (
	ews = iota
)

type Pair struct {
	Key   string
	Value int32
}


type PairInfo struct {
	Key   string
	Value *PatientSensorReadings
}
type PairInfoList []PairInfo

type PairList []Pair


func (p PairList) Len() int           { return len(p) }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }


func (bslnrCtxt *baselinerContext) calcAverageInt(arra []int32) (int32) {
	sum := int32(0)
	nsamples := 0
	for i := 0; i < len(arra); i++ {
		if arra[i] > 500 || arra[i] < -100 {
			continue
		}
        sum += arra[i]
		nsamples++
    }
	if nsamples == 0 {
		return int32(999)
	}
    
	avg := (float64(sum)) / (float64(nsamples))

	return int32(avg)
}

func (bslnrCtxt *baselinerContext) calcAverageFloat(arra []float32) (float32) {
	sum := float32(0)
	nsamples := sum

	for i := 0; i < len(arra); i++ {
		if arra[i] > 500 || arra[i] < -100 {
			continue
		}
        sum += arra[i]
		nsamples++
    }
	
	if nsamples == 0 {
		return float32(999)
	}
    avg := (float64(sum)) / (float64(nsamples))
	avg = float64((int(avg*100))/100)

	return float32(avg)
}


func (bslnrCtxt *baselinerContext) calculateEwsLocked(patientInfo *PatientSensorReadings) {
	
	rr := patientInfo.lastUpdatedLiveRr
	spo2 := patientInfo.lastUpdatedLiveSpo2
	temperature := patientInfo.lastUpdatedLiveTemperature
	hr := patientInfo.lastUpdatedLiveHr

	ews := int32(0)


	if rr == 999 {
		ews += 0
	} else if (rr <= 8 || rr >=25) {
		ews += 3 
	} else if (rr >=9 && rr<=11) {
		ews += 1
	} else if(rr >=21 && rr<=24 ) {
		ews += 2
	}

	if spo2 == 999 {
		ews  += 0
	} else if spo2 <= 91 {
		ews += 3
	} else if spo2 >=92 && spo2 <=93 {
		ews += 2
	} else if spo2 >= 94 && spo2 <= 95 {
		ews += 2
	}

	if temperature == 999 {
		ews += 0
	} else if temperature <= 95 {
		ews += 3
	} else if temperature >= 95.1 && temperature <=96.8 {
		ews += 1
	} else if temperature >= 100.5 && temperature <= 102.2 {
		ews += 1
	} else if temperature >= 102.3  {
		ews += 2
	}

	if hr == 999 {
		ews += 0
	} else if hr <= 40 || hr >= 131{
		ews += 3
	} else if hr >=41 && hr <= 50 {
		ews += 1
	} else if hr >= 91 && hr <= 110 {
		ews += 1
	} else if hr >= 111 && hr <= 130 {
		ews += 2
	}

	//bslnrCtxt.log.Debugf("ews fpr patient %s . %d , rr %d ", patientInfo.uuidPatient, ews, patientInfo.rr[0][0])

	patientInfo.currEws = ews
}

func (bslnrCtxt *baselinerContext) calculateTenantEwsOnDemand(uuidTenant string) {
	ptntMap := bslnrCtxt.tenantMapFind(uuidTenant)
	ptntMap.patientMapLock.Lock()
	defer ptntMap.patientMapLock.Unlock()
	for _, patientInfoDetails := range ptntMap.patientMap {
		
		patientInfoDetails.patientLck.Lock()
		bslnrCtxt.calculateEwsLocked(patientInfoDetails)
		bslnrCtxt.log.Debugf("Calculating EWS ...%s", patientInfoDetails.uuidPatient)
		patientInfoDetails.patientLck.Unlock()
	}
}

func (bslnrCtxt *baselinerContext) sortewsOnTenant(uuidTenant string) {

	rnd := rand.Intn(10)
	time.Sleep(time.Duration(50 + rnd)  * time.Second) // First time at 1min
	bslnrCtxt.calculateTenantEwsOnDemand(uuidTenant)
	bslnrCtxt.sortRoutine(uuidTenant, ews)
	time.Sleep(time.Duration(120 +rnd)  * time.Second) // First time at 1min

	for {
		rnd = rand.Intn(10)
		bslnrCtxt.calculateTenantEwsOnDemand(uuidTenant)
		bslnrCtxt.sortRoutine(uuidTenant, ews)
		time.Sleep(time.Duration(600 + rnd)  * time.Second)
	}
}


func (bslnrCtxt *baselinerContext) sortRoutine(uuidTenant string, sortCritereon int) {
	ptntMap := bslnrCtxt.tenantMapFind(uuidTenant)
	p := make(PairList, bslnrCtxt.patientMapGetLen(ptntMap))
	
	i := int32(0)
	ptntMap.patientMapLock.Lock()
	defer ptntMap.patientMapLock.Unlock()

	//ews sort
	for k, v := range ptntMap.patientMap {
		p[i] = Pair{k, v.currEws}
		i++
	}
	ptntMap.numPatients = i
	sort.Sort(sort.Reverse(p))
	ptntMap.sortedEwsList = nil
	
	var sortedEwsList   []string
	for _, k := range p {
		sortedEwsList = append(sortedEwsList,  k.Key)
    }
	ptntMap.sortedEwsList = sortedEwsList
	//bslnrCtxt.log.Debugf("%v ",bslnrCtxt.sortedEwsList )

	//spo2 sort
	/*
	for k, v := range bslnrCtxt.patientMap {
		p[i] = Pair{k, v.spo2[0][0]}
		i++
	}
	sort.Sort(p)
	for _, k := range p {
     	bslnrCtxt.sortedEwsList = append(bslnrCtxt.sortedEwsList,  k.Key)
    }


	//temperature sort
	for k, v := range bslnrCtxt.patientMap {
		p[i] = Pair{k, v.temperature[0][0]}
		i++
	}
	sort.Sort(p)
	for _, k := range p {
     	bslnrCtxt.sortedEwsList = append(bslnrCtxt.sortedEwsList,  k.Key)
    }
	*/

}

func (bslnrCtxt *baselinerContext) patientBaselinerListen(kafkaLstnrTopic string) {

	bslnrCtxt.log.Debugf("[RegisterTopic] Listening on SensorConsumer")
	
	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address},
		Topic:   kafkaLstnrTopic,
		GroupID: kafkaLstnrTopic+"-grp",
		RetentionTime: 300 *time.Second,
		CommitInterval: 60 *time.Second,
	})
	defer  kafkaReader.Close()
	
	var msg kafka.Message
	var err error
	var noMsg int = 0

	for {
	
		msg, err = kafkaReader.ReadMessage(context.Background())
		if err != nil {
			bslnrCtxt.log.Debugf("[ListeningTopic] Sleeping for 10s  err %s", err)
			bslnrCtxt.startGoRoutine(func() {
				time.Sleep(time.Duration( (rand.Intn(10-1) + 1 )) * time.Second)
				bslnrCtxt.patientBaselinerListen(kafkaLstnrTopic)
				bslnrCtxt.wg.Done()
			})
			time.Sleep(10 * time.Second)
			noMsg++
			break
		}

		if noMsg >= 10 {
			// Receiving message after recovery
			// Need to move the alert to Closed state
		}
		noMsg = 0
		// after receiving the message, log its value
		bslnrCtxt.log.Debugf("received: %s", string(msg.Value))		
		bslnrCtxt.sendToWorker(msg.Value)
	}
}

/*
func (bslnrCtxt *baselinerContext) storeDailyDigestInflux(patientInfo *PatientSensorReadings) (string){


	uuidPatient   := patientInfo.uuidPatient
	trendsPatient := new(pb.TrendsPatient)
	trendsPatient.PatientUUID = &uuidPatient
	trendsPatient.Livehr 	= &(patientInfo.hr[0][0])
	trendsPatient.Liverr 	= &(patientInfo.rr[0][0])
	trendsPatient.Livespo2 	= &(patientInfo.spo2[0][0])
	trendsPatient.Livepr 	= &(patientInfo.pr[0][0])
	trendsPatient.Livepi 	= &(patientInfo.pi[0][0])
	trendsPatient.Livetemperature = &(patientInfo.temperature[0][0])
	trendsPatient.Ews 		= &patientInfo.currEws
	trendsPatient.LastUpdatedLiveTime =  &patientInfo.lastUpdatedLiveTime
	trendsPatient.LastUpdatedTrendTime =  &patientInfo.lastUpdatedTrendTime


	nHistory := 96
	
	if len(patientInfo.hr[2]) > nHistory {
		trendsPatient.Trendhr 	= patientInfo.hr[3][len(patientInfo.hr[3])-nHistory:]
		trendsPatient.Trendrr 	= patientInfo.rr[3][len(patientInfo.rr[3])-nHistory:]
	} else {
		trendsPatient.Trendhr 	= patientInfo.hr[3][1:]
		trendsPatient.Trendrr 	= patientInfo.rr[3][1:]
	}
	
	if len(patientInfo.hr[2]) > nHistory {
		trendsPatient.Trendspo2 = patientInfo.spo2[3][len(patientInfo.hr[2])-nHistory:]
		trendsPatient.Trendpr 	= patientInfo.pr[3][len(patientInfo.hr[2])-nHistory:]
		trendsPatient.Trendpi 	= patientInfo.pi[3][len(patientInfo.hr[2])-nHistory:]
	} else {
		trendsPatient.Trendspo2 = patientInfo.spo2[3][1:]
		trendsPatient.Trendpr 	= patientInfo.pr[3][1:]
		trendsPatient.Trendpi 	= patientInfo.pi[3][1:]
	}
	
	if len(patientInfo.hr[3]) > nHistory {
		trendsPatient.Trendtemperature 	= patientInfo.temperature[3][len(patientInfo.hr[3])-nHistory:]
	} else {
		trendsPatient.Trendtemperature 	= patientInfo.temperature[3][1:]
	}
		
	 
	jsonBytes, _ := json.MarshalIndent(trendsPatient, "", "    ")

	return string(jsonBytes)
	

}
*/

func (bslnrCtxt *baselinerContext) foldUpImplTenant(uuidTenant string, sortCritereon int) {
	sumInt := int32(0)
	sumFloat := float32(0)
	lenVal := int32(0)
	i := 0
	var info *PatientSensorReadings
	
	/*
	httpClientInflux := &http.Client{
        Timeout: time.Second * time.Duration(60),
        Transport: &http.Transport{
            DialContext: (&net.Dialer{
                Timeout: 5 * time.Second,
            }).DialContext,

            MaxIdleConns:        10,
            MaxIdleConnsPerHost: 10,
            IdleConnTimeout:     90 * time.Second,
        },
    }
	client1 := influxdb2.NewClientWithOptions(influxdbSvr, bslnrCtxt.influxAuthToken, influxdb2.DefaultOptions().SetHTTPClient(httpClientInflux))
	writeAPI := client1.WriteAPIBlocking("test_org", influxdbDatabase)
	defer client1.Close()
	//just for initialization
	writePoint := influxdb2.NewPointWithMeasurement("test")	
	measrmnt	 := ""
	jonsWriteData := ""
		*/

	ptntMap := bslnrCtxt.tenantMapFind(uuidTenant)

	
	ptntMap.patientMapLock.Lock()
	p := make(PairInfoList, len(ptntMap.patientMap))
	for k, v := range ptntMap.patientMap {
		p[i] = PairInfo{k, v}
		i++
	}
	ptntMap.patientMapLock.Unlock()


	//ews sort
	for _, v := range p {
		info = v.Value
		info.patientLck.Lock()


		/*
		jonsWriteData = bslnrCtxt.storeDailyDigestInflux(info)
		measrmnt = info.uuidPatient + "_daily_digest"
		writePoint = influxdb2.NewPointWithMeasurement(measrmnt).
		AddField("digest", jonsWriteData)
		err := writeAPI.WritePoint(context.Background(), writePoint)
		if err != nil {
			bslnrCtxt.log.Errorf("Patient %s, %v",info.uuidPatient, err)
		}
*/

		sumInt = bslnrCtxt.calcAverageInt(info.hr[3])
		info.foldUphr[3] = append(info.foldUphr[3], sumInt)
		if len(info.hr[3]) >= 96 {
			info.hr[1] = info.hr[1][len(info.hr[1])-96:len(info.hr[1])]
			info.hr[2] = info.hr[2][len(info.hr[2])-96:len(info.hr[2])]
			info.hr[3] = info.hr[3][len(info.hr[3])-96:len(info.hr[3])]
		}
		
		sumInt = bslnrCtxt.calcAverageInt(info.rr[3])
		info.foldUprr[3] = append(info.foldUprr[3], sumInt)
		if len(info.rr[3]) >= 96 {
			info.rr[1] = info.rr[1][len(info.rr[1])-96:len(info.rr[1])]
			info.rr[2] = info.rr[2][len(info.rr[2])-96:len(info.rr[2])]
			info.rr[3] = info.rr[3][len(info.rr[3])-96:len(info.rr[3])]
		}

		sumInt = bslnrCtxt.calcAverageInt(info.spo2[3])
		info.foldUpspo2[3] = append(info.foldUpspo2[3], sumInt)
		lenVal = int32(len(info.spo2[3]))
		if lenVal >= 96 {
			info.spo2[1] = info.spo2[1][lenVal-96:lenVal]
			info.spo2[2] = info.spo2[2][lenVal-96:lenVal]
			info.spo2[3] = info.spo2[3][lenVal-96:lenVal]
		}

		sumInt = bslnrCtxt.calcAverageInt(info.pr[3])
		info.foldUppr[3] = append(info.foldUppr[3], sumInt)
		lenVal = int32(len(info.pr[3]))
		if lenVal >= 96 {
			info.pr[1] = info.pr[1][lenVal-96:lenVal]
			info.pr[2] = info.pr[2][lenVal-96:lenVal]
			info.pr[3] = info.pr[3][lenVal-96:lenVal]
		}

		sumFloat = bslnrCtxt.calcAverageFloat(info.pi[3])
		info.foldUppi[3] = append(info.foldUppi[3], sumFloat)
		lenVal = int32(len(info.pi[3]))
		if lenVal >= 96 {
			info.pi[1] = info.pi[1][lenVal-96:lenVal]
			info.pi[2] = info.pi[2][lenVal-96:lenVal]
			info.pi[3] = info.pi[3][lenVal-96:lenVal]
		}

		sumFloat = bslnrCtxt.calcAverageFloat(info.temperature[3])
		info.foldUptemperature[3] = append(info.foldUptemperature[3], sumFloat)
		lenVal = int32(len(info.temperature[3]))
		if lenVal >= 96 {
			info.temperature[1] = info.temperature[1][lenVal-96:lenVal]
			info.temperature[2] = info.temperature[2][lenVal-96:lenVal]
			info.temperature[3] = info.temperature[3][lenVal-96:lenVal]
		}

		sumInt = bslnrCtxt.calcAverageInt(info.ews)
		info.foldUpews = append(info.foldUpews, sumInt)
		lenVal = int32(len(info.ews))
		if lenVal >= 96 {
			info.ews = info.ews[lenVal-96:lenVal]
		}
		
		sumInt = bslnrCtxt.calcAverageInt(info.batteryGateway)
		info.foldUpbatteryGateway = append(info.foldUpbatteryGateway, sumInt)
		lenVal = int32(len(info.batteryGateway))
		if lenVal >= 96 {
			info.batteryGateway = info.batteryGateway[lenVal-96:lenVal]
		}

		sumInt = bslnrCtxt.calcAverageInt(info.batteryEcg)
		info.foldUpbatteryEcg = append(info.foldUpbatteryEcg, sumInt)
		lenVal = int32(len(info.batteryEcg))
		if lenVal >= 96 {
			info.batteryEcg = info.batteryEcg[lenVal-96:lenVal]
		}
		
		sumInt = bslnrCtxt.calcAverageInt(info.batterySpo2)
		info.foldUpbatterySpo2 = append(info.foldUpbatterySpo2, sumInt)
		lenVal = int32(len(info.batterySpo2))
		if lenVal >= 96 {
			info.batterySpo2 = info.batterySpo2[lenVal-96:lenVal]
		}

		sumInt = bslnrCtxt.calcAverageInt(info.batteryTemperature)
		info.foldUpbatteryTemperature = append(info.foldUpbatteryTemperature, sumInt)
		lenVal = int32(len(info.batteryTemperature))
		if lenVal >= 96 {
			info.batteryTemperature = info.batteryTemperature[lenVal-96:lenVal]
		}

		sumInt = bslnrCtxt.calcAverageInt(info.bps[1])
		info.foldUpbps[1] = append(info.foldUpbps[1], sumInt)
		lenVal = int32(len(info.foldUpbps[1]))
		if lenVal >= 96 {
			info.bps[1] = info.bps[1][lenVal-96:lenVal]
		}

		sumInt = bslnrCtxt.calcAverageInt(info.bpd[1])
		info.foldUpbpd[1] = append(info.foldUpbpd[1], sumInt)
		lenVal = int32(len(info.bpd[1]))
		if lenVal >= 96 {
			info.bpd[1] = info.bpd[1][lenVal-96:lenVal]
		}
		
		
		info.patientLck.Unlock()
	}

}


func (bslnrCtxt *baselinerContext) foldUpImpl(sortCritereon int) {
	bslnrCtxt.tenantMapLock.Lock()
	var aslice []string
	for k, _ := range bslnrCtxt.tenantMap {
		aslice = append(aslice, k)
	}
	bslnrCtxt.tenantMapLock.Unlock()

	for _, v := range aslice {
		bslnrCtxt.foldUpImplTenant(v, sortCritereon)
	}
}



func (bslnrCtxt *baselinerContext) foldUpRoutine() {
	t := time.Now()
	delay := time.Duration(1)
	currHour := 4
	for {
		t = time.Now()

		bslnrCtxt.log.Debugf("@rajsreen ... %d",t.Hour())
		//bslnrCtxt.foldUpImpl(0)
		//time.Sleep(5 * time.Minute) 
		//continue
		if t.Hour() == 4 || t.Hour() == 5 {
			currHour = t.Hour()
			bslnrCtxt.log.Debugf("Running foldup routine")
			bslnrCtxt.foldUpImpl(0)
			delay = time.Duration(24)
			bslnrCtxt.log.Debugf("@rajsreen .DELAy.. %d",delay)
			time.Sleep(delay * time.Hour) 
		} else {
			
			if currHour > t.Hour() {
				delay = time.Duration(currHour- t.Hour())
			} else {
				delay = time.Duration(24 - t.Hour() - currHour)
			}
			bslnrCtxt.log.Debugf("@rajsreen .DELAy.. %d",delay)
			time.Sleep(delay * time.Hour) 
		}
	}
}



func (bslnrCtxt *baselinerContext) storeToDbRoutine() {

	for {
		
		bslnrCtxt.log.Debugf("Running storeToDb routine")
		bslnrCtxt.storeValuesToRedis()
		bslnrCtxt.log.Debugf("Done storeToDb routine ..sleeping")
		time.Sleep(100 * time.Second)
		
	}
}
