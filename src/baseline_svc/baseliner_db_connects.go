package main

import (
	"log"
	"time"
	"bytes"
	"context"
	"strings"
	"io/ioutil"

	"math/rand"
	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	"encoding/json"
	"github.com/segmentio/kafka-go"

)

const (
	broker1Address = "kafkaBroker1:9092"
)
const (
	kafkaLstnrTopic1 = "baselinerTopic1"
)

const (
	redisSvr1 = "redisSvr1:6379"
)
const (
	influxdbSvr = "http://influxdbSvr:8086"
)


const (
	influxorg = "test_org"
)

const (
	//influxdbDatabase = "emr_dev/1year"
	influxdbDatabase = "emr_dev"
)

const (
	influxAuthToken = "prodToken"
)

type StoreInfoLive struct {
	HrCur   	int32
	LastUpdateEcgTime uint64
	Rrcur 		int32
	Spo2Cur		int32
	LastUpdateSpo2Time uint64
	PrCur		int32
	PiCur		float32

	LastUpdateTemperatureTime uint64
	TemperatureCur	float32

	LastUpdateBPTime uint64
	BPCur			int32

	LastUpdateWeightTime uint64
	WeightCur		float32

}

type StoreInfo struct {
	currTime 			uint64
	FoldUpHrMax   	[]int32
	FoldUpHrMin   	[]int32
	FoldUpHrAvg   	[]int32
	FoldUpRrMax   	[]int32
	FoldUpRrMin   	[]int32
	FoldUpRrAvg   	[]int32
	FoldUpSpo2Max   	[]int32
	FoldUpSpo2Min   	[]int32
	FoldUpSpo2Avg   	[]int32
	FoldUpPrMax   			[]int32
	FoldUpPrMin   			[]int32
	FoldUpPrAvg   			[]int32
	FoldUpPiMax   			[]float32
	FoldUpPiMin   			[]float32
	FoldUpPiAvg   			[]float32
	FoldUpTemperatureMax   	[]float32
	FoldUpTemperatureMin   	[]float32
	FoldUpTemperatureAvg   	[]float32

	HrCur   	[]int32
	HrMax   	[]int32
	HrMin   	[]int32
	HrAvg   	[]int32

	RrCur   	[]int32
	RrMax   	[]int32
	RrMin   	[]int32
	RrAvg   	[]int32

	Spo2Cur   	[]int32
	Spo2Max   	[]int32
	Spo2Min   	[]int32
	Spo2Avg   	[]int32

	PrCur   			[]int32
	PrMax   			[]int32
	PrMin   			[]int32
	PrAvg   			[]int32

	PiCur   			[]float32
	PiMax   			[]float32
	PiMin   			[]float32
	PiAvg   			[]float32

	TemperatureCur   	[]float32
	TemperatureMax   	[]float32
	TemperatureMin   	[]float32
	TemperatureAvg   	[]float32
}



const (
	kafkaPatientDiscovery = "patientDiscovery"
)


type sevlevels struct {
	Sev1	struct{
		Min 		int32 `json:min,omitempty`
		Max 		int32 `json:max,omitempty`
	} `json:"sev1"`
	Sev2	struct{
		Min 		int32 `json:min,omitempty`
		Max 		int32 `json:max,omitempty`
	} `json:"sev2,omitempty"`
	Sev3	struct{
		Min 		int32 `json:min,omitempty`
		Max 		int32 `json:max,omitempty`
	} `json:"sev3,omitempty"`
}

type patientDiscover struct {

	UuidPatient    			string `json:"uuidPatient"`
	UuidTenant 				string `json:"uuidTenant,omitempty"`
	Method 					string `json:"method"`
	Thresholds				struct {
		BPS     sevlevels  `json:"bPS,omitempty"`
		BPD     sevlevels  `json:"bPD,omitempty"`
		RR	    sevlevels  `json:"rR,omitempty"`
		HR	    sevlevels  `json:"hR,omitempty"`
		Temp	    sevlevels  `json:"temp,omitempty"`
		SPo2	    sevlevels  `json:"sPo2,omitempty"`
		DS	    	sevlevels  `json:"dS,omitempty"`
		Glucometer	sevlevels  `json:"glucometer,omitempty"`

	} `json:thresholds,omitempty`
	FrequencySetting 		int32 `json:frequencySetting,omitempty`
	Bps 					int32 `json:"bps,omitempty"`
	Bpd 					int32 `json:"bpd,omitempty"`
	Sugar 					int32 `json:"sugar,omitempty"`
	Spo2Threshold 			int32 `json:"spo2Threshold,omitempty"`
	TemperatureThreshold 	int32 `json:"temperatureThreshold,omitempty"`
	PrThreshold 		int32 `json:"prThreshold,omitempty"`
	PiThreshold 		float32 `json:"piThreshold,omitempty"`
}

func (bslnrCtxt *baselinerContext) readInfluxAuthFile() (bool){
	data, err := ioutil.ReadFile("/shared/influx_auth_token.txt")
	if err != nil {
		return false
	}

	bslnrCtxt.influxAuthToken = string(data)
	bslnrCtxt.influxAuthToken = strings.Replace(bslnrCtxt.influxAuthToken, "\n","",-1)
	bslnrCtxt.log.Debugf("influx token %s", bslnrCtxt.influxAuthToken)
	return true
}

func (bslnrCtxt *baselinerContext) listenOnPatientDiscoveryChannel() {

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address},
		Topic:   kafkaPatientDiscovery,
		GroupID: kafkaPatientDiscovery+"-grp1",
	})
	defer  kafkaReader.Close()

	var msg kafka.Message
	var err error
	patientUUID := ""
	tenantUUID := ""
	var patientDiscoverMsg patientDiscover

	bslnrCtxt.log.Debugf("listenOnPatientDiscoveryChannel  %s %s %v",kafkaPatientDiscovery, broker1Address, kafkaReader)

	for {
		bslnrCtxt.log.Debugf("entered listenOnPatientDiscoveryChannel")

		msg, err = kafkaReader.ReadMessage(context.Background())
		if err != nil {
			bslnrCtxt.log.Debugf("[ListeningTopic] Sleeping for 10s %s , err %s", kafkaPatientDiscovery, err)
			bslnrCtxt.startGoRoutine(func() {
				time.Sleep(time.Duration( (rand.Intn(10-1) + 1 )) * time.Second)
				bslnrCtxt.listenOnPatientDiscoveryChannel()
				bslnrCtxt.wg.Done()
			})
			time.Sleep(1 * time.Second)
			break
		}
		bslnrCtxt.log.Debugf("received msg listenOnPatientDiscoveryChannel")

		err = json.Unmarshal(msg.Value, &patientDiscoverMsg)

		patientUUID = strings.TrimSpace(patientDiscoverMsg.UuidPatient)
		tenantUUID = strings.TrimSpace(patientDiscoverMsg.UuidTenant)
		bslnrCtxt.log.Debugf("received patient %s on Tenant %s",patientUUID , tenantUUID)
		if patientDiscoverMsg.Method == "CREATE" {

			bslnrCtxt.log.Debugf("Create Patient] Entered %s", patientUUID)
			patientInfoDetails := bslnrCtxt.patientMapFind(tenantUUID, patientUUID)
			if patientInfoDetails == nil {
				patientInfoDetails = bslnrCtxt.createNewPatientContext(tenantUUID, patientUUID)
			}
		} else if patientDiscoverMsg.Method == "DELETE" {
			bslnrCtxt.log.Debugf("DELETE Patient] Entered %s Tenant %s", patientUUID, tenantUUID)
			bslnrCtxt.deletePatientContext(tenantUUID, patientUUID)
			bslnrCtxt.patientMapDelete(tenantUUID, patientUUID)
		}
	}
	bslnrCtxt.log.Debugf("Exiting listenOnPatientDiscoveryChannel  %s",kafkaPatientDiscovery)
		
}


func (bslnrCtxt *baselinerContext) fetchValuesFromRedis() {
	bslnrCtxt.tenantMapLock.Lock()
	defer bslnrCtxt.tenantMapLock.Unlock()

	for _, v := range bslnrCtxt.tenantMap {
		//bslnrCtxt.fetchPatientValuesFromRedis(v)
		bslnrCtxt.fetchPatientValuesLiveFromRedis(v)
	}

}


func (bslnrCtxt *baselinerContext) fetchPatientValuesLiveFromRedis(ptntMap *pmap) {

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisSvr1,
		Password: "sOmE_sEcUrE_pAsS",
		DB:       int(ptntMap.dbHashBucket),
	})
	
	defer redisClient.Close()

	var storeInfo  StoreInfoLive
	//var info *PatientSensorReadings

	val := ""
	var err error
	var i int32

	bslnrCtxt.log.Debugf("@rajsreen...fetchPatientValuesFromRedis %d", ptntMap.dbHashBucket)
	ptntMap.patientMapLock.Lock()
	defer ptntMap.patientMapLock.Unlock()
		for k, info := range ptntMap.patientMap {
			i++
			storeInfo = StoreInfoLive{}
			val, err = redisClient.Get(k+"Live").Result()
			if err != nil {
				continue
				//panic(err)
			}
			err = json.Unmarshal([]byte(val), &storeInfo)

			info.patientLck.Lock()
			
			info.lastUpdatedLiveHr = storeInfo.HrCur
			info.lastUpdatedLiveEcgTime = storeInfo.LastUpdateEcgTime	
			info.lastUpdatedLiveRr		 = storeInfo.Rrcur
	
			info.lastUpdatedLiveSpo2	= storeInfo.Spo2Cur
			info.lastUpdatedLiveSpo2Time 	= storeInfo.LastUpdateSpo2Time
			info.lastUpdatedLivePr		= storeInfo.PrCur
			info.lastUpdatedLivePi		=	storeInfo.PiCur	
	
			info.lastUpdatedLiveTemperatureTime = storeInfo.LastUpdateTemperatureTime
			info.lastUpdatedLiveTemperature	= storeInfo.TemperatureCur
	
			info.lastUpdatedLiveBPTime	= storeInfo.LastUpdateBPTime
			info.lastUpdatedLiveBp	= storeInfo.BPCur
	
			info.lastUpdatedLiveWeightTime = storeInfo.LastUpdateWeightTime
			info.lastUpdatedLiveWeight	= storeInfo.WeightCur
			
		
			info.patientLck.Unlock()
			bslnrCtxt.log.Debugf("Reading from stored\n %v --- %v\n\n", info, storeInfo)

		}
		ptntMap.numPatients = i
		bslnrCtxt.log.Debugf("@rajsreen...fetchPatientValuesFromRedis DONE")
}

func (bslnrCtxt *baselinerContext) fetchPatientValuesFromRedis(ptntMap *pmap) {

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisSvr1,
		Password: "sOmE_sEcUrE_pAsS",
		DB:       int(ptntMap.dbHashBucket),
	})
	
	defer redisClient.Close()

	var storeInfo  StoreInfo
	//var info *PatientSensorReadings

	val := ""
	var err error
	var i int32

	bslnrCtxt.log.Debugf("@rajsreen...fetchPatientValuesFromRedis %d", ptntMap.dbHashBucket)
	ptntMap.patientMapLock.Lock()
	defer ptntMap.patientMapLock.Unlock()
		for k, info := range ptntMap.patientMap {
			i++
			storeInfo = StoreInfo{}
			val, err = redisClient.Get(k).Result()
			if err != nil {
				continue
				//panic(err)
			}
			err = json.Unmarshal([]byte(val), &storeInfo)

			info.patientLck.Lock()
			
			info.hr[0] 		= storeInfo.HrCur
			
			info.hr[1] 		= storeInfo.HrMax[:]
			info.hr[2] 		= storeInfo.HrMin[:]
			info.hr[3] 		= storeInfo.HrAvg[:]

			info.rr[0] 		= storeInfo.RrCur[:]
			info.rr[1]		= storeInfo.RrMax[:]
			info.rr[2] 		= storeInfo.RrMin[:]
			info.rr[3] 		= storeInfo.RrAvg[:]

			info.spo2[0] 	= storeInfo.Spo2Cur
			info.spo2[1] 	= storeInfo.Spo2Max[:]
			info.spo2[2]	= storeInfo.Spo2Min[:]
			info.spo2[3] 	= storeInfo.Spo2Avg[:]
			
			info.pr[0]		= storeInfo.PrCur
			info.pr[1]		= storeInfo.PrMax[:]
			info.pr[2]		= storeInfo.PrMin[:]
			info.pr[3]		= storeInfo.PrAvg[:]

			info.pi[0]		= storeInfo.PiCur
			info.pi[1]		= storeInfo.PiMax[:]
			info.pi[2]		= storeInfo.PiMin[:]
			info.pi[3]		= storeInfo.PiAvg[:]

			info.temperature[0] = storeInfo.TemperatureCur
			info.temperature[1] = storeInfo.TemperatureMax[:]
			info.temperature[2]	= storeInfo.TemperatureMin[:]
			info.temperature[3]	= storeInfo.TemperatureAvg[:]

			info.foldUphr[1] = storeInfo.FoldUpHrMax[:]
			info.foldUphr[2] = storeInfo.FoldUpHrMin[:]
			info.foldUphr[3] = storeInfo.FoldUpHrAvg[:]
			info.foldUprr[1] = storeInfo.FoldUpRrMax[:]
			info.foldUprr[2] = storeInfo.FoldUpRrMin[:]
			info.foldUprr[3] = storeInfo.FoldUpRrAvg[:]
			
			info.foldUpspo2[1] = storeInfo.FoldUpSpo2Max[:]
			info.foldUpspo2[2] = storeInfo.FoldUpSpo2Min[:]
			info.foldUpspo2[3] = storeInfo.FoldUpSpo2Avg[:]
			
			info.foldUppr[1] = storeInfo.FoldUpPrMax[:]
			info.foldUppr[2] = storeInfo.FoldUpPrMin[:]
			info.foldUppr[3] = storeInfo.FoldUpPrAvg[:]
			
			info.foldUppi[1] = storeInfo.FoldUpPiMax[:]
			info.foldUppi[2] = storeInfo.FoldUpPiMin[:]
			info.foldUppi[3] = storeInfo.FoldUpPiAvg[:]
			
			info.foldUptemperature[1] = storeInfo.FoldUpTemperatureMax[:]
			info.foldUptemperature[2] = storeInfo.FoldUpTemperatureMin[:]
			info.foldUptemperature[3] = storeInfo.FoldUpTemperatureAvg[:]
			
			info.lastUpdatedTrendTime = storeInfo.currTime

			info.patientLck.Unlock()
			bslnrCtxt.log.Debugf("Reading from stored\n %v\n\n", info)

		}
		
		ptntMap.numPatients = i
		bslnrCtxt.log.Debugf("@rajsreen...fetchPatientValuesFromRedis DONE")

	
}


func (bslnrCtxt *baselinerContext) storeValuesToRedis() {
	bslnrCtxt.tenantMapLock.Lock()
	defer bslnrCtxt.tenantMapLock.Unlock()

	for _, v := range bslnrCtxt.tenantMap {
		//bslnrCtxt.storePatientValuesToRedis(v)
		bslnrCtxt.storePatientValuesLiveToRedis(v)
	}
}


func (bslnrCtxt *baselinerContext) storePatientValuesLiveToRedis(ptntMap *pmap) {
	
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisSvr1,
		Password: "sOmE_sEcUrE_pAsS",
		DB:       int(ptntMap.dbHashBucket),
	})
	
	defer redisClient.Close()

	var info *PatientSensorReadings
	i := 0
	ptntMap.patientMapLock.Lock()
	p := make(PairInfoList, len(ptntMap.patientMap))
	for k, v := range ptntMap.patientMap {
		p[i] = PairInfo{k, v}
		i++
	}
	ptntMap.patientMapLock.Unlock()

	for _, v := range p {
		info = v.Value
		info.patientLck.Lock()

		storeInfo := &StoreInfoLive{}
		storeInfo.HrCur = info.lastUpdatedLiveHr
		storeInfo.LastUpdateEcgTime	= info.lastUpdatedLiveEcgTime
		storeInfo.Rrcur				= info.lastUpdatedLiveRr

		storeInfo.Spo2Cur			= info.lastUpdatedLiveSpo2
		storeInfo.LastUpdateSpo2Time			= info.lastUpdatedLiveSpo2Time
		storeInfo.PrCur				= info.lastUpdatedLivePr
		storeInfo.PiCur				= info.lastUpdatedLivePi

		storeInfo.LastUpdateTemperatureTime =  info.lastUpdatedLiveTemperatureTime
		storeInfo.TemperatureCur	=  info.lastUpdatedLiveTemperature

		storeInfo.LastUpdateBPTime	=  info.lastUpdatedLiveBPTime
		storeInfo.BPCur				=  info.lastUpdatedLiveBp

		storeInfo.LastUpdateWeightTime =  info.lastUpdatedLiveWeightTime
		storeInfo.WeightCur				=  info.lastUpdatedLiveWeight


		info.patientLck.Unlock()

		dat, _ := json.Marshal(storeInfo)
		
		err := redisClient.Set(v.Key+"Live", string(dat), 0).Err()
		if err != nil {
			bslnrCtxt.log.Debugf("Failed to do Set %s .. %s", err, v.Key)
		}
	}

}

func (bslnrCtxt *baselinerContext) storePatientValuesToRedis(ptntMap *pmap) {
	
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisSvr1,
		Password: "sOmE_sEcUrE_pAsS",
		DB:       int(ptntMap.dbHashBucket),
	})
	
	defer redisClient.Close()

	var info *PatientSensorReadings
	i := 0
	ptntMap.patientMapLock.Lock()
	p := make(PairInfoList, len(ptntMap.patientMap))
	for k, v := range ptntMap.patientMap {
		p[i] = PairInfo{k, v}
		i++
	}
	ptntMap.patientMapLock.Unlock()

	currTimenow := time.Now()
	currTimeSec := uint64(currTimenow.Unix())

	for _, v := range p {
		info = v.Value
		info.patientLck.Lock()

		storeInfo := &StoreInfo{currTime: currTimeSec}
		if info.foldUphr != nil && len(info.foldUphr) != 0 {
			if info.foldUphr[1] != nil {
				storeInfo.FoldUpHrMax = info.foldUphr[1]
			}
			
			if info.foldUphr[2] != nil {
				storeInfo.FoldUpHrMin = info.foldUphr[2]
			}

			if info.foldUphr[3] != nil {
				storeInfo.FoldUpHrAvg = info.foldUphr[3]
			}
			
			if info.foldUprr[1] != nil {
				storeInfo.FoldUpRrMax = info.foldUprr[1]
			}
			if info.foldUprr[2] != nil {
				storeInfo.FoldUpRrMin = info.foldUprr[2]
			}
			if info.foldUprr[3] != nil {
				storeInfo.FoldUpRrAvg = info.foldUprr[3]
			}

			if info.foldUpspo2[1] != nil {
				storeInfo.FoldUpSpo2Max = info.foldUpspo2[1]
			}
			if info.foldUpspo2[2] != nil {
				storeInfo.FoldUpSpo2Min = info.foldUpspo2[2]
			}
			if info.foldUpspo2[3] != nil {
				storeInfo.FoldUpSpo2Avg = info.foldUpspo2[3]
			}

			if info.foldUppr[1] != nil {
				storeInfo.FoldUpPrMax = info.foldUppr[1]
			}
			if info.foldUppr[2] != nil {
				storeInfo.FoldUpPrMin = info.foldUppr[2]
			}
			if info.foldUppr[3] != nil {
				storeInfo.FoldUpPrAvg = info.foldUppr[3]
			}

			if info.foldUppi[1] != nil {
				storeInfo.FoldUpPiMax = info.foldUppi[1]
			}
			if info.foldUppi[2] != nil {
				storeInfo.FoldUpPiMin = info.foldUppi[2]
			}
			if info.foldUppi[3] != nil {
				storeInfo.FoldUpPiAvg = info.foldUppi[3]
			}
			
			if info.foldUptemperature[1] != nil {
				storeInfo.FoldUpTemperatureMax = info.foldUptemperature[1]
			}
			if info.foldUptemperature[2] != nil {
				storeInfo.FoldUpTemperatureMin = info.foldUptemperature[2]
			}
			if info.foldUptemperature[3] != nil {
				storeInfo.FoldUpTemperatureAvg = info.foldUptemperature[3]
			}
		}


		if len(info.hr) != 0 {

			if info.hr[0] != nil {
				storeInfo.HrCur = info.hr[0]
			}
			
			if info.hr[1] != nil {
				storeInfo.HrMax = info.hr[1]
			}
			if info.hr[2] != nil {
				storeInfo.HrMin = info.hr[2]
			}
			if info.hr[3] != nil {
				storeInfo.HrAvg = info.hr[3]
			}
			
			
			if info.rr[0] != nil {
				storeInfo.RrCur = info.rr[0]
			}
			
			if info.rr[1] != nil {
				storeInfo.RrMax = info.rr[1]
			}
			if info.rr[2] != nil {
				storeInfo.RrMin = info.rr[2]
			}
			if info.rr[3] != nil {
				storeInfo.RrAvg = info.rr[3]
			}


			if info.spo2[0] != nil {
				storeInfo.Spo2Cur = info.spo2[0]
			}
			
			if info.spo2[1] != nil {
				storeInfo.Spo2Max = info.spo2[1]
			}
			if info.spo2[2] != nil {
				storeInfo.Spo2Min = info.spo2[2]
			}
			if info.spo2[3] != nil {
				storeInfo.Spo2Avg = info.spo2[3]
			}


			if info.pr[0] != nil {
				storeInfo.PrCur = info.pr[0]
			}
			
			if info.pr[1] != nil {
				storeInfo.PrMax = info.pr[1]
			}
			if info.pr[2] != nil {
				storeInfo.PrMin = info.pr[2]
			}
			if info.pr[3] != nil {
				storeInfo.PrAvg = info.pr[3]
			}


			if info.pi[0] != nil {
				storeInfo.PiCur = info.pi[0]
			}
			
			if info.pi[1] != nil {
				storeInfo.PiMax = info.pi[1]
			}
			if info.pi[2] != nil {
				storeInfo.PiMin = info.pi[2]
			}
			if info.pr[3] != nil {
				storeInfo.PiAvg = info.pi[3]
			}


			if info.temperature[0] != nil {
				storeInfo.TemperatureCur = info.temperature[0]
			}
			
			if info.temperature[1] != nil {
				storeInfo.TemperatureMax = info.temperature[1]
			}
			if info.temperature[2] != nil {
				storeInfo.TemperatureMin = info.temperature[2]
			}
			if info.temperature[3] != nil {
				storeInfo.TemperatureAvg = info.temperature[3]
			}
		}
		info.patientLck.Unlock()

		dat, _ := json.Marshal(storeInfo)
		
		err := redisClient.Set(v.Key, string(dat), 0).Err()
		if err != nil {
			bslnrCtxt.log.Debugf("Failed to do Set %s .. %s", err, v.Key)
		}
	}

}


/********************************************************************************/
func (bslnrCtxt *baselinerContext) readRegisterTenants() {
	bslnrCtxt.createTopicKafka(kafkaLstnrTopic1)

	bslnrCtxt.log.Debugf("Registering Tenants")
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisSvr1,
		Password: "sOmE_sEcUrE_pAsS",
		DB:       1,
	})
	smembers := redisClient.SMembers("tenants")
	redisClient.Close()
	if smembers.Err() != nil {
		bslnrCtxt.log.Debugf("Error reading topics in redis")
	} else {
		for _, v := range smembers.Val() {
			bslnrCtxt.tenantMapInsert(v)			
		}
	}
}

func (bslnrCtxt *baselinerContext) readRegisterPatientFromTenant(uuidTenant string) {

	bslnrCtxt.log.Debugf("Registering Tenant %s",uuidTenant )
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisSvr1,
		Password: "sOmE_sEcUrE_pAsS",
		DB:       1,
	})
	smembers := redisClient.SMembers(uuidTenant)
	redisClient.Close()
	if smembers.Err() != nil {
		bslnrCtxt.log.Debugf("Error reading topics in redis")
	} else {
		var patientInfoDetails *PatientSensorReadings
		for _, v := range smembers.Val() {
			patientInfoDetails = bslnrCtxt.patientMapFind(uuidTenant, v)
			if patientInfoDetails == nil {
				patientInfoDetails = bslnrCtxt.createNewPatientContext(uuidTenant, v)
			}
		}
	}
}

func (bslnrCtxt *baselinerContext) populateTenantPatientDetails() {

	bslnrCtxt.readRegisterTenants()
	bslnrCtxt.tenantMapLock.Lock()
	var aslice []string
	for k, _ := range bslnrCtxt.tenantMap {
		aslice = append(aslice, k)
	}
	bslnrCtxt.tenantMapLock.Unlock()

	for _,v := range aslice {
		bslnrCtxt.readRegisterPatientFromTenant(v)
	}

	bslnrCtxt.patientBaselinerListenerSpawn(kafkaLstnrTopic1)
}

func (bslnrCtxt *baselinerContext) createTopicKafka(lstnTopic string) {

	brokerAddrs := []string{broker1Address}
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin(brokerAddrs, config)
	if err != nil {
		log.Println("Error while creating cluster admin: ", err.Error())
	}

	//defer func() { _ = admin.Close() }()
	err = admin.CreateTopic(lstnTopic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)

	if err != nil {
		log.Println("Error while creating topic: ", err.Error())
	}
	_ = admin.Close()
}
/********************************************************************************/



func (bslnrCtxt *baselinerContext) patientBaselinerListenerSpawn(patientUUID string) {
	go bslnrCtxt.patientBaselinerListen(kafkaLstnrTopic1)
}


func (bslnrCtxt *baselinerContext) sendToWorker(msg []byte) {

	bslnrCtxt.log.Infof("Sending to worker")

	responseBody := bytes.NewBuffer(msg)
	retry_again := 3
	ret := false
	idx := 0
	for retry_again > 0 {
		idx = (rand.Intn(40-1) + 1 )
		select {
		case bslnrCtxt.baselineWorkerChannel[idx] <- responseBody:
			ret = true	
			retry_again = 0
		case <- time.After(500*time.Millisecond):
			ret = false
			retry_again--
		}
	}

	if ret == false {
		bslnrCtxt.log.Errorf("Failed to enq %s ..need inline processing", msg)
	}
}
