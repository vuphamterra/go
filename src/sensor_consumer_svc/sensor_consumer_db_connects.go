package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"fmt"
	"math/rand"

	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	pb "sensor_consumer_svc/proto_out/emr_consumer_proto"
	"github.com/segmentio/kafka-go"
	"io/ioutil"

)

const (
	broker1Address = "kafkaBroker1:9092"
)

const (
	redisSvr1 = "redisSvr1:6379"
)

const (
	deepLearningEngineAddress = "127.0.0.1:50051"
)

const (
	baselinerAddress1 = "baseliner:9010"
)

const (
	//2 Minutes
	retentionKafkaTopic = "120000" 
)

const (
	influxdbSvr = "http://influxdbSvr:8086"
)

const (
	//influxdbDatabase = "emr_dev/1year"
	influxdbDatabase = "emr_dev"
)

const (
	influxAuthToken = "zVZzsDHtHBQFc97Dk7PUAGhDTW6FVN4B2huJoRMU2UM_4XH2DmDiLa8OU8-A1DLLPPGLrRX786rfpz9I88zXkw=="
)

const (
	alertaApp1 = "http://alertaApp1:8999/alert"
)

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
	PrThreshold 			int32 `json:"prThreshold,omitempty"`
	PiThreshold 			float32 `json:"piThreshold,omitempty"`
}


func (t1Ctxt *emrTier1Context) readInfluxAuthFile() (bool){
	data, err := ioutil.ReadFile("/shared/influx_auth_token.txt")
	if err != nil {
		return false
	}


	t1Ctxt.influxAuthToken = string(data)
	t1Ctxt.influxAuthToken = strings.Replace(t1Ctxt.influxAuthToken, "\n","",-1)
	t1Ctxt.log.Debugf("influx token %s", t1Ctxt.influxAuthToken)
	t1Ctxt.log.Debugf("influx tokendsdd %v", data)

	return true
}

func (t1Ctxt *emrTier1Context) patientMapInsert (uuidPatient string, ) {
	patientInfo := new(PatientData)
	t1Ctxt.patientMapLock.Lock()
	defer t1Ctxt.patientMapLock.Unlock()
	t1Ctxt.patientMap[uuidPatient] = patientInfo
}

func (t1Ctxt *emrTier1Context) patientMapFind (uuidPatient string) (patientInfo *PatientData) {
	t1Ctxt.patientMapLock.Lock()
	defer t1Ctxt.patientMapLock.Unlock()
	return t1Ctxt.patientMap[uuidPatient]
}


func (t1Ctxt *emrTier1Context) patientMapFindUpdate (uuidPatient string, bps int32, bpd int32, sugar int32) {
	t1Ctxt.patientMapLock.Lock()
	defer t1Ctxt.patientMapLock.Unlock()

	pInfo :=  t1Ctxt.patientMap[uuidPatient]
	
	pInfo.bps = bps
	pInfo.bpd = bpd
	pInfo.sugar = sugar
	
}

func (t1Ctxt *emrTier1Context) patientMapDelete (uuidPatient string) {
	t1Ctxt.patientMapLock.Lock()
	defer t1Ctxt.patientMapLock.Unlock()
	t1Ctxt.patientMap[uuidPatient] = nil
	delete(t1Ctxt.patientMap, uuidPatient)
}


func (t1Ctxt *emrTier1Context) listenOnPatientDiscoveryChannel() {

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address},
		Topic:   kafkaPatientDiscovery,
		GroupID: kafkaPatientDiscovery+"-grp",
	})
	defer  kafkaReader.Close()

	var msg kafka.Message
	var err error
	patientUUID := ""
	tenantUUID := ""

	var patientDiscoverMsg patientDiscover
	var bps int32
	var bpd int32
	var sugar int32

	t1Ctxt.log.Debugf("Listening on Kafka topic %s", kafkaPatientDiscovery)

	for {
		msg, err = kafkaReader.ReadMessage(context.Background())
		if err != nil {
			t1Ctxt.log.Debugf("[ListeningTopic] Sleeping for 10s %s , err %s", kafkaPatientDiscovery, err)
			t1Ctxt.startGoRoutine(func() {
				time.Sleep(time.Duration( (rand.Intn(10-1) + 1 )) * time.Second)
				t1Ctxt.listenOnPatientDiscoveryChannel()
				t1Ctxt.wg.Done()
			})
			time.Sleep(1 * time.Second)
			break
		}
		err = json.Unmarshal(msg.Value, &patientDiscoverMsg)
		patientUUID = strings.TrimSpace(patientDiscoverMsg.UuidPatient)
		tenantUUID = strings.TrimSpace(patientDiscoverMsg.UuidTenant)

		t1Ctxt.log.Debugf("received patient %s on tenant %s",patientUUID, tenantUUID)
		if patientDiscoverMsg.Method == "CREATE" {
			t1Ctxt.log.Debugf("[CREATE Patient] Entered %s %s", tenantUUID, patientUUID)
			t1Ctxt.createTopicKafka(tenantUUID, patientUUID)
			t1Ctxt.patientTopicRegisterListenerSpawn(tenantUUID, patientUUID)
		} else if patientDiscoverMsg.Method == "UPDATE" {
			t1Ctxt.log.Debugf("UPDATE Patient] Entered %s on tenant %s", patientUUID, tenantUUID)
			if patientDiscoverMsg.Bps != 0 {
				bps = patientDiscoverMsg.Bps
			}
			if patientDiscoverMsg.Bpd != 0 {
				bps = patientDiscoverMsg.Bpd
			}
			if patientDiscoverMsg.Sugar != 0 {
				bps = patientDiscoverMsg.Sugar
			}
			t1Ctxt.patientMapFindUpdate(patientUUID, bps, bpd, sugar)
		} else if patientDiscoverMsg.Method == "DELETE" {
			t1Ctxt.log.Debugf("DELETE Patient] Entered %s", patientUUID)
			t1Ctxt.patientMapDelete(patientUUID)
		}
	}	
}

func (t1Ctxt *emrTier1Context) readRegisterTenants() {
	//t1Ctxt.createTopicKafka(kafkaLstnrTopic1)

	t1Ctxt.log.Debugf("Registering Tenants")
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisSvr1,
		Password: "sOmE_sEcUrE_pAsS",
		DB:       1,
	})
	smembers := redisClient.SMembers("tenants")
	redisClient.Close()
	if smembers.Err() != nil {
		t1Ctxt.log.Debugf("Error reading topics in redis")
	} else {
		for _, v := range smembers.Val() {
			t1Ctxt.readRegisterPatientFromTenant(v)
		}
	}
}

func (t1Ctxt *emrTier1Context) readRegisterPatientFromTenant(uuidTenant string) {

	t1Ctxt.log.Debugf("Registering Tenant %s",uuidTenant )
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisSvr1,
		Password: "sOmE_sEcUrE_pAsS",
		DB:       1,
	})

	smembers := redisClient.SMembers(uuidTenant)
	redisClient.Close()
	if smembers.Err() != nil {
		t1Ctxt.log.Debugf("Error reading topics in redis")
	} else {
		t1Ctxt.createTopicBatchKafka(smembers.Val())
		for _, v := range smembers.Val() {
			t1Ctxt.patientTopicRegisterListenerSpawn(uuidTenant, strings.TrimSpace(v))
		}
	}
}

func (t1Ctxt *emrTier1Context) createTopicBatchKafka(patientUUIDList []string) {

	brokerAddrs := []string{broker1Address}
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin(brokerAddrs, config)
	
	for err != nil { 
		log.Println("Error while creating cluster admin ...sleeping: ", err.Error())
		time.Sleep(5 * time.Second)
		admin, err = sarama.NewClusterAdmin(brokerAddrs, config)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisSvr1,
		Password: "sOmE_sEcUrE_pAsS",
		DB:       1,
	})

	retention := retentionKafkaTopic

	for _, v := range patientUUIDList {
		err = admin.CreateTopic(strings.TrimSpace(v), &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: map[string]*string{
				"retention.ms": &retention,
			},
		}, false)

		if err != nil {
			t1Ctxt.log.Debugf("Error while creating topic %s : %s\n", v, err.Error())
		}

		err = redisClient.SAdd("topics", strings.TrimSpace(v)).Err()
		if err != nil {
			t1Ctxt.log.Debugf("Failed to do Set Topic %s .. %s", v, err)
		}
	}

	fmt.Println("Done topic batch creation")
	redisClient.Close()

	admin.Close()
}

func (t1Ctxt *emrTier1Context) createTopicKafka(tenantUUID string, patientUUID string) {

	brokerAddrs := []string{broker1Address}
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin(brokerAddrs, config)
	if err != nil {
		log.Println("Error while creating cluster admin: ", err.Error())
	}

	//defer func() { _ = admin.Close() }()
	err = admin.CreateTopic(patientUUID, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)

	if err != nil {
		log.Println("Error while creating topic: ", err.Error())
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisSvr1,
		Password: "sOmE_sEcUrE_pAsS",
		DB:       1,
	})

	t1Ctxt.log.Debugf("[createTopicKafka] Adding to tenant %s to tenants in Redis", tenantUUID )
	err = redisClient.SAdd("tenants", tenantUUID).Err()
	if err != nil {
		t1Ctxt.log.Debugf("Failed to do Set %s .. %s", err, patientUUID)
	}

	t1Ctxt.log.Debugf("[createTopicKafka] Adding to tenant %s to tenants in Redis again ", tenantUUID )
	err = redisClient.SAdd("tenants", tenantUUID).Err()
	if err != nil {
		t1Ctxt.log.Debugf("Failed to do Set %s .. %s", err, patientUUID)
	}

	t1Ctxt.log.Debugf("[createTopicKafka] Adding on tenant %s to patient %s in Redis", tenantUUID, patientUUID)
	err = redisClient.SAdd(tenantUUID, patientUUID).Err()
	if err != nil {
		t1Ctxt.log.Debugf("Failed to do Set %s .. %s", err, patientUUID)
	}


	t1Ctxt.log.Debugf("[createTopicKafka] Adding on tenant %s to patient %s in Redis .. again", tenantUUID, patientUUID)
	err = redisClient.SAdd(tenantUUID, patientUUID).Err()
	if err != nil {
		t1Ctxt.log.Debugf("Failed to do Set %s .. %s", err, patientUUID)
	}


	fmt.Println("Done topic creation")
	redisClient.Close()

	_ = admin.Close()
}



func (t1Ctxt *emrTier1Context) deleteTopicKafka(patientUUID string) {

	brokerAddrs := []string{broker1Address}
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin(brokerAddrs, config)
	if err != nil {
		t1Ctxt.log.Debugf("Error while creating cluster admin: ", err.Error())
	}

	err = admin.DeleteTopic(patientUUID)
	if err != nil {
		t1Ctxt.log.Debugf("Error while deleting topic: ", err.Error())
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisSvr1,
		Password: "sOmE_sEcUrE_pAsS",
		DB:       1,
	})

	err = redisClient.SRem("topics", patientUUID).Err()
	if err != nil {
		t1Ctxt.log.Debugf("Failed to do Set %s .. %s", err, patientUUID)
	}

	fmt.Println("Done topic delete")
	redisClient.Close()

	_ = admin.Close()
}

func (t1Ctxt *emrTier1Context) writeEcgToInflux(patientUUID string, spo2 SPO2, hrMax int, hrMin int, motion int) {

	recvdEcg := spo2.Data.Extras
	// create new client with default option for server url authenticate by token
	client := influxdb2.NewClient(influxdbSvr, "my-token")
	// user blocking write client for writes to desired bucket
	writeAPI := client.WriteAPIBlocking("", influxdbDatabase)
	// create point using full params constructor

	measrmnt := patientUUID + "_ecg"
	recordTime := time.Unix(int64(recvdEcg.RecordTime/1000), 0)
	//for _, v := range spo2.Ecg {

	// create point using fluent style
	p := influxdb2.NewPointWithMeasurement(measrmnt).
		AddField("ecg", arrayToString(recvdEcg.Ecg, ",")).
		AddField("hr", int32(recvdEcg.Hr)).
		AddField("hrMax", int32(hrMax)).
		AddField("hrMin", int32(hrMin)).
		AddField("AvgRR", int32(recvdEcg.AvgRR)).
		AddField("motion", int32(motion)).
		SetTime(recordTime)

	err := writeAPI.WritePoint(context.Background(), p)
	if err != nil {
		t1Ctxt.log.Errorf("Patient %s, %v",patientUUID, err)
	}
	// Ensures background processes finish
	client.Close()
}

func (t1Ctxt *emrTier1Context) writeSpo2ToInflux(patientUUID string, spo2 SPO2) {

	/*
	  // Create HTTP client
	  httpClient := &http.Client{
        Timeout: time.Second * time.Duration(60),
        Transport: &http.Transport{
            DialContext: (&net.Dialer{
                Timeout: 5 * time.Second,
            }).DialContext,
            TLSHandshakeTimeout: 5 * time.Second,
            TLSClientConfig: &tls.Config{
                InsecureSkipVerify: true,
            },
            MaxIdleConns:        100,
            MaxIdleConnsPerHost: 100,
            IdleConnTimeout:     90 * time.Second,
        },
    }
	client1 := influxdb2.NewClientWithOptions("https://10.0.0.4:8086", "my-token", influxdb2.DefaultOptions().SetHTTPClient(httpClient))
*/
	// create new client with default option for server url authenticate by token
	client := influxdb2.NewClient(influxdbSvr, "my-token")
	// user blocking write client for writes to desired bucket
	writeAPI := client.WriteAPIBlocking("", "emr_dev/90days")
	// create point using full params constructor

	measrmnt := patientUUID + "_spo2"
	recordTime := time.Unix(int64(spo2.Time/1000), 0)
	// create point using fluent style
	p := influxdb2.NewPointWithMeasurement(measrmnt).
		AddField("spo2", int32(spo2.SPo2)).
		AddField("pi", spo2.Pi).
		AddField("pr", int32(spo2.Pr)).
		SetTime(recordTime)
	writeAPI.WritePoint(context.Background(), p)

	// Ensures background processes finish
	defer client.Close()
}

func (t1Ctxt *emrTier1Context) sendAlert(sev string, resource string, event string, origin string, valstr string, createTime uint64, motion int) {

		t := time.Unix(int64(createTime), 0)
		createTimeStr := t.Format(time.RFC3339)
		tmp := createTimeStr[:len(createTimeStr)-1] + ".000Z"

		//valstr := fmt.Sprintf("%v_%v",value, motion);

		evStr := fmt.Sprintf("%s_%d",event, ((createTime>>8)<<8));  // Dedupe alerts for about 5-6 minutes

		res2D := &Alertjson{
			Environment: "Production",
			Event:       evStr,
			Group:       event,//subtype
			Resource:    resource,//pid
			Severity:    sev,
			Origin:		 origin,//type
			Service:     []string{"emr"},
			CreateTime:  tmp,
			Value:       valstr}

		postBody, _ := json.Marshal(res2D)
		responseBody := bytes.NewBuffer(postBody)

		t1Ctxt.log.Infof("Sending alert for %s event %s", resource, event)
		t1Ctxt.startGoRoutine(func() {
			retry_again := 10
			ret := false
			idx := 0
			for retry_again > 0 {
				idx = (rand.Intn(40-1) + 1 )
				select {
				case t1Ctxt.alertChannel[idx] <- responseBody:
					ret = true	
					retry_again = 0
				case <- time.After(1*time.Second):
					ret = false
					retry_again--
				}
			}
			postBody = nil
			res2D = nil
			if ret == false {
				t1Ctxt.log.Errorf("Failed to enq %d : %v", idx, responseBody )
			}
		})
		res2D = nil
		postBody = nil
}

func (t1Ctxt *emrTier1Context) sendToBaseliner(baselineData *pb.BaselinesPatient) {

	//t1Ctxt.log.Infof("Sending to baseliner %v ", baselineData)
	//t1Ctxt.startGoRoutine(func() {
		retry_again := 5
		ret := false
		idx := 0
		for retry_again > 0 {
			idx = (rand.Intn(20-1) + 1 )
			select {
			case t1Ctxt.baselineChannel[idx] <- baselineData:
				ret = true	
				retry_again = 0
			case <- time.After(500*time.Millisecond):
				ret = false
				retry_again--
			}
		}
		if ret == false {
			t1Ctxt.log.Errorf("Failed to enq %d : %v", idx, baselineData )
		}
	//})
}


func (t1Ctxt *emrTier1Context) patientTopicRegisterListenerSpawn(tenantUUID string, patientUUID string) {
	t1Ctxt.startGoRoutine(func() {
		if t1Ctxt.patientMapFind(patientUUID) == nil {
			t1Ctxt.patientMapInsert(patientUUID)
			
			t1Ctxt.patientTopicRegisterListen(tenantUUID, patientUUID)
			
		}
		t1Ctxt.wg.Done()
	})
}
