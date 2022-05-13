package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	//"net"
	"os"
	"strconv"
	"sync"
	"time"
	"strings"
	//pb "github.com/rajsreen/sensor_producer_sim/emr_consumer_proto"

	//pb "../proto"
	"net/http"
	"bytes"
	"github.com/go-redis/redis"
	"github.com/segmentio/kafka-go"
	"encoding/json"

	//"google.golang.org/grpc"
)

var sim int
const (
	EcgSim =iota
	Spo2Sim
	TempSim
)
// the topic and broker address are initialized as constants
const (
	topic          = "ec6f195d-c86e-4d2c-b709-1b2a61ab75c1"
	broker1Address = "kafkaBroker1:9092"
	apigw          = "http://vpn.live247.ai:7160/liveapi/gateway/push_data"
)

type emrTier1Context struct {
	wg sync.WaitGroup
}

// EmrT1Server gRPC server helper
type EmrT1Server struct {
	t1Ctxt *emrTier1Context
}


type accel struct {
	Xaxis int16  `json:"x,omitempty"`
	Yaxis int16  `json:"y,omitempty"`
	Zaxis int16  `json:"z,omitempty"`
} 



type SPO2 struct {
	Rssi				int8   `json:"RSSI,omitempty"`
	TemperatureBattery	uint8   `json:"batteryPercent,omitempty"`
	DisplayTemperature	float32   `json:"displayTemperature,omitempty"`
	TemperatureFw		string   `json:"fw,omitempty"`
	TemperatureMac		string   `json:"mac,omitempty"`
	RawTemperature		float32   `json:"rawTemperature,omitempty"`
	TemperatureStatus	string   `json:"temperatureStatus,omitempty"`

	Pi             float32 `json:"pi,omitempty"`
	Time           uint64  `json:time,omitempty"`
	SPo2           uint8   `json:spo2,omitempty"`
	Pr             uint8   `json:pr,omitempty"`
	Steps          uint8   `json:steps,omitempty"`
	Flash          uint8    `json:flash,omitempty"`
	Battery        uint8   `json:"battery,omitempty"`
	ChargingStatus uint8   `json:"chargingStatus,omitempty"`
	Data           struct {
		DeviceID    string `json:"deviceID"`
		DeviceModel string `json:"deviceModel"`
		DeviceName string `json:deviceName,omitempty"`
		DeviceSN    string `json:"deviceSN"`
		Extras      struct {
			RecordTime     uint64  `json:"time,omitempty"`
			LeadOn         bool 	`json:"leadOn,omitempty"`
			Activity      bool     `json:"activity,omitempty"`
			Hr            uint16    `json:"HR,omitempty"`
			Rri           []int16   `json:"rri,omitempty"`
			Rwl           []int16   `json:"rwl,omitempty"`
			Acc           []accel     `json:"acc,omitempty"`
			Ecg           []int32   `json:"ecg,omitempty"`
			Magnification uint16    `json:"magnification,omitempty"`
			EcgFrequency  uint16    `json:"ecgFrequency,omitempty"`
			AccAccuracy   uint16    `json:"accAccuracy,omitempty"`
			AccFrequency  uint16    `json:"accFrequency,omitempty"`
			Protocol      string    `json:"protocol,omitempty"`
			HwVer         string    `json:"hwVer,omitempty"`
			FwVer         string    `json:"fwVer,omitempty"`
			Flash          bool    `json:"flash,omitempty"`
			ReceiveTime    uint64  `json:receiveTime,omitempty"`
			Battery        uint8   `json:"battery,omitempty"`
			Rr            float32   `json:"RR,omitempty"`
			AvgRR          float32   `json:"avRR,omitempty"`
			EcgInMillivolt  []float32   `json:"ecgInMillivolt,omitempty"`



			Pi             float32 `json:"pi,omitempty"`
			Spo2           uint8   `json:"spo2,omitempty"`
			Pr             uint8   `json:"pr,omitempty"`
			Steps          uint8   `json:"steps,omitempty"`
			ChargingStatus uint8   `json:"chargingStatus,omitempty"`
		} `json:"extras,omitempty"`
		Id         uint8  `json:"id"`
		RecordTimeData uint64 `json:"time"`
	} `json:"data,omitempty"`
	DeviceType  string `json:"deviceType,omitempty"`
	PatientUUID string `json:"patientUUID,omitempty"`
	GwBattery        uint8   `json:"gwBattery,omitempty"`
}


type SPO2_old struct {
	Temp             float32 `json:"temp,omitempty"`
	Pi             float32 `json:"pi,omitempty"`
	Time           uint64  `json:time,omitempty"`
	SPo2           uint8   `json:spo2,omitempty"`
	Pr             uint8   `json:pr,omitempty"`
	Steps          uint8   `json:steps,omitempty"`
	Flash          uint8    `json:flash,omitempty"`
	Battery        uint8   `json:"battery,omitempty"`
	ChargingStatus uint8   `json:"chargingStatus,omitempty"`
	Data           struct {
		DeviceID    string `json:"deviceID"`
		DeviceModel string `json:"deviceModel"`
		DeviceSN    string `json:"deviceSN"`
		Extras      struct {
			RecordTime     uint64  `json:time"`
			ReceiveTime    uint64  `json:receiveTime"`
			Flash          bool    `json:"flash"`
			Pi             float32 `json:"pi"`
			Spo2           uint8   `json:"spo2"`
			Pr             uint8   `json:"pr"`
			Steps          uint8   `json:"steps"`
			Battery        uint8   `json:"battery"`
			ChargingStatus uint8   `json:"chargingStatus"`
		}
		Id         uint8  `json:"id"`
		RecordTime uint64 `json:time"`
	} `json:"data,omitempty"`
	DeviceType  string `json:"deviceType,omitempty"`
	PatientUUID string `json:"patientUUID,omitempty"`

	DeviceId   string `json:"deviceId,omitempty"`
	DeviceSN   string `json:deviceSN,omitempty"`
	DeviceName string `json:deviceName,omitempty"`
	//DeviceType    string    `json:deviceType,omitempty"`
	RecordTime  uint64 `json:recordTime,omitempty"`
	ReceiveTime uint64 `json:receiveTime,omitempty"`
	LeadOn      uint8  `json:"leadOn,omitempty"`
	//Flash         uint8     `json:"flash,omitempty"`
	Activity      uint8     `json:"activity,omitempty"`
	Magnification uint16    `json:"magnification,omitempty"`
	Acc           [][]int16 `json:"acc,omitempty"`
	Ecg           []int32   `json:"ecg,omitempty"`
	Rri           []int16   `json:"rri,omitempty"`
	Rwl           []int16   `json:"rwl,omitempty"`
	Rr            float32   `json:"rr,omitempty"`
	Hr            uint16    `json:"hr,omitempty"`
	AvgRR          float32   `json:"avRR,omitempty"`
	AccAccuracy   uint16    `json:"accAccuracy,omitempty"`
	AccFrequency  uint16    `json:"accFrequency,omitempty"`
	Sf            uint16    `json:"sf,omitempty"`
}

// NewEmrT1Server - returns the pointer to the implementation.
func NewEmrT1Server() *EmrT1Server {
	return &EmrT1Server{}
}


func loopProduce(patientUUID string) {
	localUUID := patientUUID
	if localUUID == "patient098c6e4c-78cb-4349-aa51-27d1b8702f2a" {
		return
	}

	for {
		produce(localUUID)
	}
}



const (
	kafkaPatientDiscovery = "patientDiscovery"
)


type patientDiscover struct {

	UuidPatient    	string `json:"uuidPatient"`
	Method 		string `json:"method"`
	Bps 		int32 `json:"bps,omitempty"`
	Bpd 		int32 `json:"bps,omitempty"`
	Sugar 		int32 `json:"sugar,omitempty"`
	Spo2Threshold 		int32 `json:"spo2Threshold,omitempty"`
	TemperatureThreshold 		int32 `json:"temperatureThreshold,omitempty"`
	PrThreshold 		int32 `json:"prThreshold,omitempty"`
	PiThreshold 		int32 `json:"piThreshold,omitempty"`
}

func listenOnPatientDiscoveryChannel() {

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address},
		Topic:   kafkaPatientDiscovery,
		GroupID: kafkaPatientDiscovery,
	})
	defer  kafkaReader.Close()

	var msg kafka.Message
	var err error
	patientUUID := ""
	var patientDiscoverMsg patientDiscover

	for {
		msg, err = kafkaReader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("[ListeningTopic] Sleeping for 10s %s , err %s", kafkaPatientDiscovery, err)
			go listenOnPatientDiscoveryChannel()
			time.Sleep(1 * time.Second)
			break
		}
		err = json.Unmarshal(msg.Value, &patientDiscoverMsg)

		patientUUID = strings.TrimSpace(patientDiscoverMsg.UuidPatient)
		fmt.Println("received patient %s",patientUUID)
		if patientDiscoverMsg.Method == "CREATE" {

			fmt.Println("Create Patient] Entered %s", patientUUID)
			go loopProduce(patientUUID)
		}
	}		
}

func sendHttpReq (client *http.Client, payload *bytes.Buffer) {
	//fmt.Println("@rajsreen %v", payload)

	req, _ :=  http.NewRequest("POST", apigw, payload)
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	res,_ := client.Do(req)
	defer res.Body.Close()
	req = nil
	
}


func produce(patientUUID string) {
	// initialize a counter
	i := 0

	patientUUID = strings.TrimSpace(patientUUID)
	ctx := context.Background()
	// intialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address},
		Topic:   patientUUID,
	})
	defer w.Close()

	filename := "/root/ecg_vector.txt"
	if sim == Spo2Sim {
		filename = "/root/o2_vector.txt"
	} else if sim == TempSim {
		filename = "/root/temperature.txt"
	}
	fmt.Println("Starting Production on ", patientUUID, filename)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	
	/*
	var spo2 SPO2_old
	payload := new(bytes.Buffer)
	client 	 :=  &http.Client{}
	*/

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		/*
		err = json.Unmarshal([]byte(scanner.Text()), &spo2)
		spo2.PatientUUID = patientUUID
		json.NewEncoder(payload).Encode(spo2)
		//payload, _ = json.Marshal(spo2)
		sendHttpReq(client, payload)
		if sim == Spo2Sim {
			time.Sleep(10 * time.Second)
		} else if sim == TempSim {
			time.Sleep(10 * time.Second)
		}
		// sleep for a second
		time.Sleep(1000 * time.Millisecond)
		continue
		*/

		// each kafka message has a key and value. The key is used
		// to decide which partition (and consequently, which broker)
		// the message gets published on
		//fmt.Println("msg " + scanner.Text())
		err = w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			// create an arbitrary message payload for the value
			Value: []byte(scanner.Text()),
		})
		if err != nil {
			fmt.Println("could not write message " + err.Error() + patientUUID)
		}

		// log a confirmation once the message is written
		//fmt.Println("writes:", i)
		i++

		if sim == Spo2Sim {
			time.Sleep(15 * time.Second)
		} else if sim == TempSim {
			time.Sleep(40 * time.Second)
		}
		// sleep for a second
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("Done loop for %s", patientUUID)
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func batchReadProducer() {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "redisSvr1:6379",
		Password:  "sOmE_sEcUrE_pAsS",
		DB:       1,
	})
	smembers := redisClient.SMembers("topics")
	redisClient.Close()
	if smembers.Err() != nil {
		fmt.Println("Error reading topics in redis")
	} else {
		fmt.Println("batch reading ")

		for _, v := range smembers.Val() {
			go loopProduce(v)
			time.Sleep(3 * time.Second)
		}
	}

	go listenOnPatientDiscoveryChannel()
}

/*
// RegisterPatient : API for registration of Patient UUID and kafka registration
func (s *EmrT1Server) RegisterPatient(context context.Context, in *pb.PatientRegister) (*pb.PatientRegisterRsp, error) {
	patientUUID := *in.PatientUUID

	fmt.Println("Starting Simulator for ", patientUUID)
	go produce(patientUUID)
	// Need to spwan off the topic as a kafka consumer and complete registration

	rsp := true
	return &pb.PatientRegisterRsp{Rsp: &rsp}, nil

}

func grpcServerSetup() error {
	var s *grpc.Server

	l, err := net.Listen("tcp", "127.0.0.1:7778")
	if err != nil {
		fmt.Println("failed to listen: %v", err)
		return err
	}

	fmt.Println("Starting grpc server ")

	s = grpc.NewServer()

	osSrvr := NewEmrT1Server()
	pb.RegisterEmrT1RpcServer(s, osSrvr)
	fmt.Println("Registered grpc server ")

	if err := s.Serve(l); err != nil {
		fmt.Println("failed to serve: %v", err)
		s = nil
		l = nil
	}
	return err
}
*/
func main() {
	// create a new contex
	// produce messages in a new go routine, since
	// both the produce and consume functions are
	// blocking


	argLength := len(os.Args[1:])  
	fmt.Printf("Arg length is %d\n", argLength)

	for i, a := range os.Args[1:] {
		fmt.Printf("Arg %d is %s\n", i+1, a) 
	}

	if os.Args[1] == "ecg" {
		sim = EcgSim
	} else if os.Args[1] == "spo2" {
		sim = Spo2Sim
	} else {
		sim = TempSim
	}
	var t1Ctxt emrTier1Context
	batchReadProducer()
	//grpcServerSetup()

	t1Ctxt.wg.Wait()
	messages := make(chan string)
    msg := <-messages
    fmt.Println(msg)

	fmt.Println("DONE")
}
