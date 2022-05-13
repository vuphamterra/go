package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"

	"net/http"
	"strings"
	"time"
	"net"

    deepAnalysispb "sensor_consumer_svc/proto_out/deep_analyser"
	pb "sensor_consumer_svc/proto_out/emr_consumer_proto"

	"github.com/go-redis/redis"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/segmentio/kafka-go"
	"gonum.org/v1/gonum/stat"
	"google.golang.org/grpc"

)

const (
	useCurTimeForSimulation = true
)

const (
	MotionHistory = 100
)
const (
	Motion = iota
	Standing
	Bending
	Sleeping
	Slouching
)

const (
	Spo2AlertClear = iota
	Spo2AlertSev2
	Spo2AlertSev1
)

const (
	TemperatureAlertClear = iota
	TemperatureAlertSev2
	TemperatureAlertSev1
)

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

	Bpd 			uint16   `json:"bpd,omitempty"`
	Bps				uint16   `json:"bps,omitempty"`
	Weight 			uint16   `json:"weight,omitempty"`
	
	/*****ManualENtry*********/
	VitalData struct {
		Height      uint16    `json:"height,omitempty"`
		weight      uint16    `json:"weight,omitempty"`
		spo2		uint16    `json:"spo2,omitempty"`
		Pulse		uint16    `json:"pulse,omitempty"`
		Rr			uint16    `json:"rr,omitempty"`
		Bpd			uint16    `json:"bpd,omitempty"`
		Bps			uint16    `json:"bps,omitempty"`
		PainIdx		uint16    `json:"painIdx,omitempty"`

	}	`json:"vitalData,omitempty"`
	/*************************/

	/*****BP Specifics*********/
	CurrentCount	uint16   `json:"currentCount,omitempty"`
	Progress 		uint8   `json:"progress,omitempty"`
	TotalCount		uint16   `json:"totalCount,omitempty"`
	/*************************/
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
			
			// BP Specifics
			DataID			string    `json:"dataID,omitempty"`
			Sys			uint8    `json:"sys,omitempty"`
			Dia			uint8    `json:"dia,omitempty"`
			HeartRate	uint8    `json:"heartRate,omitempty"`
			HsdValue	bool    `json:"hsdValue,omitempty"`
			Arrhythmia	bool    `json:"arrhythmia,omitempty"`
			
		} `json:"extras,omitempty"`
		Id         uint8  `json:"id"`
		RecordTimeData uint64 `json:"time"`
	} `json:"data,omitempty"`
	DeviceType  string `json:"deviceType,omitempty"`
	PatientUUID string `json:"patientUUID,omitempty"`
	GwBattery        uint8   `json:"gwBattery,omitempty"`
}

/**************************************************************/
/**************************************************************/
func (t1Ctxt *emrTier1Context) sendEcgToDeepAnalyser(deepAnalyserconn *grpc.ClientConn, patientUUID string, currTime int64 , ecgData []int32,
								hr int32, rr int32, spo2 int32, pr int32 , pi float32, temperature float32, motion int32 ) {
	c := deepAnalysispb.NewDeepAnalyserRpcClient(deepAnalyserconn)

	if c == nil {
		deepAnalyserconn, _ = grpc.Dial(deepLearningEngineAddress, grpc.WithInsecure(), grpc.WithBlock())
		if c == nil {
			t1Ctxt.log.Errorf("Unable to connect to Deep Learning Engine %s ", patientUUID)
		return
	}
	}

	outEcgVector := new(deepAnalysispb.EcgVector)

	ctx, _ := context.WithTimeout(context.Background(), 5000 * time.Millisecond)
	if ctx == nil {
		return
	}

	var rriTemp []int32
	rriTemp = append(rriTemp,1)
	
	outEcgVector.PatientUUID = &patientUUID
	outEcgVector.CurrTime =  &currTime
	outEcgVector.EcgVal = ecgData
	outEcgVector.Hr 	= &hr
	outEcgVector.Rr 	= &rr
	outEcgVector.Spo2 	= &spo2
	outEcgVector.Pr		= &pr
	outEcgVector.Pi		= &pi
	outEcgVector.Temperature = &temperature
	outEcgVector.Rri = rriTemp
	outEcgVector.Motion	= &motion
	
	if _, e := c.ProcessEcg(ctx, outEcgVector); e != nil {
		t1Ctxt.log.Errorf("Was not able to insert %s : %v", patientUUID , e)
	}
	outEcgVector = nil
}

	
func arrayToString(a []int32, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
	//return strings.Trim(strings.Join(strings.Split(fmt.Sprint(a), " "), delim), "[]")
	//return strings.Trim(strings.Join(strings.Fields(fmt.Sprint(a)), delim), "[]")
}



type Alertjson struct {
	Environment string   `json:"environment"`
	Event       string   `json:"event"`
	Group       string   `json:"group"`
	Origin      string   `json:"origin"`
	Resource    string   `json:"resource"`
	Severity    string   `json:"severity"`
	Service     []string `json:"service"`
	CreateTime  string   `json:"createTime"`
	Value       string   `json:"value"`
}

func (t1Ctxt *emrTier1Context) motionGuidanceNew(accelrtn []accel,windowdAccelXAxis []float64,
	windowdAccelYAxis []float64, windowdAccelZAxis []float64 ) int {

	
	//t1Ctxt.log.Debugf("%v ",windowdAccelXAxis)
	//t1Ctxt.log.Debugf("YYYY %v ",windowdAccelYAxis)
	//t1Ctxt.log.Debugf("ZZZZ %v ",windowdAccelZAxis)




	retMotion := 0

	xvar := stat.StdDev(windowdAccelXAxis,nil)
	yvar := stat.StdDev(windowdAccelYAxis, nil)
	zvar := stat.StdDev(windowdAccelZAxis, nil)

	//t1Ctxt.log.Debugf("variance ...%d %d %d\n",xvar,yvar,zvar)
	if xvar > 40 || yvar > 40 || zvar > 40 {
		//t1Ctxt.log.Debugf("variance ...%d %d %d\n",xvar,yvar,zvar)
		return 0
	}
	

	maxIndex := "x"
	max := int16(0)
	if accelrtn[0].Xaxis > max {
		maxIndex = "x"
		max = accelrtn[0].Xaxis
	}
	if accelrtn[0].Yaxis > max {
		maxIndex = "y"
		max = accelrtn[0].Yaxis
	}
	if accelrtn[0].Zaxis > max {
		maxIndex = "z"
		max = accelrtn[0].Zaxis
	}


	if maxIndex == "x" {
		retMotion = 2 //BENDING
	} else if maxIndex == "y" {
		retMotion = 1 //STANDING
	} else {
		retMotion = 3 //SLEEPING
	}
	return retMotion	

}

func diff(a, b int16) int16 {
	if a < b {
	   return b - a
	}
	return a - b
 }


func (t1Ctxt *emrTier1Context) checkSpo2Alert(patientUUID string, spo2AlertState int, spo2Val int32, rcvTimeSec uint64) (int){
	alrtState := 0
	switch {
	case spo2Val > 95 && spo2AlertState != Spo2AlertClear:
		alrtState = Spo2AlertClear
			valstr := fmt.Sprintf("SPO2:%d",spo2Val);
			t1Ctxt.sendAlert("normal", patientUUID, "SPO2_BACK_NORMAL", "SPO2",valstr, rcvTimeSec, int(spo2Val))
		
	case spo2Val < 90 && spo2AlertState != Spo2AlertSev1:
	
		alrtState = Spo2AlertSev1
		valstr := fmt.Sprintf("SPO2:%d",spo2Val);
		t1Ctxt.sendAlert("major", patientUUID, "SPO2_VERY_LOW", "SPO2", valstr, rcvTimeSec, int(spo2Val))
		
	case spo2Val >= 90 && spo2Val < 94 && spo2AlertState != Spo2AlertSev2:
		alrtState = Spo2AlertSev1
		valstr := fmt.Sprintf("SPO2:%d", spo2Val);
		t1Ctxt.sendAlert("major", patientUUID, "SPO2_LOW", "SPO2", valstr, rcvTimeSec, int(spo2Val))
	default:
	}
	return alrtState
}

func (t1Ctxt *emrTier1Context) patientTopicRegisterListen(tenantUUID string, patientUUID string) {

	t1Ctxt.log.Debugf("[RegisterTopic] Listening on Tenant %s , Patient %s",tenantUUID, patientUUID)
	timeCnt := 0
	var hrMax int32 = 0
	var hrMin uint16 = 0
	var motion int = 0
	var cached_motion int = 0
	var cachedHr int32 = 999
	var cachedAvgRR int32 = 999
	var cachedSystolic int32 = 999
	var cachedDiastolic int32 = 999
	var cachedWeight float32 = 999
	
	//spo2 sensor specifics
	//var windowdSpo2 []float64 = nil
	var cachedSpo2 int32 = 999
	var cachedPr int32 = 999
	var cachedPi float32 = 999
	spo2AlertState := Spo2AlertClear
	prevRRi := int16(0)
	//var meanSpo2  int = 0
	
	var spO2HourlyBaselineAvg int32 = 0
	var prHourlyBaselineAvg int32 = 0
	var piHourlyBaselineAvg float32 = 0
	var spO2HourlyBaselineMin int32 = 0
	var prHourlyBaselineMin int32 = 0
	var piHourlyBaselineMin float32 = 0
	var spO2HourlyBaselineMax int32 = 0
	var prHourlyBaselineMax int32 = 0
	var piHourlyBaselineMax float32 = 0
	var spo2HourlyNumsamples int32 = 0
	
	var spO2HourlyBaselineAvgSend int32 = 0
	var prHourlyBaselineAvgSend int32 = 0
	var piHourlyBaselineAvgSend float32 = 0
	var spO2HourlyBaselineMinSend int32 = 0
	var prHourlyBaselineMinSend int32 = 0
	var piHourlyBaselineMinSend float32 = 0
	var spO2HourlyBaselineMaxSend int32 = 0
	var prHourlyBaselineMaxSend int32 = 0
	var piHourlyBaselineMaxSend float32 = 0	


	//battery status
	var batterySpo2 int32 		 	= 0
	var batteryEcg int32  			= 0
	var batteryTemperature 	int32  	= 0
	var batteryBP			int32	= 0
	var batteryGateway int32     	= 0
	var livebatterySpo2 int32 		 	= 0
	var livebatteryEcg int32  			= 0
	var livebatteryTemperature int32  	= 0
	//var livebatteryBP 			int32	= 0
	var livebatteryGateway int32     	= 0

	//temperature sensor specifics
	var windowdTemperature []float64 = nil
	TemperatureAlertState := TemperatureAlertClear
	var meanTemperature  float32 = 0	
	var cachedTemp float32 = 999
	var temperatureHourlyNumsamples uint16 = 0
	var temperatureHourlyBaselineAvg float32 = 0
	var temperatureHourlyBaselineMin float32 = 0
	var temperatureHourlyBaselineMax float32 = 0	
	var temperatureHourlyBaselineAvgSend float32 = 0
	var temperatureHourlyBaselineMinSend float32 = 0
	var temperatureHourlyBaselineMaxSend float32 = 0
	var agg60SecSampleRcvd int = 0
	var agg10SecSampleRcvd int = 0


	var baselineSleepingNumSamples int = 0
	var baselineSleepingStart uint64 = 0
	var baselineStandingNumSamples int = 0
	var baselineStandingStart uint64 = 0

	
	rcvTime := time.Now()
	rcvTimeSec := uint64(rcvTime.Unix())
	agg60SecSampleSendTimeSec := rcvTimeSec
	agg10SecSampleSendTimeSec := rcvTimeSec

	patchRecordTime  := rcvTimeSec
	windowedBaselineWrite := rcvTimeSec
	
	
	/******************************/
	// Hourly counts Hr/RR
	
	var hrHourlySleepingBaselineAvg uint32 = 0
	var avgRRHourlySleepingBaselineAvg uint32 = 0
	var hrHourlySleepingBaselineMax uint16 = 0
	var avgRRHourlySleepingBaselineMax uint16 = 0
	var hrHourlySleepingBaselineMin uint16 = 0
	var avgRRHourlySleepingBaselineMin uint16 = 0
	
	
	var hrHourlyStandingBaselineAvg uint32 = 0
	var avgRRHourlyStandingBaselineAvg uint32 = 0
	var hrHourlyStandingBaselineMax uint16 = 0
	var avgRRHourlyStandingBaselineMax uint16 = 0
	var hrHourlyStandingBaselineMin uint16 = 0
	var avgRRHourlyStandingBaselineMin uint16 = 0
	
	
	var hrHourlyStandingBaselineMaxInt int32 =0
	var hrHourlyStandingBaselineMinInt  int32 =0
	var hrHourlyStandingBaselineAvgInt  int32 =0
	var avgRRHourlyStandingBaselineMaxInt  int32 =0
	var avgRRHourlyStandingBaselineMinInt  int32 =0
	var avgRRHourlyStandingBaselineAvgInt  int32 =0


	var liveSendhrMax int32
	var liveSendRrMax int32
	var liveSendSpo2Max int32
	var liveSendPrMax int32
	var liveSendPiMax float32
	var liveSendTemperatureMax float32
	var liveSendBPSystolicMax int32
	var liveSendBPDiastolicMax int32
	var liveSendWeight float32

	var pInfo *PatientData
	var sugar int32


	// baselines
	outBaselines := new(pb.BaselinesPatient)
	outBaselinesHr := new(pb.BaselinesHr)
	outBaselinesRr := new(pb.BaselinesRr)
	outBaselinesPr := new(pb.BaselinesPr)
	outBaselinesSpo2 := new(pb.BaselinesSpo2)
	outBaselinesBP 		:= new(pb.BaselinesBP)
	outBaselinesPi := new(pb.BaselinesPi)
	outBaselinesTemperature := new(pb.BaselinesTemperature)
	baselnOpCurrent :=  pb.BaselineOperation_BSLN_OP_CURRENT
	baselnOpAggregate :=  pb.BaselineOperation_BSLN_OP_AGGREGATE
	zeroCacheInt := int32(0)
	zeroCacheFloat := float32(0)


	valstr := ""
	measrmnt := ""
	recordTime :=time.Unix(int64(0), 0)

	preferStanding := false


	//////////////////////////////
	// SDK FLIP
	var newsdk int = 1
	var spo2 SPO2
	recvdEcg := spo2.Data.Extras
	vitals := spo2.VitalData

	
	//var spo2 SPO2_old
	//var recvdEcg SPO2_old
	//////////////////////////////
	
	windowdAccelXAxis := make([]float64, 0, MotionHistory)
	windowdAccelYAxis := make([]float64, 0, MotionHistory)
	windowdAccelZAxis := make([]float64, 0, MotionHistory)

	spo2WritePendingCnt := 0
	temperatureWritePendingCnt := 0

	
	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address},
		Topic:   patientUUID,
		GroupID: patientUUID+"-grp",
		RetentionTime: 300 *time.Second,
		CommitInterval: 60 *time.Second,
	})
	defer  kafkaReader.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisSvr1,
		Password: "sOmE_sEcUrE_pAsS",
		DB:       0,
	})
	defer redisClient.Close()



	// ***************************************** //
	// Create HTTP client for influxdb1
	// ***************************************** //
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
	client1 := influxdb2.NewClientWithOptions(influxdbSvr, t1Ctxt.influxAuthToken, influxdb2.DefaultOptions().SetHTTPClient(httpClientInflux))
	writeAPI := client1.WriteAPIBlocking("test_org", influxdbDatabase)
	defer client1.Close()

	//just for initialization
	p := influxdb2.NewPointWithMeasurement("test").
				SetTime(recordTime)
	// ***************************************** //


	// ***************************************** //
	// gRPC towards deepAnalyser                 //
	deepAnalyserconn, _ := grpc.Dial(deepLearningEngineAddress, grpc.WithInsecure(), grpc.WithBlock())
	// ***************************************** //
	
	var msg kafka.Message
	var err error
	var noMsg int = 0

	for {
		// the `ReadMessage` method blocks until we receive the next event

		// Bail out and raise alert if no msgs from patient gw
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		//msg, err := kafkaReader.FetchMessage(ctx)
		
		msg, err = kafkaReader.ReadMessage(ctx)
		if err == context.DeadlineExceeded {
			//t1Ctxt.log.Debugf("[ListeningTopic] ERROR %s , err %s err2 %v", patientUUID, ctx.Err(), err)
			time.Sleep(1 * time.Second)
			noMsg++
			rcvTime = time.Now()
			rcvTimeSec = uint64(rcvTime.Unix())
			/*
			if noMsg == 10 || noMsg == 100 || noMsg == 1000 {
				valstr = fmt.Sprintf("NumMsgsNotRcvd:%d",noMsg);
				t1Ctxt.sendAlert("major", patientUUID, "NO_MSG_RECVD", "SYSTEM", valstr, rcvTimeSec , 0)
			}
			*/

			if t1Ctxt.patientMapFind(patientUUID) == nil {
				t1Ctxt.deleteTopicKafka(patientUUID)
				return
			}
			goto baselineSend
			//raise alert if none in last 5min
			//continue
		} else if err != nil {
			t1Ctxt.log.Debugf("[ListeningTopic] Sleeping for 10s %s , err %s", patientUUID, err)
			t1Ctxt.startGoRoutine(func() {
				time.Sleep(time.Duration( (rand.Intn(10-1) + 1 )) * time.Second)
				t1Ctxt.patientTopicRegisterListen(tenantUUID, patientUUID)
				t1Ctxt.wg.Done()
			})
			time.Sleep(10 * time.Second)
			noMsg++
			break
		}


		cancel()
		ctx = nil
		cancel = nil
		if noMsg >= 10 {
			// Receiving message after recovery
			// Need to move the alert to Closed state
		}
		noMsg = 0
		//kafkaReader.CommitMessages(ctx, msg)

		// after receiving the message, log its value
		//t1Ctxt.log.Debugf("received: %s", string(msg.Value))
		err = json.Unmarshal(msg.Value, &spo2)
		if err != nil {
			//t1Ctxt.log.Debugf("Unmarshal error: %s for patient %s %v", err, patientUUID, msg)
		}
		
		//if strings.ToLower(spo2.DeviceType) == "temperature" {
		//	t1Ctxt.log.Debugf("@rajsreen --- %s ,  %s %d", patientUUID, spo2.DeviceType, spo2.DisplayTemperature)
		//}

		rcvTime = time.Now()
		rcvTimeSec = uint64(rcvTime.Unix())

		if spo2.DeviceType == "Checkme_O2" {
			// ********SPO2 sensor************************* //
			//t1Ctxt.log.Debugf("SPO2 %+v %s -- %s\n", spo2.SPo2, spo2.DeviceType, patientUUID)

			batteryGateway = int32(spo2.GwBattery)
			batterySpo2 = int32(spo2.Battery)
			if (int32(spo2.SPo2) > cachedSpo2  && (int32(spo2.SPo2) - cachedSpo2) > 2) ||
				(int32(spo2.SPo2) < cachedSpo2  && (cachedSpo2 - int32(spo2.SPo2)) > 2) ||
				(int32(spo2.Pr) > cachedPr  && (int32(spo2.Pr) - cachedPr) > 2) ||
				(int32(spo2.Pr) < cachedPr  && (cachedPr - int32(spo2.Pr)) > 2) ||
				(spo2.Pi > cachedPi  && (spo2.Pi - cachedPi) > 0.3) ||
				(spo2.Pi < cachedPi  && (cachedPi - spo2.Pi) > 0.3){				
				cachedSpo2 = int32(spo2.SPo2)
				cachedPr = int32(spo2.Pr)
				cachedPi = float32(math.Round(float64(spo2.Pi*100))/100)
				spo2WritePendingCnt++
			}
						

			if spo2WritePendingCnt > 5 {
				//If data was not written for 5 consecutive receipts, force a write on influx
				measrmnt = patientUUID + "_spo2"
				if useCurTimeForSimulation {
					recordTime = time.Unix(int64(rcvTimeSec), 0)
				} else {
					recordTime = time.Unix(int64(spo2.Time >> 10), 0)
				}
				// create point using fluent style
				p = influxdb2.NewPointWithMeasurement(measrmnt).
					AddField("spo2", int32(spo2.SPo2)).
					AddField("pi", spo2.Pi).
					AddField("pr", int32(spo2.Pr)).
					SetTime(recordTime)
				err = writeAPI.WritePoint(context.Background(), p)
				if err != nil {
					t1Ctxt.log.Errorf("Patient %s, %v",patientUUID, err)
				} else {
					spo2WritePendingCnt = 0
				}
			}
			
			/************************************************************
			// for hourly accounting
			/************************************************************/
			if  int32(spo2.SPo2) <= 100  &&  int32(spo2.SPo2) > 0 {
				spO2HourlyBaselineAvg += int32(spo2.SPo2)
				piHourlyBaselineAvg += spo2.Pi
				prHourlyBaselineAvg += int32(spo2.Pr)
				spo2HourlyNumsamples++
				
				// min max for spo2
				if int32(spo2.SPo2) > spO2HourlyBaselineMax {
					spO2HourlyBaselineMax = int32(spo2.SPo2)
				}

				if spO2HourlyBaselineMin == 0 {
					spO2HourlyBaselineMin = int32(spo2.SPo2)
				}
				if int32(spo2.SPo2) < spO2HourlyBaselineMin {
					spO2HourlyBaselineMin = int32(spo2.SPo2)
				}

				//------------------------------------//
				if int32(spo2.Pr) > prHourlyBaselineMax {
					prHourlyBaselineMax  = int32(spo2.Pr)
				}

				if prHourlyBaselineMin == 0 {
					prHourlyBaselineMin = int32(spo2.Pr)
				}
				if int32(spo2.Pr) < prHourlyBaselineMin {
					prHourlyBaselineMin = int32(spo2.Pr)
				}
				//------------------------------------//
				if spo2.Pi > piHourlyBaselineMax {
					piHourlyBaselineMax  = spo2.Pi
				}

				if piHourlyBaselineMin == 0 {
					piHourlyBaselineMin = spo2.Pi
				}
				if spo2.Pi < piHourlyBaselineMin {
					piHourlyBaselineMin = spo2.Pi
				}
			}
			//------------------------------------//
			if cachedSpo2 != 255 {
				agg60SecSampleRcvd++
			}

		} else if spo2.DeviceType == "temperature" || spo2.DeviceType == "Temperature" {
			
			//t1Ctxt.log.Debugf("@rajsreen ---TEMPERATURE %s , err %f", patientUUID, spo2.DisplayTemperature)

			batteryTemperature = int32(spo2.TemperatureBattery)
			if spo2.DisplayTemperature != cachedTemp {

				if (spo2.DisplayTemperature > cachedTemp && (spo2.DisplayTemperature-cachedTemp) > 0.5) || 
					(spo2.DisplayTemperature < cachedTemp && (cachedTemp - spo2.DisplayTemperature) > 0.5) {
					cachedTemp = float32(math.Round(float64( spo2.DisplayTemperature*100))/100)
					temperatureWritePendingCnt++
				}
			}
			agg60SecSampleRcvd++

			switch {
			case spo2.DisplayTemperature < 100 && TemperatureAlertState != TemperatureAlertClear:
				windowdTemperature  = append(windowdTemperature, float64(spo2.DisplayTemperature))
				if cap(windowdTemperature) > 5 {
					meanTemperature = float32(stat.Mean(windowdTemperature, nil))
					if meanTemperature < 100 {
						windowdTemperature = nil
						TemperatureAlertState = TemperatureAlertClear
						valstr = fmt.Sprintf("Temperature:%.1f",cachedTemp);
						t1Ctxt.sendAlert("normal", patientUUID, "TEMPERATURE_BACK_NORMAL", "TEMPERATURE", valstr, rcvTimeSec, int(meanTemperature))
					}
				}
			case spo2.DisplayTemperature > 102 && TemperatureAlertState != TemperatureAlertSev1:
				windowdTemperature  = append(windowdTemperature, float64(spo2.DisplayTemperature))
				if cap(windowdTemperature) > 5 {
					meanTemperature = float32(stat.Mean(windowdTemperature, nil))
					if meanTemperature > 102 {
						windowdTemperature = nil
						TemperatureAlertState = TemperatureAlertSev1
						valstr = fmt.Sprintf("Temperature:%0.1f",cachedTemp);
						t1Ctxt.sendAlert("major", patientUUID, "TEMPERATURE_VERY_HIGH", "TEMPERATURE", valstr, rcvTimeSec, int(meanTemperature))
						//t1Ctxt.log.Debugf("SPO2 alert %s %d",patientUUID, meanSpo2)	
					}
				}
			case spo2.DisplayTemperature >= 100 && spo2.DisplayTemperature <= 102 && TemperatureAlertState != TemperatureAlertSev2:
				windowdTemperature  = append(windowdTemperature, float64(spo2.DisplayTemperature))
				if cap(windowdTemperature) > 5 {
					meanTemperature = float32(stat.Mean(windowdTemperature, nil))
					if meanTemperature >= 100 {
						windowdTemperature = nil
						TemperatureAlertState = TemperatureAlertSev2
						valstr = fmt.Sprintf("Temperature:%.1f",cachedTemp);
						t1Ctxt.sendAlert("major", patientUUID, "TEMPERATURE_HIGH", "TEMPERATURE", valstr, rcvTimeSec, int(meanTemperature))
						//t1Ctxt.log.Debugf("SPO2 alert %s %d",patientUUID, meanSpo2)	
					}
				}
			default:
			}

			/************************************************************
			// for hourly accounting
			/************************************************************/
			if spo2.DisplayTemperature <= 70  && spo2.DisplayTemperature > 10 {
				temperatureHourlyBaselineAvg +=  spo2.DisplayTemperature
				temperatureHourlyNumsamples++
				
				// min max for spo2
				if spo2.DisplayTemperature > temperatureHourlyBaselineMax {
					temperatureHourlyBaselineMax = spo2.DisplayTemperature
				}

				if temperatureHourlyBaselineMin == 0 {
					temperatureHourlyBaselineMin =  spo2.DisplayTemperature
				}
				if spo2.DisplayTemperature < temperatureHourlyBaselineMin {
					temperatureHourlyBaselineMin = spo2.DisplayTemperature
				}
			}
			//------------------------------------//
		} else if spo2.DeviceType == "BP" {
			recvdEcg = spo2.Data.Extras
			cachedSystolic = int32(recvdEcg.Sys)
			cachedDiastolic = int32(recvdEcg.Dia)
			batteryBP = int32(spo2.Battery)
			bpRcvTime := uint64(recvdEcg.RecordTime)/1000

			t1Ctxt.log.Debugf("@rajsreen %d %d -- %d %v", cachedSystolic,cachedDiastolic,batteryBP , recvdEcg)
			liveSendBPSystolicMax		= cachedSystolic
			liveSendBPDiastolicMax		= cachedDiastolic
			outBaselines.TenantUUID = &tenantUUID
			outBaselines.PatientUUID = &patientUUID
			outBaselines.BaselineOp = &baselnOpCurrent
			outBaselinesBP.Bps = &liveSendBPSystolicMax
			outBaselinesBP.Bpd = &liveSendBPDiastolicMax
			outBaselinesBP.Time = &bpRcvTime

			outBaselines.Bp = outBaselinesBP

			
			outBaselines.Weight = nil
			outBaselines.Sugar = nil
			outBaselines.Temperature = nil
			outBaselines.Pr = nil
			outBaselines.Pi = nil
			outBaselines.Spo2 = nil
			outBaselines.Hr = nil
			outBaselines.Rr = nil


			t1Ctxt.sendToBaseliner(outBaselines)
		} else if spo2.DeviceType == "UrionBP" {
			cachedSystolic = int32(spo2.Bps)
			cachedDiastolic = int32(spo2.Bpd)
			bpRcvTime := uint64(spo2.Time)
			if bpRcvTime == 0 {
				bpRcvTime = rcvTimeSec
			} else {
				bpRcvTime = rcvTimeSec/1000
			}
			liveSendBPSystolicMax		= cachedSystolic
			liveSendBPDiastolicMax		= cachedDiastolic
			
			outBaselines.TenantUUID = &tenantUUID
			outBaselines.PatientUUID = &patientUUID
			outBaselines.BaselineOp = &baselnOpCurrent
			outBaselinesBP.Bps = &liveSendBPSystolicMax
			outBaselinesBP.Bpd = &liveSendBPDiastolicMax
			outBaselinesBP.Time = &bpRcvTime
			outBaselines.Bp = outBaselinesBP

			
			outBaselines.Weight = nil
			outBaselines.Sugar = nil
			outBaselines.Temperature = nil
			outBaselines.Pr = nil
			outBaselines.Pi = nil
			outBaselines.Spo2 = nil
			outBaselines.Hr = nil
			outBaselines.Rr = nil

			t1Ctxt.sendToBaseliner(outBaselines)
			t1Ctxt.log.Debugf("@rajsreen Systolic : %d Diastolic : %d", cachedSystolic,cachedDiastolic , bpRcvTime, spo2.Time)
		} else if spo2.DeviceType == "BodyFatScale" {
			cachedWeight = float32(spo2.Weight)/100.0
			t1Ctxt.log.Debugf("@rajsreen Weight :%s  %f -- %d", patientUUID, cachedWeight ,spo2.Weight)		
			agg10SecSampleRcvd++
		} else if spo2.DeviceType == "MANUAL" {
			vitals = spo2.VitalData
			t1Ctxt.log.Debugf("@rajsreen Vitals : %v", vitals )	
		} else {
			if useCurTimeForSimulation {
				patchRecordTime = rcvTimeSec
			} else {
				patchRecordTime = recvdEcg.RecordTime >> 10 // Divide by 1000
			}

			if newsdk == 1 {
				recvdEcg = spo2.Data.Extras
				
				if recvdEcg.Flash {
					//Ignore everything received from flash for now
					continue
				}
				if cap(windowdAccelXAxis) > MotionHistory {
					windowdAccelXAxis = windowdAccelXAxis[5:]
					windowdAccelYAxis = windowdAccelYAxis[5:]
					windowdAccelZAxis = windowdAccelZAxis[5:]
				}
				for i := 0; i<5;i++ {
					windowdAccelXAxis = append(windowdAccelXAxis, float64(recvdEcg.Acc[0].Xaxis))
					windowdAccelYAxis = append(windowdAccelYAxis, float64(recvdEcg.Acc[0].Yaxis))
					windowdAccelZAxis = append(windowdAccelZAxis, float64(recvdEcg.Acc[0].Zaxis))
	
				}
				motion = t1Ctxt.motionGuidanceNew(recvdEcg.Acc, windowdAccelXAxis, windowdAccelYAxis, windowdAccelZAxis)
			}
			
			
			if  motion != cached_motion {
				cached_motion = motion
				
				if motion == Sleeping && baselineSleepingNumSamples < 50 {
					baselineSleepingStart = patchRecordTime - 20
					baselineSleepingNumSamples = 20

					hrHourlySleepingBaselineMax = recvdEcg.Hr
					hrHourlySleepingBaselineMin = recvdEcg.Hr
					hrHourlySleepingBaselineAvg = uint32(recvdEcg.Hr)*20

					avgRRHourlySleepingBaselineMax = uint16(recvdEcg.AvgRR)
					avgRRHourlySleepingBaselineMin = uint16(recvdEcg.AvgRR)
					avgRRHourlySleepingBaselineAvg = uint32(recvdEcg.AvgRR)*20
					//t1Ctxt.log.Debugf("@rajsreen1 Sleeping %d %v %v %d %d %d %d %d %d\n",baselineSleepingNumSamples, baselineSleepingStart, windowedBaselineWrite, hrHourlySleepingBaselineAvg, hrHourlySleepingBaselineMax, hrHourlySleepingBaselineMin ,avgRRHourlySleepingBaselineAvg, avgRRHourlySleepingBaselineMax, avgRRHourlySleepingBaselineMin);
				}
				if motion == Standing && baselineStandingNumSamples < 50 {
					baselineStandingStart = patchRecordTime - 20
					baselineStandingNumSamples = 20

					hrHourlyStandingBaselineMax = recvdEcg.Hr
					hrHourlyStandingBaselineMin = recvdEcg.Hr
					hrHourlyStandingBaselineAvg = uint32(recvdEcg.Hr)*20

					avgRRHourlyStandingBaselineMax = uint16(recvdEcg.AvgRR)
					avgRRHourlyStandingBaselineMin = uint16(recvdEcg.AvgRR)
					avgRRHourlyStandingBaselineAvg = uint32(recvdEcg.AvgRR)*20
				}
				prevRRi = 0
			} else {
				if motion == Sleeping {
					baselineSleepingNumSamples++
					hrHourlySleepingBaselineAvg += uint32(recvdEcg.Hr)
					avgRRHourlySleepingBaselineAvg += uint32(recvdEcg.AvgRR)
					
					if uint16(recvdEcg.Hr) >  hrHourlySleepingBaselineMax {
						hrHourlySleepingBaselineMax = uint16(recvdEcg.Hr)
					}
					if hrHourlySleepingBaselineMin == 0 || 
					   uint16(recvdEcg.Hr) < hrHourlySleepingBaselineMin {
						hrHourlySleepingBaselineMin = uint16(recvdEcg.Hr)
					}

					if uint16(recvdEcg.AvgRR) >  avgRRHourlySleepingBaselineMax {
						avgRRHourlySleepingBaselineMax = uint16(recvdEcg.AvgRR)
					}
					if avgRRHourlySleepingBaselineMin == 0 || 
					   uint16(recvdEcg.AvgRR) < avgRRHourlySleepingBaselineMin {
						avgRRHourlySleepingBaselineMin = uint16(recvdEcg.AvgRR)
					}
					//t1Ctxt.log.Debugf("@rajsreen Sleeping %d %v %v %d %d %d %d %d %d\n",baselineSleepingNumSamples, baselineSleepingStart, windowedBaselineWrite, hrHourlySleepingBaselineAvg, hrHourlySleepingBaselineMax, hrHourlySleepingBaselineMin ,avgRRHourlySleepingBaselineAvg, avgRRHourlySleepingBaselineMax, avgRRHourlySleepingBaselineMin);
				}
				if motion == Standing {
					baselineStandingNumSamples++
					hrHourlyStandingBaselineAvg += uint32(recvdEcg.Hr)
					avgRRHourlyStandingBaselineAvg += uint32(recvdEcg.AvgRR)

					if uint16(recvdEcg.Hr) >  hrHourlyStandingBaselineMax {
						hrHourlyStandingBaselineMax = uint16(recvdEcg.Hr)
					}
					if hrHourlyStandingBaselineMin == 0 || 
					   uint16(recvdEcg.Hr) < hrHourlyStandingBaselineMin {
						hrHourlyStandingBaselineMin = uint16(recvdEcg.Hr)
					}

					if uint16(recvdEcg.AvgRR) >  avgRRHourlyStandingBaselineMax {
						avgRRHourlyStandingBaselineMax = uint16(recvdEcg.AvgRR)
					}
					if avgRRHourlyStandingBaselineMin == 0 || 
					   uint16(recvdEcg.AvgRR) < avgRRHourlyStandingBaselineMin {
						avgRRHourlyStandingBaselineMin = uint16(recvdEcg.AvgRR)
					}
				}
			}
			agg60SecSampleRcvd++

			batteryEcg = int32(recvdEcg.Battery)
			batteryGateway = int32(spo2.GwBattery)

			/***************************************************/
			/* RR Interval Analysis ****************************/
			for i:=0 ; i <len(recvdEcg.Rri); i++ {				
				if motion == 0 || recvdEcg.Rri[i] == 0 || prevRRi == 0{
					break
				}
				if diff(prevRRi,recvdEcg.Rri[i]) > 200 {
					valstr = fmt.Sprintf("RR-Interval:%d",recvdEcg.Rri[i]);
					t1Ctxt.sendAlert("major", patientUUID, "AFIB_ECG", "ECG", valstr, patchRecordTime, motion)
				}
				prevRRi = recvdEcg.Rri[i]
			}
			/***************************************************/

			if recvdEcg.Rri[0] == 0 && motion != 0{
				//t1Ctxt.log.Debugf("Pause %v",spo2.Ecg, spo2.Rri)
				valstr = fmt.Sprintf("HR:%d",recvdEcg.Hr);
				t1Ctxt.sendAlert("major", patientUUID, "PAUSE_ECG", "ECG", valstr, patchRecordTime, motion)
			}

			
			/*******************************************************************/
			/***** HR_MAX - HR_MIN     *****************************************/
			/*******************************************************************/
			if hrMax == 0 {
				hrMax = int32(recvdEcg.Hr)
				hrMin = recvdEcg.Hr
			}
			hrMax = int32(math.Max(float64(hrMax), float64(recvdEcg.Hr)))
			hrMin = uint16(math.Min(float64(hrMin), float64(recvdEcg.Hr)))
			
			if timeCnt == 120 {
				timeCnt = 0
				hrMax = int32(recvdEcg.Hr)
				//newsdk
				//hrMax = recvdEcg.Hr
				hrMin = uint16(hrMax)
			} else {
				timeCnt++
			}

			
			/*******************************************************************/

			cachedHr = int32(recvdEcg.Hr)
			cachedAvgRR = int32(recvdEcg.AvgRR)

			t1Ctxt.sendEcgToDeepAnalyser(deepAnalyserconn, patientUUID, int64(patchRecordTime), recvdEcg.Ecg, cachedHr, cachedAvgRR,
										cachedSpo2, cachedPr, cachedPi, cachedTemp, int32(motion))
			//newsdk
			//t1Ctxt.sendEcgToDeepAnalyser(deepAnalyserconn, patientUUID, spo2, recvdEcg.Ecg)
			
			measrmnt = patientUUID + "_ecg"
			recordTime = time.Unix(int64(patchRecordTime), 0)
			p = influxdb2.NewPointWithMeasurement(measrmnt).
				AddField("ecg", arrayToString(recvdEcg.Ecg, ",")).
				AddField("hr", int32(recvdEcg.Hr)).
				AddField("hrMax", int32(hrMax)).
				AddField("hrMin", int32(hrMin)).
				AddField("AvgRR", int32(recvdEcg.AvgRR)).
				AddField("spo2", int32(cachedSpo2)).
				AddField("pr", int32(cachedPr)).
				AddField("pi", int32(cachedPi)).
				AddField("temperature", float32(cachedTemp)).
				AddField("batteryEcg", int32(batteryEcg)).
				AddField("batteryTemperature", int32(batteryTemperature)).
				AddField("batterySpo2", int32(batterySpo2)).
				AddField("batteryGateway", int32(batteryGateway)).
				AddField("motion", int32(motion)).
				SetTime(recordTime)
			err = writeAPI.WritePoint(context.Background(), p)
			if err != nil {
				t1Ctxt.log.Errorf("Patient %s, %v",patientUUID, err)
			} else {
				spo2WritePendingCnt = 0
				temperatureWritePendingCnt = 0
			}
			
		}

	baselineSend:
		if (agg10SecSampleRcvd != 0) && ((rcvTimeSec - agg10SecSampleSendTimeSec)  > 10) {
			liveSendWeight	= cachedWeight
			outBaselines.TenantUUID = &tenantUUID
			outBaselines.PatientUUID = &patientUUID
			outBaselines.BaselineOp = &baselnOpCurrent
			outBaselines.Weight = &liveSendWeight
			cachedWeight = 0
			agg10SecSampleRcvd = 0
			agg10SecSampleSendTimeSec = rcvTimeSec

			outBaselines.Bp = nil
			outBaselines.Sugar = nil
			outBaselines.Temperature = nil
			outBaselines.Pr = nil
			outBaselines.Pi = nil
			outBaselines.Spo2 = nil
			outBaselines.Hr = nil
			outBaselines.Rr = nil

			t1Ctxt.sendToBaseliner(outBaselines)
			continue
		}

		if (agg60SecSampleRcvd != 0) && ((rcvTimeSec - agg60SecSampleSendTimeSec)  > 60) {

			t1Ctxt.log.Debugf("@rajsreen on 60sec send %d .. %d", cachedHr, cachedAvgRR)
			liveSendhrMax 				= cachedHr
			liveSendRrMax 				= cachedAvgRR


			livebatteryEcg = batteryEcg
			livebatterySpo2 = batterySpo2
			livebatteryTemperature = batteryTemperature
			//livebatteryBP = batteryBP
			livebatteryGateway = batteryGateway

			outBaselines.TenantUUID = &tenantUUID
			outBaselines.PatientUUID = &patientUUID
			outBaselines.BaselineOp = &baselnOpCurrent

			if liveSendhrMax != 999 {
				outBaselinesHr.HrMax = &liveSendhrMax
				outBaselinesHr.HrMin = &zeroCacheInt
				outBaselinesHr.HrAvg = &zeroCacheInt
				outBaselinesRr.RrMax = &liveSendRrMax
				outBaselinesRr.RrMin = &zeroCacheInt
				outBaselinesRr.RrAvg = &zeroCacheInt
				outBaselines.Hr = outBaselinesHr
				outBaselines.Rr = outBaselinesRr
			} else{
				outBaselines.Hr = nil
				outBaselines.Rr = nil
			}
			cachedHr = 999
			cachedAvgRR = 999

			
			if cachedTemp != 0 {
				liveSendTemperatureMax		= cachedTemp
				outBaselinesTemperature.TemperatureMax = &liveSendTemperatureMax
				outBaselinesTemperature.TemperatureMin = &zeroCacheFloat
				outBaselinesTemperature.TemperatureAvg = &zeroCacheFloat
				outBaselines.Temperature = outBaselinesTemperature
				cachedTemp = 0
			} else {
				outBaselines.Temperature = nil
			}
		
			

			liveSendSpo2Max = cachedSpo2
			if liveSendSpo2Max != 255 {
				outBaselinesSpo2.Spo2Max = &liveSendSpo2Max
				outBaselinesSpo2.Spo2Min = &zeroCacheInt
				outBaselinesSpo2.Spo2Avg = &zeroCacheInt
				outBaselines.Spo2 = outBaselinesSpo2
				cachedSpo2 = 0
				liveSendPrMax = cachedPr
				outBaselinesPr.PrMax = &liveSendPrMax
				outBaselinesPr.PrMin = &zeroCacheInt
				outBaselinesPr.PrAvg = &zeroCacheInt
				outBaselines.Pr = outBaselinesPr
				cachedPr = 0
				
				liveSendPiMax = cachedPi
				outBaselinesPi.PiMax = &liveSendPiMax
				outBaselinesPi.PiMin = &zeroCacheFloat
				outBaselinesPi.PiAvg = &zeroCacheFloat
				outBaselines.Pi = outBaselinesPi
				outBaselines.BatterySpo2 = &livebatterySpo2

				cachedPi = 0
			} else {
				outBaselines.Spo2 = nil
				outBaselines.Pr = nil
				outBaselines.Pi = nil
				outBaselines.BatterySpo2 = &livebatterySpo2

			}
			cachedSpo2 = 255

			outBaselines.Weight = nil
			outBaselines.Sugar = nil
			outBaselines.Bp = nil
			
			outBaselines.CurrTime = &rcvTimeSec

			outBaselines.BatteryEcg = &livebatteryEcg
			outBaselines.BatteryTemperature = &livebatteryTemperature
			outBaselines.BatteryGateway = &livebatteryGateway

			agg60SecSampleRcvd = 0
			agg60SecSampleSendTimeSec = rcvTimeSec
			t1Ctxt.sendToBaseliner(outBaselines)
			continue
		} else 	if rcvTimeSec - windowedBaselineWrite > 900 {

			continue
			if spo2HourlyNumsamples != 0 {
				spO2HourlyBaselineAvg = (spO2HourlyBaselineAvg/spo2HourlyNumsamples)
				prHourlyBaselineAvg = (prHourlyBaselineAvg/spo2HourlyNumsamples)
				piHourlyBaselineAvg = float32(math.Round(float64((piHourlyBaselineAvg/float32(spo2HourlyNumsamples)*100)))/100)
				spo2AlertState = t1Ctxt.checkSpo2Alert(patientUUID, spo2AlertState, spO2HourlyBaselineAvg, rcvTimeSec)
			} else {
				spO2HourlyBaselineAvg 	= 999
				prHourlyBaselineAvg 	= 999
				piHourlyBaselineAvg		= 999
				spO2HourlyBaselineMax	= 999
				prHourlyBaselineMax		= 999
				piHourlyBaselineMax		= 999
				spO2HourlyBaselineMin	= 999
				prHourlyBaselineMin		= 999
				piHourlyBaselineMin		= 999
				cachedSpo2 = 255
				cachedPr = 255
				cachedPi = 999
			}


			continue
			pInfo = t1Ctxt.patientMapFind(patientUUID)
			if pInfo == nil {
				break
			} else {
				//bps = pInfo.bps
				//bpd = pInfo.bpd
				sugar = pInfo.sugar
				t1Ctxt.patientMapFindUpdate(patientUUID, 999, 999,999)
			}

			/* Windowed baseline writes*/
			windowedBaselineWrite = rcvTimeSec
			
			preferStanding = false
			if baselineSleepingNumSamples !=0  {
				hrHourlySleepingBaselineAvg = uint32(hrHourlySleepingBaselineAvg/uint32(baselineSleepingNumSamples))
				avgRRHourlySleepingBaselineAvg = uint32(avgRRHourlySleepingBaselineAvg/uint32(baselineSleepingNumSamples))
			}
			if baselineStandingNumSamples !=0  {
				hrHourlyStandingBaselineAvg = uint32(hrHourlyStandingBaselineAvg/uint32(baselineStandingNumSamples))
				avgRRHourlyStandingBaselineAvg = uint32(avgRRHourlyStandingBaselineAvg/uint32(baselineStandingNumSamples))
				if baselineSleepingNumSamples < 100 {
					preferStanding = true
				}
			}

			if baselineStandingNumSamples == 0 && baselineSleepingNumSamples == 0 {
				hrHourlyStandingBaselineAvg = 999
				hrHourlyStandingBaselineMax = 999
				hrHourlyStandingBaselineMin = 999
				hrHourlySleepingBaselineAvg = 999
				hrHourlySleepingBaselineMax = 999
				hrHourlySleepingBaselineMin = 999

				avgRRHourlyStandingBaselineAvg = 999
				avgRRHourlyStandingBaselineMin = 999
				avgRRHourlyStandingBaselineMin = 999

				avgRRHourlySleepingBaselineAvg = 999
				avgRRHourlySleepingBaselineMin = 999
				avgRRHourlySleepingBaselineMin = 999

				cachedHr = 999
				cachedAvgRR = 999
			}

			/*
			
			*/
			

			if temperatureHourlyNumsamples != 0 {
				temperatureHourlyBaselineAvg = float32(math.Round(float64((temperatureHourlyBaselineAvg/float32(temperatureHourlyNumsamples)))*10)/10)
			} else {
				temperatureHourlyBaselineAvg = 999
				temperatureHourlyBaselineMin = 999
				temperatureHourlyBaselineMax = 999
				cachedTemp = 999
			}

			livebatteryEcg = batteryEcg
			livebatterySpo2 = batterySpo2
			livebatteryTemperature = batteryTemperature
			livebatteryGateway = batteryGateway
	
			t1Ctxt.log.Debugf("start %d %d\n", baselineSleepingStart, baselineStandingStart)
			//t1Ctxt.log.Debugf("BaselineSamples Sleeping %s %d %v %v %d %d %d %d %d %d\n",patientUUID, baselineSleepingNumSamples, baselineSleepingStart, windowedBaselineWrite, hrHourlySleepingBaselineAvg, hrHourlySleepingBaselineMax, hrHourlySleepingBaselineMin ,avgRRHourlySleepingBaselineAvg, avgRRHourlySleepingBaselineMax, avgRRHourlySleepingBaselineMin);
			//t1Ctxt.log.Debugf("BaselineSamples Standing %s %d %v %v %d %d %d %d %d %d\n",patientUUID, baselineStandingNumSamples, baselineStandingStart, windowedBaselineWrite, 
			//																		hrHourlyStandingBaselineAvg, hrHourlyStandingBaselineMax, hrHourlyStandingBaselineMin ,
		    //																			avgRRHourlyStandingBaselineAvg, avgRRHourlyStandingBaselineMax, avgRRHourlyStandingBaselineMin);			

			if preferStanding {
				hrHourlyStandingBaselineMaxInt = int32(hrHourlyStandingBaselineMax)
				hrHourlyStandingBaselineMinInt = int32(hrHourlyStandingBaselineMin)
				hrHourlyStandingBaselineAvgInt = int32(hrHourlyStandingBaselineAvg)
				avgRRHourlyStandingBaselineMaxInt = int32(avgRRHourlyStandingBaselineMax)
				avgRRHourlyStandingBaselineMinInt = int32(avgRRHourlyStandingBaselineMin)
				avgRRHourlyStandingBaselineAvgInt = int32(avgRRHourlyStandingBaselineAvg)
			} else {
				hrHourlyStandingBaselineMaxInt = int32(hrHourlySleepingBaselineMax)
				hrHourlyStandingBaselineMinInt = int32(hrHourlySleepingBaselineMin)
				hrHourlyStandingBaselineAvgInt = int32(hrHourlySleepingBaselineAvg)
				avgRRHourlyStandingBaselineMaxInt = int32(avgRRHourlySleepingBaselineMax)
				avgRRHourlyStandingBaselineMinInt = int32(avgRRHourlySleepingBaselineMin)
				avgRRHourlyStandingBaselineAvgInt = int32(avgRRHourlySleepingBaselineAvg)
			}
			
			spO2HourlyBaselineMaxSend = int32(spO2HourlyBaselineMax)
			spO2HourlyBaselineMinSend = int32(spO2HourlyBaselineMin)
			spO2HourlyBaselineAvgSend = int32(spO2HourlyBaselineAvg)
			prHourlyBaselineMaxSend = int32(prHourlyBaselineMax)
			prHourlyBaselineMinSend = int32(prHourlyBaselineMin)
			prHourlyBaselineAvgSend = int32(prHourlyBaselineAvg)
			piHourlyBaselineMaxSend = (piHourlyBaselineMax)
			piHourlyBaselineMinSend = (piHourlyBaselineMin)
			piHourlyBaselineAvgSend = (piHourlyBaselineAvg)
			temperatureHourlyBaselineMaxSend = (temperatureHourlyBaselineMax)
			temperatureHourlyBaselineMinSend = (temperatureHourlyBaselineMin)
			temperatureHourlyBaselineAvgSend = (temperatureHourlyBaselineAvg)
			


			outBaselines.PatientUUID = &patientUUID
			outBaselines.TenantUUID = &tenantUUID
			outBaselines.BaselineOp = &baselnOpAggregate

			outBaselinesHr.HrMax = &hrHourlyStandingBaselineMaxInt
			outBaselinesHr.HrMin = &hrHourlyStandingBaselineMinInt
			outBaselinesHr.HrAvg = &hrHourlyStandingBaselineAvgInt
			outBaselines.Hr = outBaselinesHr


			outBaselinesRr.RrMax = &avgRRHourlyStandingBaselineMaxInt
			outBaselinesRr.RrMin = &avgRRHourlyStandingBaselineMinInt
			outBaselinesRr.RrAvg = &avgRRHourlyStandingBaselineAvgInt
			outBaselines.Rr = outBaselinesRr
			
			outBaselinesSpo2.Spo2Max = &spO2HourlyBaselineMaxSend
			outBaselinesSpo2.Spo2Min = &spO2HourlyBaselineMinSend
			outBaselinesSpo2.Spo2Avg = &spO2HourlyBaselineAvgSend
			outBaselines.Spo2 = outBaselinesSpo2


			outBaselinesPr.PrMax = &prHourlyBaselineMaxSend
			outBaselinesPr.PrMin = &prHourlyBaselineMinSend
			outBaselinesPr.PrAvg = &prHourlyBaselineAvgSend
			outBaselines.Pr = outBaselinesPr
			

			outBaselinesPi.PiMax = &piHourlyBaselineMaxSend
			outBaselinesPi.PiMin = &piHourlyBaselineMinSend
			outBaselinesPi.PiAvg = &piHourlyBaselineAvgSend
			outBaselines.Pi = outBaselinesPi
			

			outBaselinesTemperature.TemperatureMax = &temperatureHourlyBaselineMaxSend
			outBaselinesTemperature.TemperatureMin = &temperatureHourlyBaselineMinSend
			outBaselinesTemperature.TemperatureAvg = &temperatureHourlyBaselineAvgSend
			outBaselines.Temperature = outBaselinesTemperature
			outBaselines.CurrTime = &rcvTimeSec

			outBaselines.BatteryEcg = &livebatteryEcg
			outBaselines.BatterySpo2 = &livebatterySpo2
			outBaselines.BatteryTemperature = &livebatteryTemperature
			outBaselines.BatteryGateway = &livebatteryGateway

			outBaselinesBP.Bps = &liveSendBPSystolicMax
			outBaselinesBP.Bpd = &liveSendBPDiastolicMax
			outBaselines.Bp = outBaselinesBP

			outBaselines.Sugar = &sugar
			outBaselines.Weight = &liveSendWeight
			
			t1Ctxt.sendToBaseliner(outBaselines)


			

			//reset baselines
			spO2HourlyBaselineAvg = 0
			spO2HourlyBaselineMax = 0
			spO2HourlyBaselineMin = 0
			prHourlyBaselineAvg = 0
			prHourlyBaselineMax = 0
			prHourlyBaselineMin = 0
			piHourlyBaselineAvg = 0
			piHourlyBaselineMax = 0
			piHourlyBaselineMin = 0
			spo2HourlyNumsamples = 0
			temperatureHourlyBaselineAvg = 0
			temperatureHourlyBaselineMax = 0
			temperatureHourlyBaselineMin = 0
			temperatureHourlyNumsamples = 0

			batteryTemperature = -1
			batterySpo2			= -1
			batteryEcg			= -1
			batteryGateway		= -1
		

			if motion == Sleeping && baselineSleepingNumSamples != 0 {
				baselineSleepingStart = rcvTimeSec-20//(recvdEcg.RecordTime/1000) - 20
				baselineSleepingNumSamples = 20
				
				
				hrHourlySleepingBaselineMax = uint16(hrHourlySleepingBaselineAvg)
				hrHourlySleepingBaselineMin = uint16(hrHourlySleepingBaselineAvg)
				hrHourlySleepingBaselineAvg = hrHourlySleepingBaselineAvg *20

				avgRRHourlySleepingBaselineMax = uint16(avgRRHourlySleepingBaselineAvg)
				avgRRHourlySleepingBaselineMin = uint16(avgRRHourlySleepingBaselineAvg)
				avgRRHourlySleepingBaselineAvg = avgRRHourlySleepingBaselineAvg *20

				baselineStandingNumSamples = 0
			} else if motion == Standing && baselineStandingNumSamples != 0{
				baselineStandingStart = rcvTimeSec-20 //(recvdEcg.RecordTime/1000) - 20 TODO @rajsreen
				baselineStandingNumSamples = 20
				
				hrHourlyStandingBaselineMax = uint16(hrHourlyStandingBaselineAvg)
				hrHourlyStandingBaselineMin = uint16(hrHourlyStandingBaselineAvg)
				hrHourlyStandingBaselineAvg = hrHourlyStandingBaselineAvg *20
				avgRRHourlyStandingBaselineMax = uint16(avgRRHourlyStandingBaselineAvg)
				avgRRHourlyStandingBaselineMin = uint16(avgRRHourlyStandingBaselineAvg)
				avgRRHourlyStandingBaselineAvg = avgRRHourlyStandingBaselineAvg *20

				baselineSleepingNumSamples = 0
			} else {

				baselineSleepingStart = 0
				baselineSleepingNumSamples = 0								
				hrHourlySleepingBaselineMax = 0
				hrHourlySleepingBaselineMin = 0
				hrHourlySleepingBaselineAvg = 0
				avgRRHourlySleepingBaselineMax = 0
				avgRRHourlySleepingBaselineMin = 0
				avgRRHourlySleepingBaselineAvg = 0


				baselineStandingStart = 0
				baselineStandingNumSamples = 0
				hrHourlyStandingBaselineMax = 0
				hrHourlyStandingBaselineMin = 0
				hrHourlyStandingBaselineAvg = 0
				avgRRHourlyStandingBaselineMax = 0
				avgRRHourlyStandingBaselineMin = 0
				avgRRHourlyStandingBaselineAvg = 0
			}
		}
	}
}