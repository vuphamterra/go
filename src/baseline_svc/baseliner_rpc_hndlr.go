package main

/* listed alphabetically */
import (
	"time"
	_ "bytes"
	"context"
	_ "log"
	_ "math"
	_ "sort"
	"strconv"
	"fmt"
	"strings"
	"reflect"
	_ "encoding/json"
	pb "baseline_svc/proto_out/emr_consumer_proto"
	"github.com/nakabonne/tstorage"

	_ "google.golang.org/grpc"
)

type BaselinerServer struct {
	bslnrCtxt *baselinerContext
}

func getDurationFoldup(duration pb.FetchDuration) (int, bool) {
	var  nHistory int = 12
	var foldUp bool = false
	if duration == pb.FetchDuration_FETCH_DURATION_8HR {
		nHistory = 8
	} else if duration == pb.FetchDuration_FETCH_DURATION_12HR {
		nHistory = 12
	} else if duration == pb.FetchDuration_FETCH_DURATION_24HR {
		nHistory = 24
	} else if duration == pb.FetchDuration_FETCH_DURATION_48HR {
		nHistory = 48
		foldUp = true
	} else if duration == pb.FetchDuration_FETCH_DURATION_76HR {
		nHistory = 76
		foldUp = true
	} else if duration == pb.FetchDuration_FETCH_DURATION_1WK {
		nHistory = 252
		foldUp = true
	} else if duration == pb.FetchDuration_FETCH_DURATION_2WK {
		nHistory = 336
		foldUp = true
	} else if duration == pb.FetchDuration_FETCH_DURATION_3WK {
		nHistory = 504
		foldUp = true
	} else if duration == pb.FetchDuration_FETCH_DURATION_4WK {
		nHistory = 672
		foldUp = true
	}
	return nHistory,foldUp
}


// NewBaselinerServer - returns the pointer to the implementation.
func NewBaselinerServer() *BaselinerServer {
	return &BaselinerServer{}
}

// RegisterPatient : API for registration of Patient UUID and kafka registration
func (s *BaselinerServer) RegisterPatient(context context.Context, in *pb.PatientRegister) (*pb.PatientRegisterRsp, error) {
	bslnrCtxt := s.bslnrCtxt
	patientUUID := strings.TrimSpace(*in.PatientUUID)

	bslnrCtxt.log.Debugf("[Register Patient] Entered %s", patientUUID)

	// Need to spwan off the topic as a kafka consumer and complete registration

	rsp := true
	return &pb.PatientRegisterRsp{Rsp: &rsp}, nil

}



// RegisterPatient : API for registration of Patient UUID and kafka registration
func (s *BaselinerServer) DeRegisterPatient(context context.Context, in *pb.PatientRegister) (*pb.PatientRegisterRsp, error) {
	bslnrCtxt := s.bslnrCtxt
	patientUUID := strings.TrimSpace(*in.PatientUUID)

	bslnrCtxt.log.Debugf("[DeRegister Patient] Entered %s", patientUUID)
	
	// Need to spwan off the topic as a kafka consumer and complete registration

	rsp := true
	return &pb.PatientRegisterRsp{Rsp: &rsp}, nil
}

func (bslnrCtxt *baselinerContext) deletePatientContext(uuidTenant string, uuidPatient string) {
	patientInfoDetails := bslnrCtxt.patientMapFind(uuidTenant, uuidPatient)
	if patientInfoDetails == nil {
		return
	}
	patientInfoDetails.patientLck.Lock()
	defer patientInfoDetails.patientLck.Unlock()
	patientInfoDetails = nil
}

func (bslnrCtxt *baselinerContext) createNewPatientContext(uuidTenant string, uuidPatient string) (*PatientSensorReadings) {
	bslnrCtxt.log.Debugf("Registering Patient %s on Tenant %s", uuidPatient, uuidTenant )

	patientInfo := new(PatientSensorReadings)
	if patientInfo == nil {
		bslnrCtxt.log.Errorf("patient creation failed.Memory %s", patientInfo)
	}	
	
	patientInfo.uuidPatient = uuidPatient

	bslnrCtxt.initializeMeasurement(patientInfo, "HR")
	bslnrCtxt.initializeMeasurement(patientInfo, "RR")
	bslnrCtxt.initializeMeasurement(patientInfo, "SPO2")
	bslnrCtxt.initializeMeasurement(patientInfo, "PR")
	bslnrCtxt.initializeMeasurement(patientInfo, "PI")
	bslnrCtxt.initializeMeasurement(patientInfo, "TEMPERATURE")
	bslnrCtxt.initializeMeasurement(patientInfo, "BP")	
	bslnrCtxt.initializeMeasurement(patientInfo, "WEIGHT")


	/*
	hr := make([][]int32, NUMROWS)
	for i := 0; i < NUMROWS; i++ {
		hr[i] = make([]int32, 1)
	}	
	patientInfo.hr = hr
	
	rr := make([][]int32, NUMROWS)
	for i := 0; i < NUMROWS; i++ {
		rr[i] = make([]int32, 1)
	}	
	patientInfo.rr = rr
	
	spo2 := make([][]int32, NUMROWS)
	for i := 0; i < NUMROWS; i++ {
		spo2[i] = make([]int32, 1)
	}	
	patientInfo.spo2 = spo2
	
	pr := make([][]int32, NUMROWS)
	for i := 0; i < NUMROWS; i++ {
		pr[i] = make([]int32, 1)
	}	
	patientInfo.pr = pr
	
	pi := make([][]float32, NUMROWS)
	for i := 0; i < NUMROWS; i++ {
		pi[i] = make([]float32, 1)
	}	
	patientInfo.pi = pi
	
	temperature := make([][]float32, NUMROWS)
	for i := 0; i < NUMROWS; i++ {
		temperature[i] = make([]float32, 1)
	}	
	patientInfo.temperature = temperature
	
	

	bps := make([][]int32, NUMROWSBP)
	for i := 0; i < NUMROWSBP; i++ {
		bps[i] = make([]int32, 1)
	}
	patientInfo.bps = bps

	bpd := make([][]int32, NUMROWSBP)
	for i := 0; i < NUMROWSBP; i++ {
		bpd[i] = make([]int32, 1)
	}
	patientInfo.bpd = bpd

	
	weight := make([][]float32, NUMROWSBP)
	for i := 0; i < NUMROWSBP; i++ {
		weight[i] = make([]float32, 1)
	}
	patientInfo.weight = weight
	*/


	sugar := make([]int32, 1)
	patientInfo.sugar = sugar

	ews := make([]int32, 0)
	patientInfo.ews = ews

	batteryEcg := make([]int32, 0)
	patientInfo.batteryEcg = batteryEcg

	batterySpo2 := make([]int32, 0)
	patientInfo.batterySpo2 = batterySpo2

	batteryTemperature := make([]int32, 0)
	patientInfo.batteryTemperature = batteryTemperature

	batteryGateway := make([]int32, 0)
	patientInfo.batteryGateway = batteryGateway

	/*

	foldUphr := make([][]int32, NUMROWS)
	for i := 0; i < NUMROWS; i++ {
		foldUphr[i] = make([]int32, 0)
	}	
	patientInfo.foldUphr = foldUphr
	
	foldUprr := make([][]int32, NUMROWS)
	for i := 0; i < NUMROWS; i++ {
		foldUprr[i] = make([]int32, 0)
	}	
	patientInfo.foldUprr = foldUprr
	
	foldUpspo2 := make([][]int32, NUMROWS)
	for i := 0; i < NUMROWS; i++ {
		foldUpspo2[i] = make([]int32, 0)
	}	
	patientInfo.foldUpspo2 = foldUpspo2
	
	foldUppr := make([][]int32, NUMROWS)
	for i := 0; i < NUMROWS; i++ {
		foldUppr[i] = make([]int32, 0)
	}	
	patientInfo.foldUppr = foldUppr
	
	foldUppi := make([][]float32, NUMROWS)
	for i := 0; i < NUMROWS; i++ {
		foldUppi[i] = make([]float32, 0)
	}	
	patientInfo.foldUppi = foldUppi
	
	foldUptemperature := make([][]float32, NUMROWS)
	for i := 0; i < NUMROWS; i++ {
		foldUptemperature[i] = make([]float32, 0)
	}	
	patientInfo.foldUptemperature = foldUptemperature
	foldUpbpd := make([][]int32, NUMROWSBP)
	for i := 0; i < NUMROWSBP; i++ {
		foldUpbpd[i] = make([]int32, 0)
	}	
	patientInfo.foldUpbpd = foldUpbpd

	foldUpbps := make([][]int32, NUMROWSBP)
	for i := 0; i < NUMROWSBP; i++ {
		foldUpbps[i] = make([]int32, 0)
	}
	patientInfo.foldUpbps = foldUpbps
	
	foldUpweight := make([][]float32, NUMROWSBP)
	for i := 0; i < NUMROWSBP; i++ {
		foldUpweight[i] = make([]float32, 0)
	}	
	patientInfo.foldUpweight = foldUpweight
	*/
	
	foldUpews := make([]int32, 0)
	patientInfo.foldUpews = foldUpews

	

	foldUpsugar := make([]int32, 0)
	patientInfo.foldUpsugar = foldUpsugar

	
	foldUpbatteryEcg := make([]int32, 0)
	patientInfo.foldUpbatteryEcg = foldUpbatteryEcg

	foldUpbatterySpo2 := make([]int32, 0)
	patientInfo.foldUpbatterySpo2 = foldUpbatterySpo2

	foldUpbatteryTemperature := make([]int32, 0)
	patientInfo.foldUpbatteryTemperature = foldUpbatteryTemperature

	foldUpbatteryGateway := make([]int32, 0)
	patientInfo.foldUpbatteryGateway = foldUpbatteryGateway


	patientInfo.currEws = 20
	bslnrCtxt.patientMapInsert(uuidTenant, uuidPatient, patientInfo)
		
	bslnrCtxt.addNewPatientToSortList(uuidTenant, uuidPatient)
	return patientInfo
}

func (bslnrCtxt *baselinerContext) patientCurrentUpdateDetails(uuidTenant string, uuidPatient string, in *pb.BaselinesPatient) {

	ptntMap := bslnrCtxt.tenantMapFind(uuidTenant)
	if ptntMap == nil {
		ptntMap = bslnrCtxt.tenantMapInsert(uuidTenant)
	}
	bslnrCtxt.log.Debugf("patientCurrentUpdateDetails %s -- %v", uuidPatient,*in)

	currentTime  := time.Now().Unix()

	patientInfoDetails := bslnrCtxt.patientMapFind(uuidTenant, uuidPatient)
	if patientInfoDetails == nil {
		patientInfoDetails = bslnrCtxt.createNewPatientContext(uuidTenant, uuidPatient)
	}
	patientInfoDetails.patientLck.Lock()
	defer patientInfoDetails.patientLck.Unlock()

	if in.Hr != nil && *in.Hr.HrMax != 0 {
		patientInfoDetails.lastUpdatedLiveHr 		= *in.Hr.HrMax
		patientInfoDetails.lastUpdatedLiveEcgTime 	= uint64(currentTime)
		bslnrCtxt.insertMetricInflux(uuidPatient, HR, currentTime, float64(*in.Hr.HrMax))
	}
	if in.Rr != nil && *in.Rr.RrMax != 0 {
		patientInfoDetails.rr[0][0] = int32(*in.Rr.RrMax)
		bslnrCtxt.insertMetricInflux(uuidPatient, RR, currentTime, float64(*in.Rr.RrMax))
	}
	if in.Spo2 != nil && *in.Spo2.Spo2Max != 0 {
		if *in.Spo2.Spo2Max != 255 {
			patientInfoDetails.lastUpdatedLiveSpo2			= *in.Spo2.Spo2Max
			patientInfoDetails.lastUpdatedLiveSpo2Time		= uint64(currentTime)
			bslnrCtxt.insertMetricInflux(uuidPatient, SPO2, currentTime, float64(*in.Spo2.Spo2Max))
		}
	}
	if in.Pr != nil && *in.Pr.PrMax != 0 {
		if *in.Spo2.Spo2Max != 255 {
			patientInfoDetails.lastUpdatedLivePr			=  *in.Pr.PrMax
			bslnrCtxt.insertMetricInflux(uuidPatient, PR, currentTime, float64(*in.Pr.PrMax))
		}
	}
	if in.Pi != nil && *in.Pi.PiMax !=  float32(0) {
		if *in.Spo2.Spo2Max != 255 {
			patientInfoDetails.lastUpdatedLivePi			= float32(*in.Pi.PiMax)
			bslnrCtxt.insertMetricInflux(uuidPatient, PI, currentTime, float64(*in.Pi.PiMax))
		}
	}
	if in.Temperature != nil && *in.Temperature.TemperatureMax != float32(0) {
		patientInfoDetails.lastUpdatedLiveTemperature			= float32(*in.Temperature.TemperatureMax)
		patientInfoDetails.lastUpdatedLiveTemperatureTime		=uint64(currentTime)
		bslnrCtxt.insertMetricInflux(uuidPatient, TEMPERATURE, currentTime, float64(*in.Temperature.TemperatureMax))
	}
	if in.Bp != nil  && *in.Bp.Bps != 0 {
		bslnrCtxt.log.Debugf("@rajsreen ---BP Current Update %d %d %d %v",  int32(*in.Bp.Bps),  int32(*in.Bp.Bpd), (((*in.Bp.Bps)*256) + *in.Bp.Bpd), *in.Bp.Time)
		patientInfoDetails.lastUpdatedLiveBp			= (((*in.Bp.Bps)*256) + *in.Bp.Bpd)
		patientInfoDetails.lastUpdatedLiveBPTime	= *in.Bp.Time
		bslnrCtxt.insertMetricInflux(uuidPatient, BP, int64(*in.Bp.Time), float64(((*in.Bp.Bps)*256) + *in.Bp.Bpd) )
	}

	if in.Weight != nil && *in.Weight !=  float32(0) {
		bslnrCtxt.log.Debugf("@rajsreen --Weight Current Update %s -- %d", uuidPatient, float32(*in.Weight))
		patientInfoDetails.lastUpdatedLiveWeight		= float32(*in.Weight)
		patientInfoDetails.lastUpdatedLiveWeightTime	= uint64(currentTime)
		bslnrCtxt.insertMetricInflux(uuidPatient, WEIGHT, currentTime, float64(patientInfoDetails.lastUpdatedLiveWeight))
	}

	if in.BatteryEcg != nil {
		patientInfoDetails.currbatteryEcg = *in.BatteryEcg
		if (uint64(currentTime) - patientInfoDetails.lastUpdatedBatteryEcg) > 600 {
			patientInfoDetails.lastUpdatedBatteryEcg = uint64(currentTime)
			bslnrCtxt.insertMetricInflux(uuidPatient, BATTERY_ECG, 
								currentTime, float64(*in.BatteryEcg))
		}
	}
	if in.BatterySpo2 != nil {
		patientInfoDetails.currbatterySpo2 = *in.BatterySpo2
		if (uint64(currentTime) - patientInfoDetails.lastUpdatedBatterySpo2) > 600 {
			patientInfoDetails.lastUpdatedBatterySpo2 = uint64(currentTime)
			bslnrCtxt.insertMetricInflux(uuidPatient, BATTERY_SPO2, 
								currentTime, float64(*in.BatterySpo2))
		}
	}
	if in.BatteryTemperature != nil {
		patientInfoDetails.currbatteryTemperature = *in.BatteryTemperature
		if (uint64(currentTime) - patientInfoDetails.lastUpdatedBatteryTemperature) > 600 {
			patientInfoDetails.lastUpdatedBatteryTemperature = uint64(currentTime)
			bslnrCtxt.insertMetricInflux(uuidPatient, BATTERY_TEMPERATURE, 
									currentTime, float64(*in.BatteryTemperature))
		}
	}
	if in.BatteryBp != nil {
		patientInfoDetails.currbatteryBp = *in.BatteryBp
		if (uint64(currentTime) - patientInfoDetails.lastUpdatedBatteryBp) > 600 {
			patientInfoDetails.lastUpdatedBatteryBp = uint64(currentTime)
			bslnrCtxt.insertMetricInflux(uuidPatient, BATTERY_TEMPERATURE, currentTime, float64(*in.BatteryBp))
		}
	}

	if in.BatteryGateway != nil {
		patientInfoDetails.currbatteryGateway = *in.BatteryGateway
		if (uint64(currentTime) - patientInfoDetails.lastUpdatedBatteryGateway) > 600 {
			patientInfoDetails.lastUpdatedBatteryGateway =uint64(currentTime)
			bslnrCtxt.insertMetricInflux(uuidPatient, BATTERY_GATEWAY, currentTime, float64(*in.BatteryGateway))
		}
	}

	//New patient calculate EWS score
	//if patientInfoDetails.lastUpdatedLiveTime == 0 {
	//	bslnrCtxt.calculateEwsLocked(patientInfoDetails)
	//}
	if in.CurrTime != nil {
		patientInfoDetails.lastUpdatedLiveTime = *in.CurrTime
	}
	bslnrCtxt.log.Debugf("patientCurrentUpdateDetails Entered T: %s P: %s Syst: %v Dias:%v W:%v", uuidTenant, uuidPatient, patientInfoDetails.bps, patientInfoDetails.bpd, patientInfoDetails.weight)

}

func (bslnrCtxt *baselinerContext) patientBaselineUpdateDetails(uuidTenant string, uuidPatient string, in *pb.BaselinesPatient) {

	patientInfoDetails := bslnrCtxt.patientMapFind(uuidTenant, uuidPatient)
	if patientInfoDetails == nil {
		patientInfoDetails = bslnrCtxt.createNewPatientContext(uuidTenant, uuidPatient)
	}

	patientInfoDetails.patientLck.Lock()
	defer patientInfoDetails.patientLck.Unlock()
	
	lenSplice := len(patientInfoDetails.hr[0])
	if lenSplice > 1 {
		patientInfoDetails.hr[1] =append([]int32{int32(*in.Hr.HrMax)},patientInfoDetails.hr[1][1:lenSplice]...)
		patientInfoDetails.hr[2] =append([]int32{int32(*in.Hr.HrMin)},patientInfoDetails.hr[2][1:lenSplice]...)
		patientInfoDetails.hr[3] =append([]int32{int32(*in.Hr.HrAvg)},patientInfoDetails.hr[3][1:lenSplice]...)
	} else {
		if in.Hr != nil {
			patientInfoDetails.hr[1] = append(patientInfoDetails.hr[1], int32(*in.Hr.HrMax))
			patientInfoDetails.hr[2] = append(patientInfoDetails.hr[2], int32(*in.Hr.HrMin))
			patientInfoDetails.hr[3] = append(patientInfoDetails.hr[3], int32(*in.Hr.HrAvg))
		}
		if in.Rr != nil {
			patientInfoDetails.rr[1] = append(patientInfoDetails.rr[1], int32(*in.Rr.RrMax))
			patientInfoDetails.rr[2] = append(patientInfoDetails.rr[2], int32(*in.Rr.RrMin))
			patientInfoDetails.rr[3] = append(patientInfoDetails.rr[3], int32(*in.Rr.RrAvg))
		}
		if in.Spo2 != nil {
			patientInfoDetails.spo2[1] = append(patientInfoDetails.spo2[1], int32(*in.Spo2.Spo2Max))
			patientInfoDetails.spo2[2] = append(patientInfoDetails.spo2[2], int32(*in.Spo2.Spo2Min))
			patientInfoDetails.spo2[3] = append(patientInfoDetails.spo2[3], int32(*in.Spo2.Spo2Avg))
		}
		if in.Pr != nil {
			patientInfoDetails.pr[1] = append(patientInfoDetails.pr[1], int32(*in.Pr.PrMax))
			patientInfoDetails.pr[2] = append(patientInfoDetails.pr[2], int32(*in.Pr.PrMin))
			patientInfoDetails.pr[3] = append(patientInfoDetails.pr[3], int32(*in.Pr.PrAvg))
		}
		if in.Pi != nil {
			patientInfoDetails.pi[1] = append(patientInfoDetails.pi[1], float32(*in.Pi.PiMax))
			patientInfoDetails.pi[2] = append(patientInfoDetails.pi[2], float32(*in.Pi.PiMin))
			patientInfoDetails.pi[3] = append(patientInfoDetails.pi[3], float32(*in.Pi.PiAvg))
		}
		if in.Temperature != nil {				
			patientInfoDetails.temperature[1] = append(patientInfoDetails.temperature[1], float32(*in.Temperature.TemperatureMax))
			patientInfoDetails.temperature[2] = append(patientInfoDetails.temperature[2], float32(*in.Temperature.TemperatureMin))
			patientInfoDetails.temperature[3] = append(patientInfoDetails.temperature[3], float32(*in.Temperature.TemperatureAvg))
			/*
			//@rajsreen= temp hack for older SDK
			patientInfoDetails.temperature[1] = append(patientInfoDetails.temperature[1], float32(((*in.Spo2.Spo2Max + 5)-32))*0.55)
			patientInfoDetails.temperature[2] = append(patientInfoDetails.temperature[2], float32(((*in.Spo2.Spo2Min +5)-32))*0.55)
			patientInfoDetails.temperature[3] = append(patientInfoDetails.temperature[3], float32(((*in.Spo2.Spo2Avg + 5)-32))*0.55)
			*/
			
		}
	}
	if in.CurrTime != nil {
		patientInfoDetails.lastUpdatedTrendTime = *in.CurrTime
	}
	//bslnrCtxt.calculateEwsLocked(patientInfoDetails)
	patientInfoDetails.ews = append(patientInfoDetails.ews, patientInfoDetails.currEws)

	if in.BatteryEcg != nil {
		patientInfoDetails.batteryEcg = append(patientInfoDetails.batteryEcg, *in.BatteryEcg)
	}
	if in.BatterySpo2 != nil {
		patientInfoDetails.batterySpo2 = append(patientInfoDetails.batterySpo2, *in.BatterySpo2)
	}
	if in.BatteryTemperature != nil {
		patientInfoDetails.batteryTemperature = append(patientInfoDetails.batteryTemperature, *in.BatteryTemperature)
	}
	if in.BatteryGateway != nil {
		patientInfoDetails.batteryGateway = append(patientInfoDetails.batteryGateway, *in.BatteryGateway)
	}
	if in.Bp != nil {
		patientInfoDetails.bps[1] = append(patientInfoDetails.bps[1], *in.Bp.Bps)
		patientInfoDetails.bpd[1] = append(patientInfoDetails.bpd[1], *in.Bp.Bpd)
	}
	
	if in.Sugar != nil {
		patientInfoDetails.sugar = append(patientInfoDetails.sugar, *in.Sugar)
	}
	if in.Weight != nil {
		patientInfoDetails.weight[1] = append(patientInfoDetails.weight[1], *in.Weight)
	}

	bslnrCtxt.log.Debugf("patientBaselineUpdateDetails Entered T: %s P: %s  bp:%v w:%v", uuidTenant, uuidPatient, patientInfoDetails.bps, patientInfoDetails.weight)
}


// RegisterPatient : API for registration of Patient UUID and kafka registration
func (s *BaselinerServer) BaselineSend(context context.Context, in *pb.BaselinesPatient) (*pb.PatientRegisterRsp, error) {
	bslnrCtxt := s.bslnrCtxt
	uuidPatient := strings.TrimSpace(*in.PatientUUID)
	uuidTenant := strings.TrimSpace(*in.TenantUUID)

	//bslnrCtxt.log.Debugf("[Baseline Send] Entered %s %d %v", uuidPatient, *in.BaselineOp, *in)
	
	if *in.BaselineOp == pb.BaselineOperation_BSLN_OP_AGGREGATE {
		bslnrCtxt.patientBaselineUpdateDetails(uuidTenant, uuidPatient, in)
	} else if *in.BaselineOp == pb.BaselineOperation_BSLN_OP_CURRENT {
		bslnrCtxt.patientCurrentUpdateDetails(uuidTenant, uuidPatient, in)
	}
	rsp := true
	return &pb.PatientRegisterRsp{Rsp: &rsp}, nil
}

func (bslnrCtxt *baselinerContext) populateAllSensorTrends(storage tstorage.Storage, uuidPatient string,
							trendsPatient *pb.TrendsPatient, 
							startTime int64, endTime int64, nHistory int) {

	//bslnrCtxt.populateTrendInflux(uuidPatient, nHistory)


	/*================= ECG ===============*/
	bslnrCtxt.populateTrend(storage, uuidPatient, 
		HR, SENDINT, startTime, endTime, 
		trendsPatient, 0, 0, nHistory)

	bslnrCtxt.populateTrend(storage, uuidPatient, 
	RR, SENDINT, startTime, endTime, 
	trendsPatient, 0, 0, nHistory)

	/*================= ECG ===============*/


	/*================= SPO2 ===============*/
	bslnrCtxt.populateTrend(storage, uuidPatient, 
	SPO2, SENDINT, startTime, endTime, 
	trendsPatient, 255, 0, nHistory)


	if trendsPatient.LastUpdatedLiveSpo2Time != nil {
		bslnrCtxt.populateTrend(storage, uuidPatient, 
			PR, SENDINT, startTime, endTime, 
			trendsPatient, 0, *trendsPatient.LastUpdatedLiveSpo2Time, nHistory)
		bslnrCtxt.populateTrend(storage, uuidPatient, 
				PI, SENDFLOAT, startTime, endTime, 
				trendsPatient, 0, *trendsPatient.LastUpdatedLiveSpo2Time, nHistory)
	}

	/*================= SPO2 ===============*/

	/*================= TEMPERATURE ===============*/
	bslnrCtxt.populateTrend(storage, uuidPatient, 
		TEMPERATURE, SENDFLOAT, startTime, endTime, 
		trendsPatient, 0, 0, nHistory)
	/*================= TEMPERATURE ===============*/

	/*================= WEIGHT ===============*/
	bslnrCtxt.populateTrend(storage, uuidPatient, 
						WEIGHT, SENDFLOAT, startTime, endTime, 
						trendsPatient, 0, 0, nHistory)
	/*================= WEIGHT ===============*/

	/*================= BP ===============*/
	points, _ := bslnrCtxt.readMetric(storage, uuidPatient, BP, startTime, endTime,nHistory );
	var p *tstorage.DataPoint
	for _, p = range points {

		if  p.Value > 0 && p.Value < 80000 {
			trendDataPoint := new(pb.TrendDataPoint)
			tStamp				:= uint64(p.Timestamp)
			val					:= int32(p.Value)
			trendDataPoint.ValInt = &val
			trendDataPoint.TimeStamp =  &tStamp
			trendsPatient.Trendbp = append(trendsPatient.Trendbp, trendDataPoint)
		}
	}
	/*
	if  p != nil && (p.Value > 0 && p.Value < 80000) {
		liveBP := int32(p.Value)
		lastUpdatedLiveBPTime 		:= uint64(p.Timestamp)
		trendsPatient.Livebp 		= &(liveBP)
		trendsPatient.LastUpdatedLiveBPTime = &(lastUpdatedLiveBPTime)
	}
	/*================= BP ===============*/					

}



func getFloat(unk interface{}) (float64, error) {
	var floatType = reflect.TypeOf(float64(0))
	v := reflect.ValueOf(unk)
	v = reflect.Indirect(v)
	if !v.Type().ConvertibleTo(floatType) {
		return 0, fmt.Errorf("cannot convert %v to float64", v.Type())
	}
	fv := v.Convert(floatType)
	return fv.Float(), nil
}

func (bslnrCtxt *baselinerContext) populateTrendInflux(uuidPatient string, trendsPatient *pb.TrendsPatient, 
														nHistory int) {

	nHistoryStr := "-" + strconv.Itoa(nHistory) + "h"
	var trendMetric *[]*pb.TrendDataPoint
	// Get query client
	if bslnrCtxt.influxClient == nil {
		return
	}
	queryAPI := bslnrCtxt.influxClient.QueryAPI(influxorg)

	  // Query. You need to change a bit the Query from the Query Builder
    // Otherwise it won't work
    fluxQuery := fmt.Sprintf(`from(bucket: "%s")
							|> range(start:%s)
							|> filter(fn: (r) => r["_measurement"] == "%s")
							|> yield(name: "data")`, influxdbDatabase , nHistoryStr, uuidPatient)

	//bslnrCtxt.log.Debugf("Reading frm influx %v ==== %v\n %s", time.Unix(startTime, 0), time.Unix(endTime, 0), fluxQuery)

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
			//bslnrCtxt.log.Errorf("row----: %v\n", result.Record().String())
			//bslnrCtxt.log.Errorf("row----: %v \n", result.Record().Time().Unix())
			//bslnrCtxt.log.Errorf("row----: %v \n", result.Record().Field())
			//bslnrCtxt.log.Errorf("row----: %v \n", result.Record().Value())


			metric:= string(result.Record().Field())
			trendDataPoint := new(pb.TrendDataPoint)
			temp ,_:= getFloat(result.Record().Value())		

			val					:= float32(temp)
			tStamp					:= uint64(result.Record().Time().Unix())
			trendDataPoint.TimeStamp =  &tStamp
			trendDataPoint.ValFloat = &val
			//bslnrCtxt.log.Errorf("row----: %s %f -- %f \n", metric, val, tStamp)

			switch metric {
			case HR:
				trendMetric = &trendsPatient.Trendrr
			case RR:
				trendMetric = &trendsPatient.Trendrr
			case SPO2:
				trendMetric = &trendsPatient.Trendspo2
			case PI:
				trendMetric = &trendsPatient.Trendpi
			case PR:
				trendMetric = &trendsPatient.Trendpr
			case TEMPERATURE:
				trendMetric = &trendsPatient.Trendtemperature
			case WEIGHT:
				trendMetric = &trendsPatient.Trendweight
			case BP:
				if val > 0 && val < 80000 {
					trendMetric = &trendsPatient.Trendbp
				} else {
					trendDataPoint = nil
					continue
				}
			case BATTERY_ECG:
				trendMetric = &trendsPatient.TrendbatteryEcg
			case BATTERY_SPO2:
				trendMetric = &trendsPatient.TrendbatterySpo2
			case BATTERY_TEMPERATURE:
				trendMetric = &trendsPatient.TrendbatteryTemperature
			case BATTERY_GATEWAY:
				trendMetric = &trendsPatient.TrendbatteryGateway
			}

			*trendMetric = append(*trendMetric, trendDataPoint)
		}
		
		
		if result.Err() != nil {
			bslnrCtxt.log.Errorf("Query error: %s\n", result.Err().Error())
		}
	} else {
		bslnrCtxt.log.Errorf("Reading influx err: Patient %s, %v",uuidPatient, err)
	}
		
}

func (bslnrCtxt *baselinerContext) populateTrend(storage tstorage.Storage, uuidPatient string, 
					metric string, datatype int,  startTime int64, endTime int64, 
					trendsPatient *pb.TrendsPatient, invalid int, 
					invalidTstamp uint64, nHistory int)  {
	
	points, _ :=bslnrCtxt.readMetric(storage, uuidPatient, metric, startTime, endTime, nHistory );
	
	var p *tstorage.DataPoint



	var trendMetric *[]*pb.TrendDataPoint
	switch metric {
	case HR:
		trendMetric = &trendsPatient.Trendhr

	case RR:
        trendMetric = &trendsPatient.Trendrr

    case SPO2:
        trendMetric = &trendsPatient.Trendspo2

	case PI:
        trendMetric = &trendsPatient.Trendpi

	case PR:
        trendMetric = &trendsPatient.Trendpr

	case TEMPERATURE:
        trendMetric = &trendsPatient.Trendtemperature

	case WEIGHT:
        trendMetric = &trendsPatient.Trendweight
	}

	for _, p = range points {
		trendDataPoint := new(pb.TrendDataPoint)
		tStamp				:= uint64(p.Timestamp)
		if datatype == 1 {
			val					:= int32(p.Value)
			trendDataPoint.ValInt = &val
			/*
			if invalid != 0 || invalidTstamp !=0 {
				if (invalid != 0 && val != int32(invalid)) ||  (invalidTstamp != 0 && invalidTstamp == tStamp) {
					liveValInt = val
				}
			}
			if (invalid != 0 && val != int32(invalid)) || (invalidTstamp != 0 && invalidTstamp == tStamp) {
				liveValInt = val
			}
			*/
		} else {
			val					:= float32(p.Value)
			trendDataPoint.ValFloat = &val
			/*
			if invalid != 0 || invalidTstamp !=0 {
				if (invalid != 0 && val != float32(invalid)) || (invalidTstamp != 0 && invalidTstamp == tStamp) {
					liveValFloat = val
				}
			} else {
				liveValFloat = val
			}
			*/
		}
		
		trendDataPoint.TimeStamp =  &tStamp
		*trendMetric = append(*trendMetric, trendDataPoint)
		//bslnrCtxt.log.Debugf("%s : %s timestamp: %v, value: %v\n",uuidPatient, metric,  p.Timestamp, p.Value, liveValInt, liveValFloat)

	}
	
	/*
	if p != nil {
		if datatype == 1 {
			*lastLiveVal = &(liveValInt)
		} else {
			bslnrCtxt.log.Debugf("@rajsreen---entered p not nil, %v",liveValFloat , p.Value )
			*lastLiveFloat = &liveValFloat
		}
		liveTstamp =  uint64(p.Timestamp)
		*lastLiveValTstamp = &liveTstamp
	}
	if metric == WEIGHT {
		bslnrCtxt.log.Debugf("@rajsreen --- %p === %p --- %v  --- %v\n",&trendsPatient.Liveweight, lastLiveFloat, liveValFloat, liveVal)
	}
	*/
}

// GetTrendsPatient : API for registration of Patient UUID and kafka registration
func (s *BaselinerServer) GetTrendsPatient(context context.Context, in *pb.TrendsPatientReq) (*pb.TrendsPatient, error) {
	bslnrCtxt := s.bslnrCtxt
	uuidPatient := strings.TrimSpace(*in.PatientUUID)
	uuidTenant := strings.TrimSpace(*in.TenantUUID)
	nHistory := 12
	foldUp 		:= false


	if in.Duration != nil{
		nHistory, foldUp = getDurationFoldup(*in.Duration)
	}
	
	bslnrCtxt.log.Debugf("[GetTrendsPatient] Entered T: %s P: %s %v", uuidTenant, uuidPatient, *in)

	endTime := time.Now().Unix()
	startTime := time.Now().Unix() - int64((nHistory*60 * 60))
	pmap := bslnrCtxt.tenantMapFind(uuidTenant)
	if pmap == nil {
		bslnrCtxt.log.Debugf("@TENANT NOT FOUND ... %s", uuidTenant)
		rsp := false
		return &pb.TrendsPatient{Rsp: &rsp}, nil
	} 
	//lastValidIntVal := int32(255)
	//lastValidFloatVal := float32(0)
	

	patientInfo := bslnrCtxt.patientMapFind(uuidTenant, uuidPatient)
	if patientInfo ==nil {
		rsp := false
		return &pb.TrendsPatient{Rsp: &rsp}, nil
	}
	
	bslnrCtxt.log.Debugf("[GetTrendsPatient] Entered T: %s P: %s %d %v Time: %v %v", uuidTenant, uuidPatient, patientInfo.bps, 
						patientInfo.weight, startTime, endTime)

	patientInfo.patientLck.Lock()
	trendsPatient := new(pb.TrendsPatient)
	trendsPatient.PatientUUID = &uuidPatient
	
	trendsPatient.Ews 		= &patientInfo.currEws
	
	trendsPatient.BatteryEcg 			=  &patientInfo.currbatteryEcg
	trendsPatient.BatterySpo2 			=  &patientInfo.currbatterySpo2
	trendsPatient.BatteryTemperature 	=  &patientInfo.currbatteryTemperature
	trendsPatient.BatteryGateway 		=  &patientInfo.currbatteryGateway

	if patientInfo.lastUpdatedLiveHr != EINVAL {
		trendsPatient.Livehr = &patientInfo.lastUpdatedLiveHr
		trendsPatient.LastUpdatedLiveEcgTime = &patientInfo.lastUpdatedLiveEcgTime
		trendsPatient.Liverr = &patientInfo.lastUpdatedLiveRr
	}

	if patientInfo.lastUpdatedLiveSpo2 != EINVAL {
		trendsPatient.Livespo2 = &patientInfo.lastUpdatedLiveSpo2
		trendsPatient.LastUpdatedLiveSpo2Time = &patientInfo.lastUpdatedLiveSpo2Time
		trendsPatient.Livepr = &patientInfo.lastUpdatedLivePr
		trendsPatient.Livepi = &patientInfo.lastUpdatedLivePi
	}

	if patientInfo.lastUpdatedLiveTemperature != EINVAL {
		trendsPatient.Livetemperature = &patientInfo.lastUpdatedLiveTemperature
		trendsPatient.LastUpdatedLiveTemperatureTime = &patientInfo.lastUpdatedLiveTemperatureTime
	}

	if patientInfo.lastUpdatedLiveBp != EINVAL {
		trendsPatient.Livebp = &patientInfo.lastUpdatedLiveBp
		trendsPatient.LastUpdatedLiveBPTime = &patientInfo.lastUpdatedLiveBPTime
	}

	if patientInfo.lastUpdatedLiveWeight != EINVAL {
		trendsPatient.Liveweight = &patientInfo.lastUpdatedLiveWeight
		trendsPatient.LastUpdatedLiveWeightTime = &patientInfo.lastUpdatedLiveWeightTime
	}

	bslnrCtxt.populateTrendInflux(uuidPatient,trendsPatient, nHistory)
	//bslnrCtxt.populateAllSensorTrends(pmap.storage, uuidPatient,trendsPatient, startTime, endTime, nHistory )

	/*
	points, _ :=bslnrCtxt.readMetric(pmap.storage, uuidPatient, SPO2, startTime, endTime );
	var p *tstorage.DataPoint
	for _, p = range points {
		trendDataPoint := new(pb.TrendDataPoint)
		val					:= int32(p.Value)
		tStamp				:= uint64(p.Timestamp)
		trendDataPoint.ValInt = &val
		trendDataPoint.TimeStamp =  &tStamp
		trendsPatient.Trendspo2 = append(trendsPatient.Trendspo2, trendDataPoint)
		bslnrCtxt.log.Debugf("SPO2 timestamp: %v, value: %v\n", p.Timestamp, p.Value)
		if int32(p.Value) != 255 {
			patientInfo.spo2[0][0] = int32(p.Value)
			trendsPatient.Livespo2 = &(patientInfo.spo2[0][0])
			patientInfo.lastUpdatedLiveSpo2Time = uint64(p.Timestamp)
			trendsPatient.LastUpdatedLiveSpo2Time = &(patientInfo.lastUpdatedLiveSpo2Time)
		}
	}

	points, _ =bslnrCtxt.readMetric(pmap.storage, uuidPatient, PR, startTime, endTime );
	for _, p = range points {
		val					:= int32(p.Value)
		tStamp				:= uint64(p.Timestamp)
		trendDataPoint.ValInt = &val
		trendDataPoint.TimeStamp =  &tStamp
		trendsPatient.Trendpr = append(trendsPatient.Trendpr, trendDataPoint)
		bslnrCtxt.log.Debugf("SPO2 timestamp: %v, value: %v\n", p.Timestamp, p.Value)
		if uint64(p.Timestamp) == patientInfo.lastUpdatedLiveSpo2Time {
			(patientInfo.pr[0][0]) = int32(p.Value)
			trendsPatient.Livepr =&(patientInfo.pr[0][0])
		}
	}
	points, _ =bslnrCtxt.readMetric(pmap.storage, uuidPatient, PI, startTime, endTime );
	for _, p = range points {
		trendsPatient.Trendpi = append(trendsPatient.Trendpi, float32(p.Value))
		bslnrCtxt.log.Debugf("SPO2 timestamp: %v, value: %v\n", p.Timestamp, p.Value)
		if uint64(p.Timestamp) == patientInfo.lastUpdatedLiveSpo2Time {
			(patientInfo.pi[0][0]) = float32(p.Value)
			trendsPatient.Livepi =  &(patientInfo.pi[0][0])
		}
	}
	/*================= SPO2 ===============*/


	/*================= TEMPERATURE ===============
	points, _ =bslnrCtxt.readMetric(pmap.storage, uuidPatient, TEMPERATURE, startTime, endTime );
	for _, p = range points {
		trendsPatient.Trendtemperature = append(trendsPatient.Trendtemperature, float32(p.Value))
		bslnrCtxt.log.Debugf("TEMPERATURE timestamp: %v, value: %v\n", p.Timestamp, p.Value)
		
	}
	(patientInfo.temperature[0][0]) = float32(p.Value)
	patientInfo.lastUpdatedLiveTemperatureTime =uint64 (p.Timestamp)
	trendsPatient.Livetemperature = &(patientInfo.temperature[0][0])
	trendsPatient.LastUpdatedLiveTemperatureTime = &(patientInfo.lastUpdatedLiveTemperatureTime)
	/*================= TEMPERATURE ===============*/

	/*================= WEIGHT ===============
	points, _ =bslnrCtxt.readMetric(pmap.storage, uuidPatient, WEIGHT, startTime, endTime );
	for _, p = range points {
		trendsPatient.Trendweight = append(trendsPatient.Trendweight, float32(p.Value))
		bslnrCtxt.log.Debugf("WEIGHT timestamp: %v, value: %v\n", p.Timestamp, p.Value)
	}
	(patientInfo.weight[0][0]) 	= float32(p.Value)
	patientInfo.lastUpdatedLiveWeightTime 	= uint64(p.Timestamp)
	trendsPatient.Liveweight 	= &(patientInfo.weight[0][0])
	trendsPatient.LastUpdatedLiveWeightTime	= &(patientInfo.lastUpdatedLiveWeightTime)
	/*================= WEIGHT ===============*/

	/*================= BP ===============
	points, _ =bslnrCtxt.readMetric(pmap.storage, uuidPatient, BP, startTime, endTime );
	for _, p = range points {
		if  p.Value > 0 && p.Value < 80000 {
			trendsPatient.Trendbps = append(trendsPatient.Trendbps, int32(int32(p.Value)/256))
			trendsPatient.Trendbpd = append(trendsPatient.Trendbpd, int32(int32(p.Value)%256))
			bslnrCtxt.log.Debugf("BP timestamp: %v, value: %v\n", p.Timestamp, p.Value)
		}
	}
	if  p.Value > 0 && p.Value < 80000 {
		(patientInfo.bps[0][0]) =int32(int32(p.Value)/256)
		(patientInfo.bpd[0][0]) =int32(int32(p.Value)%256)
		patientInfo.lastUpdatedLiveBPTime 		= uint64(p.Timestamp)
		trendsPatient.Livebps 		= &(patientInfo.bps[0][0])
		trendsPatient.Livebpd 		= &(patientInfo.bpd[0][0])
		trendsPatient.LastUpdatedLiveBPTime = &(patientInfo.lastUpdatedLiveBPTime)
	}
	/*================= BP ===============*/




	



	if !foldUp {
		

		if len(patientInfo.ews) > nHistory {
			trendsPatient.Trendews = patientInfo.ews[len(patientInfo.ews)-nHistory:]
		} else {
			trendsPatient.Trendews = patientInfo.ews[:]
		}
		

	}
	
	patientInfo.patientLck.Unlock()
	rsp := true
	return &pb.TrendsPatient{Rsp: &rsp, PatientUUID: trendsPatient.PatientUUID, 
		Livehr: trendsPatient.Livehr, Liverr: trendsPatient.Liverr, LastUpdatedLiveEcgTime: trendsPatient.LastUpdatedLiveEcgTime,
		Livespo2: trendsPatient.Livespo2, Livepr: trendsPatient.Livepr, Livepi: trendsPatient.Livepi, LastUpdatedLiveSpo2Time: trendsPatient.LastUpdatedLiveSpo2Time,
		Livetemperature: trendsPatient.Livetemperature, LastUpdatedLiveTemperatureTime: trendsPatient.LastUpdatedLiveTemperatureTime,
		Livebp: trendsPatient.Livebp, LastUpdatedLiveBPTime: trendsPatient.LastUpdatedLiveBPTime,
		Liveweight: trendsPatient.Liveweight, LastUpdatedLiveWeightTime:  trendsPatient.LastUpdatedLiveWeightTime,
		Trendhr:trendsPatient.Trendhr, Trendrr:trendsPatient.Trendrr, 
		Trendspo2:trendsPatient.Trendspo2, Trendpi:trendsPatient.Trendpi, Trendpr:trendsPatient.Trendpr,
		Trendtemperature: trendsPatient.Trendtemperature, Trendbp: trendsPatient.Trendbp, Trendweight:trendsPatient.Trendweight,
		Ews: trendsPatient.Ews,
		LastUpdatedLiveTime: trendsPatient.LastUpdatedLiveTime, LastUpdatedTrendTime: trendsPatient.LastUpdatedTrendTime,
		Trendews: trendsPatient.Trendews, 
		BatteryEcg: trendsPatient.BatteryEcg, BatterySpo2: trendsPatient.BatterySpo2, BatteryTemperature: trendsPatient.BatteryTemperature,  
		BatteryGateway: trendsPatient.BatteryGateway, 
		TrendbatteryEcg: trendsPatient.TrendbatteryEcg, TrendbatterySpo2: trendsPatient.TrendbatterySpo2, 
		TrendbatteryTemperature: trendsPatient.TrendbatteryTemperature,  TrendbatteryGateway: trendsPatient.TrendbatteryGateway,
		
		}, nil
}

// GetSortedTrendsPatient : API for registration of Patient UUID and kafka registration
func (s *BaselinerServer) GetSortedTrendsPatient(context context.Context, in *pb.Pagination) (*pb.AllPatientTrends, error) {
	bslnrCtxt := s.bslnrCtxt
	numEntries := *in.PgNumEntries
	pgIdx := *in.PgIdx
	startIdx := pgIdx //numEntries*pgIdx // TODO ...for now idx is offset
	bslnrCtxt.log.Debugf("[GetSortedTrendsPatient]")
	nHistory := 12
	foldUp 		:= false
	uuidTenant := strings.TrimSpace(*in.TenantUUID)
	found		:= false

	uuidPatientList := in.PatientUUIDList

	
	if in.Duration != nil{
		nHistory, foldUp = getDurationFoldup(*in.Duration)
	}

	bslnrCtxt.log.Debugf("[GetSortedTrendsPatient] Entered Sorted trends NumEntries: %d StartIdxIn: %d Duration: %d startIdx %d T:%s\n %v\n",*in.PgNumEntries,*in.PgIdx, *in.Duration, startIdx, uuidTenant, uuidPatientList)
	pmap := bslnrCtxt.tenantMapFind(uuidTenant)
	if pmap == nil {
		trndsAllPatient := make([]*pb.TrendsPatient, 0) 
		numPatients := int32(0)
		bslnrCtxt.log.Debugf("@TENANT NOT FOUND ... %s", uuidTenant)
		return &pb.AllPatientTrends{TrndsPtnt: trndsAllPatient, NumPatients: &numPatients}, nil

	} 

	pmap.patientMapLock.Lock()
	defer pmap.patientMapLock.Unlock()

	i := int32(0)
	numSent := i

	trndsAllPatient := make([]*pb.TrendsPatient, 0) 

	//endTime := time.Now().Unix()
	//startTime := time.Now().Unix() - int64((nHistory*60 * 60))
	//lastValidFloatVal := float32(0)
	
	for _, pt := range pmap.sortedEwsList {

		if i >= startIdx {
			if numSent >= numEntries {
				break
			}
			
			if uuidPatientList != nil {
				found = bslnrCtxt.find(uuidPatientList, pt)
				if !found {
					bslnrCtxt.log.Debugf("[@rajsreen---sortedEwsList] %s --- not found in uuidPatientList", pt)
					continue
				}
			}
			
			bslnrCtxt.log.Debugf("[@rajsreen---sortedEwsList] %s", pt)
			patientInfo := bslnrCtxt.patientMapFindlocked(pmap, pt)
			if patientInfo ==nil {
				bslnrCtxt.log.Debugf("[@rajsreen---sortedEwsList] %s --- IS NOT FOUND", pt)
				continue
			}
			

			patientInfo.patientLck.Lock()
			trendsPatient := new(pb.TrendsPatient)
			uuidPatient := patientInfo.uuidPatient
			trendsPatient.PatientUUID = &uuidPatient
			trendsPatient.Livehr 	= &(patientInfo.hr[0][0])
			trendsPatient.Liverr 	= &(patientInfo.rr[0][0])
			trendsPatient.Livespo2 	= &(patientInfo.spo2[0][0])
			trendsPatient.Livepr 	= &(patientInfo.pr[0][0])
			trendsPatient.Livepi 	= &(patientInfo.pi[0][0])
			trendsPatient.Livetemperature = &(patientInfo.temperature[0][0])

			bslnrCtxt.log.Debugf("[@rajsreen---sorted all] %d, %f", patientInfo.bpd[0][0],patientInfo.weight[0][0] )
			trendsPatient.Ews 		= &patientInfo.currEws
			trendsPatient.LastUpdatedLiveTime =  &patientInfo.lastUpdatedLiveTime
			trendsPatient.LastUpdatedTrendTime =  &patientInfo.lastUpdatedTrendTime
			
			trendsPatient.BatteryEcg			= &patientInfo.currbatteryEcg
			trendsPatient.BatterySpo2			= &patientInfo.currbatterySpo2
			trendsPatient.BatteryTemperature	= &patientInfo.currbatteryTemperature
			trendsPatient.BatteryGateway		= &patientInfo.currbatteryGateway

			bslnrCtxt.log.Debugf("Patient %s , ews %d", uuidPatient,patientInfo.currEws )
			
					
			if patientInfo.lastUpdatedLiveHr != EINVAL {
				trendsPatient.Livehr = &patientInfo.lastUpdatedLiveHr
				trendsPatient.LastUpdatedLiveEcgTime = &patientInfo.lastUpdatedLiveEcgTime
				trendsPatient.Liverr = &patientInfo.lastUpdatedLiveRr
			}
		
			if patientInfo.lastUpdatedLiveSpo2 != EINVAL {
				trendsPatient.Livespo2 = &patientInfo.lastUpdatedLiveSpo2
				trendsPatient.LastUpdatedLiveSpo2Time = &patientInfo.lastUpdatedLiveSpo2Time
				trendsPatient.Livepr = &patientInfo.lastUpdatedLivePr
				trendsPatient.Livepi = &patientInfo.lastUpdatedLivePi
			}
		
			if patientInfo.lastUpdatedLiveTemperature != EINVAL {
				trendsPatient.Livetemperature = &patientInfo.lastUpdatedLiveTemperature
				trendsPatient.LastUpdatedLiveTemperatureTime = &patientInfo.lastUpdatedLiveTemperatureTime
			}
		
			if patientInfo.lastUpdatedLiveBp != EINVAL {
				trendsPatient.Livebp = &patientInfo.lastUpdatedLiveBp
				trendsPatient.LastUpdatedLiveBPTime = &patientInfo.lastUpdatedLiveBPTime
			}
		
			if patientInfo.lastUpdatedLiveWeight != EINVAL {
				trendsPatient.Liveweight = &patientInfo.lastUpdatedLiveWeight
				trendsPatient.LastUpdatedLiveWeightTime = &patientInfo.lastUpdatedLiveWeightTime
			}
			bslnrCtxt.populateTrendInflux(uuidPatient,trendsPatient, nHistory)
			//bslnrCtxt.populateAllSensorTrends(pmap.storage, uuidPatient,trendsPatient, startTime, endTime, nHistory )

			
			if !foldUp {
				if len(patientInfo.ews) > nHistory {
					trendsPatient.Trendews = patientInfo.ews[len(patientInfo.ews)-nHistory:]
				} else {
					trendsPatient.Trendews = patientInfo.ews[:]
				}
			}
			
			patientInfo.patientLck.Unlock()
			trndsAllPatient = append(trndsAllPatient, trendsPatient)
			numSent++
		}
		i++
	}
	numPatients := pmap.numPatients

	bslnrCtxt.log.Debugf("Numpatients - %d", numPatients)
	return &pb.AllPatientTrends{TrndsPtnt: trndsAllPatient, NumPatients: &numPatients, CurrentIdx: &i }, nil
}

