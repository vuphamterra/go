package main

const NUM_ROWS_SIMPLE=1
const NUM_ROWS_HR=4
const NUM_ROWS_BP=2
const HR = "HR"
const PR = "PR"
const PI = "PI"
const RR = "RR"
const SPO2 = "SPO2"
const TEMPERATURE = "TEMPERATURE"
const WEIGHT = "WEIGHT"
const BP = "BP"
const BATTERY_ECG 			= "BATTERY_ECG"
const BATTERY_SPO2 			= "BATTERY_SPO2"
const BATTERY_TEMPERATURE 	= "BATTERY_TEMPERATURE"
const BATTERY_GATEWAY 	= "BATTERY_GATEWAY"

const SENDINT=1
const SENDFLOAT=0
const EINVAL=999


func initializeSensorInt( rows int)([][]int32, [][]int32) {

	msrmnt1 := make([][]int32, rows)
	for i := 0; i < rows; i++ {
		msrmnt1[i] = make([]int32, 1)
	}
	msrmnt2 := make([][]int32, rows)
	for i := 0; i < rows; i++ {
		msrmnt2[i] = make([]int32, 1)
	}	
	return msrmnt1, msrmnt2
}


func initializeSensorFloat( rows int)([][]float32, [][]float32) {

	msrmnt1 := make([][]float32, rows)
	for i := 0; i < rows; i++ {
		msrmnt1[i] = make([]float32, 1)
	}	

	msrmnt2 := make([][]float32, rows)
	for i := 0; i < rows; i++ {
		msrmnt2[i] = make([]float32, 1)
	}	
	return msrmnt1, msrmnt2
}

func (bslnrCtxt *baselinerContext) initializeMeasurement( patientInfo *PatientSensorReadings,  measurement string) {

	switch {
		case measurement == "HR":
			patientInfo.hr, patientInfo.foldUphr = initializeSensorInt(NUM_ROWS_HR)
		case measurement == "RR":
			patientInfo.rr, patientInfo.foldUprr = initializeSensorInt(NUM_ROWS_HR)
		case measurement == "PR":
			patientInfo.pr, patientInfo.foldUppr = initializeSensorInt(NUM_ROWS_HR)
		case measurement == "SPO2":
			patientInfo.spo2, patientInfo.foldUpspo2 = initializeSensorInt(NUM_ROWS_HR)
		case measurement == "PI":
			patientInfo.pi, patientInfo.foldUppi = initializeSensorFloat(NUM_ROWS_HR)
		case measurement == "TEMPERATURE":
			patientInfo.temperature, patientInfo.foldUptemperature = initializeSensorFloat(NUM_ROWS_HR)
		case measurement == "BP":
			patientInfo.bps, patientInfo.foldUpbps = initializeSensorInt(NUM_ROWS_BP)
			patientInfo.bpd, patientInfo.foldUpbpd = initializeSensorInt(NUM_ROWS_BP)
		case measurement == "WEIGHT":
			patientInfo.weight, patientInfo.foldUpweight = initializeSensorFloat(NUM_ROWS_BP)
		default: 
	}
}