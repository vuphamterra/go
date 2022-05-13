package main

import (
	"log"
	"net"
	"net/http"
	"fmt"
	"context"
	"math"
	//"encoding/json"


	"io/ioutil"

	"time"

	pb "sensor_consumer_svc/proto_out/emr_consumer_proto"

	"google.golang.org/grpc"
	credentials "google.golang.org/grpc/credentials"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"

)

func (t1Ctxt *emrTier1Context) startGoRoutine(f func()) {
	t1Ctxt.wg.Add(1)
	go f()
}

func (t1Ctxt *emrTier1Context) processAlertChannel(i int) {
	t1Ctxt.startGoRoutine(func() {


		myClient := &http.Client{
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
		/*
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns = 500
		t.MaxConnsPerHost = 500
		t.MaxIdleConnsPerHost = 500
		t.IdleConnTimeout = 300
		
		myClient := &http.Client{
			Timeout:   5 * time.Second,
			Transport: t,
		}
		*/
		for {
			alertData := <-t1Ctxt.alertChannel[i]
			resp, err := myClient.Post(alertaApp1, "application/json", alertData)
			if err != nil {
				t1Ctxt.log.Errorf("An Error Occured during alert %v %v", err, alertData)
				t1Ctxt.alertChannel[i] <- alertData
				time.Sleep(1 * time.Second)
				continue
			}
			ioutil.ReadAll(resp.Body)
			alertData = nil
			resp.Body.Close()	
		}
	})
}



func (t1Ctxt *emrTier1Context) dialDeepAnalyser() *grpc.ClientConn {

	var err error
	address := "127.0.0.1:50051"

	t1Ctxt.log.Infof("Dialing to %s", address)

	t1Ctxt.deepAnalyserconn, err = grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())

	if err != nil {
		log.Println("did not connect: %v", err)
		if t1Ctxt.deepAnalyserconn != nil {
			t1Ctxt.deepAnalyserconn.Close()
			t1Ctxt.deepAnalyserconn = nil
		}
	}
	return t1Ctxt.deepAnalyserconn
}

func (t1Ctxt *emrTier1Context) grpcServerSetup(lstngIPPort string, secure bool) error {
	var s *grpc.Server

	l, err := net.Listen("tcp", lstngIPPort)
	if err != nil {
		t1Ctxt.log.Errorf("failed to listen: %v", err)
		return err
	}

	if secure {
		creds, err := credentials.NewServerTLSFromFile(t1Ctxt.cfg.grpcSrvrCert, t1Ctxt.cfg.grpcSrvrKey)
		if err != nil {
			t1Ctxt.log.Errorf("could not load TLS keys: %s", err)
			l = nil
			return err
		}

		t1Ctxt.log.Infof("Creating secure server %s", lstngIPPort)
		s = grpc.NewServer(grpc.Creds(creds), grpc.KeepaliveParams(t1Ctxt.std.grpcProfile.keepAliveSrvr))
	} else {
		ctrlLstngIPPort := "127.0.0.1:" + t1Ctxt.std.ctrlPort
		t1Ctxt.log.Infof("Creating insecure server: %s", lstngIPPort)
		if lstngIPPort == ctrlLstngIPPort {
			// Lets keep Localhost channel without keepalive for now.
			s = grpc.NewServer()
			//s = grpc.NewServer(grpc.KeepaliveParams(t1Ctxt.std.grpcProfile.localKeepAliveSrvr))
		} else {
			s = grpc.NewServer(grpc.KeepaliveParams(t1Ctxt.std.grpcProfile.keepAliveSrvr))
		}
	}

	osSrvr := NewEmrT1Server()
	osSrvr.t1Ctxt = t1Ctxt
	pb.RegisterEmrT1RpcServer(s, osSrvr)

	if err := s.Serve(l); err != nil {
		t1Ctxt.log.Errorf("failed to serve: %v", err)
		s = nil
		l = nil
		/* fallthrough */
	}

	return err
}


// Miscellaneous functions...mostly not used

func Convolve(input []int32) ([]int32, error) {

	kernels := [5]int32{0, 0, 32768, 0, 0}
	if !(len(input) > len(kernels)) {
		return nil, fmt.Errorf("provided data set is not greater than the filter weights")
	}

	output := make([]int32, len(input))
	for i := 0; i < len(kernels); i++ {
		var sum int32

		for j := 0; j < i; j++ {
			sum += (input[j] * kernels[len(kernels)-(1+i-j)])
		}
		output[i] = sum
	}

	for i := len(kernels); i < len(input); i++ {
		var sum int32
		for j := 0; j < len(kernels); j++ {
			sum += (input[i-j] * kernels[j])
		}
		output[i] = sum
	}

	return output, nil
}

// RegisterPatient : API for registration of Patient UUID and kafka registration
//func (s *EmrT1Server) ReadSensorHistoryPatient(context context.Context, in *pb.PatientRegister) (*pb.PatientRegisterRsp, error) {
func (t1Ctxt *emrTier1Context)  ReadSensorHistoryPatient() {

		//t1Ctxt := s.t1Ctxt
		//patientUUID := strings.TrimSpace(*in.PatientUUID)
		//t1Ctxt.log.Debugf("[Query History Patient] Entered %s", patientUUID)
	
	
		client1 := influxdb2.NewClientWithOptions(influxdbSvr, "my-token", influxdb2.DefaultOptions().SetHTTPClient(t1Ctxt.httpClientInfluxCached))
		queryAPI := client1.QueryAPI("")
		measrmnt := "patientcaa36e30-6f0a-4758-9dbd-e9e4a569d2ea" + "_ecg"
		
		//valstr := "from(bucket:\"emr_dev/1year\")|> range(start: 2020-10-06T18:53:10Z, stop:2020-10-06T18:53:21Z) |> filter(fn: (r) => r._measurement == "+ measrmnt +")"
		valstr:= "from(bucket:\"emr_dev/1year\")|> range(start: 2020-10-06T18:53:10Z, stop:2020-10-06T18:53:21Z) |> filter(fn: (r) => r._measurement == \""+ measrmnt +"\" and r._field == \"ecg\")"
		
		//result, err := queryAPI.Query(context.Background(),"from(bucket:\"emr_dev/1year\")|> range(start: 2020-10-06T18:53:10Z, stop:2020-10-06T18:53:21Z) |> filter(fn: (r) => r._measurement == \"patientcaa36e30-6f0a-4758-9dbd-e9e4a569d2ea_ecg\")")
		result, err := queryAPI.Query(context.Background(),valstr)
		finalStr := ""
		if err == nil {
			// Use Next() to iterate over query result lines
			for result.Next() {
				// Observe when there is new grouping key producing new table
				//if result.TableChanged() {
				//	t1Ctxt.log.Debugf("table: %s\n", result.TableMetadata().String())
				//}
				// read result
				tmp := fmt.Sprintf("%s", result.Record().Value());
				finalStr = finalStr + tmp
				//t1Ctxt.log.Debugf("row: %s\n", result.Record().Value())
			}
			if result.Err() != nil {
				t1Ctxt.log.Debugf("Query error: %s\n", result.Err().Error())
			}
		} else {
			t1Ctxt.log.Debugf("Query error: %v\n", err)
		}
		t1Ctxt.log.Debugf(" %s\n",finalStr)
	
}
func getLastNonZeroIdx(my_list []int16) int {
	for i, s := range my_list {
		if s == 0 {
			return i
		}
	}
	return -1
}

/**************************************************************/
/**************************************************************/
// Filterer is the interface used for filters
type Filterer interface {
	Filter(arr []float64) []float64
}

// LPFilter is a first order low pass filter usig H(p) = 1 / (1 + pRC)
type LPFilter struct {
	alpha float64
	rc    float64
}

// NewLPFilter creates a new low pass Filter
func NewLPFilter(cutoff, sampleRate float64) *LPFilter {
	rc := 1 / (cutoff * 2 * math.Pi)
	dt := 1 / sampleRate
	alpha := dt / (rc + dt)
	return &LPFilter{alpha: alpha, rc: rc}
}

// XXX: This filter could be improved
// Filter filters the given array of values, panics if array is empty
func (lp *LPFilter) Filter(arr []int32) []int32 {
	res := make([]int32, 0, len(arr))

	// First value should be arr[0] / RC using the initial value theorem
	res = append(res, int32(float64(arr[0])*lp.alpha))

	for i := range arr[:len(arr)-1] {
		tmp := res[i] + int32(lp.alpha*(float64(arr[i+1]/1000)-float64(res[i])))
		res = append(
			res,
			// Formula used is:
			// y(n+1) = y(n) + alpha * (x(n+1) -y(n))
			tmp,
		)
	}
	return res
}

/*
func marshalBinary(i ecgDataVector) ([]byte, error) {
	return json.Marshal(i)
}

func marshalBinaryf64(i []float64) ([]byte, error) {
	return json.Marshal(i)
}
*/
	