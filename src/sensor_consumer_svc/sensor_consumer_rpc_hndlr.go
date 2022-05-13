package main

/* listed alphabetically */
import (
	_ "bytes"
	"context"
	_ "log"
	_ "math"
	_ "sort"
	_ "strconv"
	"strings"
	"time"
	//pb "github.com/rajsreen/sensor_consumer_svc/proto"

	pb "sensor_consumer_svc/proto_out/emr_consumer_proto"
	"google.golang.org/grpc"

)

// EmrT1Server gRPC server helper
type EmrT1Server struct {
	t1Ctxt *emrTier1Context
}

// NewEmrT1Server - returns the pointer to the implementation.
func NewEmrT1Server() *EmrT1Server {
	return &EmrT1Server{}
}

// RegisterPatient : API for registration of Patient UUID and kafka registration
func (s *EmrT1Server) RegisterPatient(context context.Context, in *pb.PatientRegister) (*pb.PatientRegisterRsp, error) {
	t1Ctxt := s.t1Ctxt
	patientUUID := strings.TrimSpace(*in.PatientUUID)

	t1Ctxt.log.Debugf("[Register Patient] Entered %s", patientUUID)
	t1Ctxt.createTopicKafka("topics_old",patientUUID)
	t1Ctxt.patientTopicRegisterListenerSpawn("", patientUUID)

	// Need to spwan off the topic as a kafka consumer and complete registration

	rsp := true
	return &pb.PatientRegisterRsp{Rsp: &rsp}, nil

}



// RegisterPatient : API for registration of Patient UUID and kafka registration
func (s *EmrT1Server) DeRegisterPatient(context context.Context, in *pb.PatientRegister) (*pb.PatientRegisterRsp, error) {
	t1Ctxt := s.t1Ctxt
	patientUUID := strings.TrimSpace(*in.PatientUUID)

	t1Ctxt.log.Debugf("[DeRegister Patient] Entered %s", patientUUID)
	t1Ctxt.createTopicKafka("topics_old",patientUUID)
	t1Ctxt.patientTopicRegisterListenerSpawn("", patientUUID)

	// Need to spwan off the topic as a kafka consumer and complete registration

	rsp := true
	return &pb.PatientRegisterRsp{Rsp: &rsp}, nil
}


// RegisterPatient : API for registration of Patient UUID and kafka registration
func (s *EmrT1Server) BaselineSend(context context.Context, in *pb.BaselinesPatient) (*pb.PatientRegisterRsp, error) {
	// DUMMY ...not to be used as client in sensor_consumer
	rsp := true
	return &pb.PatientRegisterRsp{Rsp: &rsp}, nil
}



// Client Send Routines
func (t1Ctxt *emrTier1Context) processBaselineChannel(i int) {
	t1Ctxt.startGoRoutine(func() {
		idx := i
		for {

			t1Ctxt.log.Infof("Starting processBaselineChannel idx %d ", idx)
			baselinerconn, _ := grpc.Dial(baselinerAddress1, grpc.WithInsecure(), grpc.WithBlock())
			ctx := context.Background()
			t1Ctxt.log.Infof("Started processBaselineChannel idx %d ", idx)
			for {
				baselineData := <-t1Ctxt.baselineChannel[i]
				//t1Ctxt.log.Infof("Rcvd on baseliner channel %v ", baselineData)

				c := pb.NewEmrT1RpcClient(baselinerconn)
				if c == nil {
					t1Ctxt.log.Errorf("Unable to connect to Baseliner %s ", baselineData)
					break //do we need a retry?
				}
				
				ctx, _ = context.WithTimeout(context.Background(), 5000 * time.Millisecond)
				if ctx == nil {
					t1Ctxt.log.Errorf("Context nil %s ", baselineData )
					continue
				}
		
				if _, e := c.BaselineSend(ctx, baselineData); e != nil {
					t1Ctxt.log.Errorf("Was not able to insert %s : %v", baselineData , e)
				}
				baselineData = nil
			}
			if baselinerconn != nil {
				baselinerconn.Close()
			}
			baselinerconn = nil	
			ctx = nil
		}
	})
}


// GetTrendsPatient : API for registration of Patient UUID and kafka registration
func (s *EmrT1Server) GetTrendsPatient(context context.Context, in *pb.TrendsPatientReq) (*pb.TrendsPatient, error) {
	rsp := true
	return &pb.TrendsPatient{Rsp: &rsp}, nil
}
// GetTrendsPatient : API for registration of Patient UUID and kafka registration
func (s *EmrT1Server) GetSortedTrendsPatient(context context.Context, in *pb.Pagination) (*pb.AllPatientTrends, error) {
	trndsAllPatient := make([]*pb.TrendsPatient, 1)
	trendsPatient := new(pb.TrendsPatient)
	trndsAllPatient[0] = trendsPatient
	return &pb.AllPatientTrends{TrndsPtnt: trndsAllPatient}, nil
}
