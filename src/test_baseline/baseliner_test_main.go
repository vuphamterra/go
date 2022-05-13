package main

/* listed alphabetically */
import (
	
	"fmt"
	pb "test_baseline/proto_out/emr_consumer_proto"
	"context"
	"time"
	"google.golang.org/grpc"
	"os"
)

const (
	baselinerAddress1 = "baseliner:9010"
)

func printGetAllSortedPatients(tenantUUID string, page int32, nEntries int32) {

	conn, _ := grpc.Dial(baselinerAddress1, grpc.WithInsecure(), grpc.WithBlock())
	defer conn.Close()
	
	c := pb.NewEmrT1RpcClient(conn)
	
	if c == nil {
		fmt.Println("Unable to connect to Baseliner")
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 5000 * time.Millisecond)
	if ctx == nil {
		fmt.Println("Context nil %s ")
		return
	}
	defer cancel()

	fmt.Println("tenantUUID %s\n", tenantUUID)
	req := new(pb.Pagination)
	pg := int32(page)
	entries := int32(nEntries)
	duration := pb.FetchDuration_FETCH_DURATION_8HR

	req.PgNumEntries = &entries
	req.PgIdx = &pg
	req.Duration = &duration
	req.TenantUUID = &tenantUUID
	if rsp, e := c.GetSortedTrendsPatient(ctx, req); e != nil {
		fmt.Println("Was not able to insert %v : %v", req , e)
	} else {
		fmt.Println("Rsp %v", rsp)
	}
}


func printDetailsPatient(tenantUUID string, uuidPatient string) {

	conn, _ := grpc.Dial(baselinerAddress1, grpc.WithInsecure(), grpc.WithBlock())
	defer conn.Close()
	
	c := pb.NewEmrT1RpcClient(conn)
	
	if c == nil {
		fmt.Println("Unable to connect to Baseliner")
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 5000 * time.Millisecond)
	if ctx == nil {
		fmt.Println("Context nil %s ")
		return
	}
	defer cancel()

	req := new(pb.TrendsPatientReq)
	duration := pb.FetchDuration_FETCH_DURATION_1WK

	req.TenantUUID = &tenantUUID
	req.PatientUUID = &uuidPatient
	req.Duration = &duration
	if rsp, e := c.GetTrendsPatient(ctx, req); e != nil {
		fmt.Println("Was not able to insert %v : %v", req , e)
	} else {
		fmt.Println("Rsp %v", rsp)
	}
}


func main() {
	tenantUUID := ""
	patientUUID := ""
	fmt.Println("Arguments ", len(os.Args),  os.Args[1])

	if len(os.Args) == 2 {
		tenantUUID = os.Args[1]
		printGetAllSortedPatients(tenantUUID, 0, 10)
	} else if len(os.Args) == 3 {
		tenantUUID  = os.Args[1]
		patientUUID = os.Args[2]
		printDetailsPatient(tenantUUID, patientUUID)
	}
	

	//printGetAllSortedPatients(tenantUUID, 0, 10)
	//printGetAllSortedPatients(11, 10)
	//printGetAllSortedPatients(21, 10)
	//printDetailsPatient("patient04d32f91-38d1-4baa-8942-2a3f80bf9eb0")
}
