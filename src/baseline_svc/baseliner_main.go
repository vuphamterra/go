package main

/* listed alphabetically */
import (
	_ "context"
	_ "crypto/tls"
	"flag"
	"fmt"
	_ "io"
	_ "io/ioutil"
	"log"
	_ "math"
	"net/http"
	_ "net/http/pprof"
	"os"
	_ "strconv"
	"sync"
	_ "sync/atomic"
	"time"
	"bytes"
	"net"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxdb2api "github.com/influxdata/influxdb-client-go/v2/api"


	_ "github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"github.com/nakabonne/tstorage"

	//credentials "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	_ "google.golang.org/grpc/metadata"
	_ "google.golang.org/grpc/peer"
)

const emrLogPath = "/var/log/baseliner_live247.log"

const NUMENTRIES = 100
const NUM_DB_BUCKETS = 16

type grpcConnProfile struct {
	tmOutSec           time.Duration
	rcvMsgSizMax       uint32
	sndMsgSizMax       uint32
	localKeepAliveSrvr keepalive.ServerParameters
	keepAliveSrvr      keepalive.ServerParameters
	keepAliveClnt      keepalive.ClientParameters
	keepAliveEnfP      keepalive.EnforcementPolicy
}

type baselinerStd struct {
	dataPort    string
	ctrlPort    string
	agntPort    string
	pubChnlSiz  uint32
	grpcProfile grpcConnProfile
}

type baselinerConfig struct {
	debug        bool
	secure       bool
	testMode     bool
	cfgIP        string
	grpcSrvrCert string
	grpcSrvrKey  string
}

type PatientSensorReadings struct {
	uuidPatient string
	lastUpdatedLiveTime uint64
	lastUpdatedTrendTime uint64
	
	lastUpdatedLiveEcgTime			uint64;
	lastUpdatedLiveSpo2Time 		uint64;
    lastUpdatedLiveTemperatureTime	uint64;
    lastUpdatedLiveWeightTime 		uint64;
    lastUpdatedLiveBPTime 			uint64;
	
	lastUpdatedBatteryEcg			uint64;
	lastUpdatedBatterySpo2 			uint64;
    lastUpdatedBatteryTemperature	uint64;
    lastUpdatedBatteryBp 			uint64;
    lastUpdatedBatteryGateway 		uint64;


	lastUpdatedLiveSpo2 		int32
	lastUpdatedLivePr 			int32
	lastUpdatedLivePi 			float32
	lastUpdatedLiveHr 			int32
	lastUpdatedLiveRr 			int32
	lastUpdatedLiveTemperature 	float32
	lastUpdatedLiveBp 			int32
	lastUpdatedLiveWeight 		float32
	lastUpdatedLiveSugar 		int32



	patientLck  sync.RWMutex
	hr			[][]int32
	rr			[][]int32
	spo2		[][]int32
	pr			[][]int32
	pi			[][]float32
	temperature	[][]float32
	ews 		[]int32
	bps 		[][]int32
	bpd 		[][]int32
	sugar 		[]int32
	weight 		[][]float32


	foldUphr			[][]int32
	foldUprr			[][]int32
	foldUpspo2			[][]int32
	foldUppr			[][]int32
	foldUppi			[][]float32
	foldUptemperature	[][]float32
	foldUpews 		[]int32
	foldUpbps 		[][]int32
	foldUpbpd 		[][]int32
	foldUpsugar 	[]int32
	foldUpweight 	[][]float32


	currEws 		int32
	currBps 		int32
	currBpd 		int32
	currSugar 		int32

	currbatteryGateway		 int32
	currbatterySpo2 		int32
	currbatteryTemperature	 int32
	currbatteryEcg			 int32
	currbatteryBp	 int32


	batteryGateway  []int32
	batterySpo2     []int32
	batteryTemperature []int32
	batteryEcg      []int32
	foldUpbatteryGateway  []int32
	foldUpbatterySpo2     []int32
	foldUpbatteryTemperature []int32
	foldUpbatteryEcg      []int32
}

type pmap struct {
	patientMapLock	 sync.RWMutex
	patientMap 		map[string]*PatientSensorReadings
	sortedEwsList   []string
	numPatients      int32
	dbHashBucket     int32
	storage 		tstorage.Storage
}

// ObjSyncCtxt : main module structure
// Osync stats will be updated only when VS is being deleted
// Total current stats will thus be osctxt.stats + stats
// across all active VSes
type baselinerContext struct {
	log              *zap.SugaredLogger
	cfg              baselinerConfig
	wg               sync.WaitGroup
	std              baselinerStd
	tenantMapLock	 sync.RWMutex
	baselineWorkerChannel     [40]chan *bytes.Buffer
	tenantMap		map[string]*pmap
	numTenants      int32
	influxAuthToken  string
	influxClient	influxdb2.Client
	writeAPI 		influxdb2api.WriteAPIBlocking

}

func getLogEncoder() zapcore.Encoder {
	return zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
}

func getLogWriter() zapcore.WriteSyncer {

	return zapcore.AddSync(&lumberjack.Logger{
		Filename:   emrLogPath,
		MaxSize:    50, // megabytes
		MaxBackups: 10,
		MaxAge:     20, // days
	  })
}

func initLogger() *zap.SugaredLogger {
	writerSyncer := getLogWriter()
	encoder := getLogEncoder()
	core := zapcore.NewCore(encoder, writerSyncer, zapcore.DebugLevel)
	logger := zap.New(core, zap.AddCaller())
	return logger.Sugar()
}

func baselinerSetup(bslnrCtxt *baselinerContext) {

	bslnrCtxt.log = initLogger()
	bslnrCtxt.tenantMap = make(map[string]*pmap)
	//bslnrCtxt.patientMap = make(map[string]*PatientSensorReadings)

	if bslnrCtxt.tenantMap == nil {
		bslnrCtxt.log.Errorf("Failed to allocate tenant Map. Exiting")
		os.Exit(1)
	}
	log.Println("baseliner setup done...")
}

func baselinerStart(bslnrCtxt *baselinerContext) {
	bslnrCtxt.log.Infof("Starting Listening Server baseliner...")


	for {
		if bslnrCtxt.readInfluxAuthFile() == true {
			break
		}
		log.Println("Trying to read influxAuthfile")
		time.Sleep(time.Duration( 2* time.Second))
	}

	/*============= Influx initializations ==========*/
	httpClientInflux := &http.Client{
        Timeout: time.Second * time.Duration(60),
        Transport: &http.Transport{
            DialContext: (&net.Dialer{
                Timeout: 5 * time.Second,
            }).DialContext,

            MaxIdleConns:        50,
            MaxIdleConnsPerHost: 50,
            IdleConnTimeout:     90 * time.Second,
        },
    }
	bslnrCtxt.influxClient = influxdb2.NewClientWithOptions(influxdbSvr, bslnrCtxt.influxAuthToken,
												influxdb2.DefaultOptions().SetBatchSize(10).SetHTTPClient(httpClientInflux).SetPrecision(time.Minute))
	bslnrCtxt.writeAPI = bslnrCtxt.influxClient.WriteAPIBlocking("test_org", influxdbDatabase)
	defer bslnrCtxt.influxClient.Close()
	/*============= Influx initializations ==========*/




	log.Println("setting up lstng ctxt")
	bslnrCtxt.startGoRoutine(func() {
		var err error
		for err == nil {
			err = bslnrCtxt.grpcServerSetup(bslnrCtxt.cfg.cfgIP, bslnrCtxt.cfg.secure)
		}
		bslnrCtxt.wg.Done()
	})

	for i := 0; i < 30; i++ {
		bslnrCtxt.baselineWorkerChannel[i] = make(chan *bytes.Buffer, 4096)
		bslnrCtxt.processBaselineWorkerChannel(i)
		time.Sleep(300* time.Millisecond)
	}

	go bslnrCtxt.listenOnPatientDiscoveryChannel()

	bslnrCtxt.startGoRoutine(func() {
		bslnrCtxt.foldUpRoutine()
	})
	bslnrCtxt.startGoRoutine(func() {
		bslnrCtxt.storeToDbRoutine()
	})
	
	bslnrCtxt.populateTenantPatientDetails()
	bslnrCtxt.fetchValuesFromRedis()

	


	
	// pprof
	log.Println(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", 7777), nil))

	bslnrCtxt.wg.Wait()
}

func parseArgs(bslnrCtxt *baselinerContext) {
	help := false

	/* Setup constants */
	bslnrCtxt.std.ctrlPort = "9010"
	bslnrCtxt.std.pubChnlSiz = 2048
	bslnrCtxt.std.grpcProfile.tmOutSec = time.Duration(5) * time.Second

	/*
		MaxConnectionIdle:     10 * time.Second, // If a client is idle for 10 seconds, send a GOAWAY
		MaxConnectionAge:      30 * time.Second, // If any connection is alive for more than 30 seconds, send a GOAWAY
	*/
	bslnrCtxt.std.grpcProfile.localKeepAliveSrvr = keepalive.ServerParameters{
		//MaxConnectionAgeGrace: 10 * time.Second, // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
		Time:    30 * time.Second, // Ping the client if it is idle for 30 seconds to ensure the connection is still active
		Timeout: 20 * time.Second, // Wait 20 seconds for the ping ack before assuming the connection is dead
	}

	bslnrCtxt.std.grpcProfile.keepAliveSrvr = keepalive.ServerParameters{
		MaxConnectionAgeGrace: 5 * time.Second, // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
		Time:    2 * time.Second, // Ping the client if it is idle for 5 seconds to ensure the connection is still active
		Timeout: 1 * time.Second, // Wait 2 seconds for the ping ack before assuming the connection is dead
	}

	bslnrCtxt.std.grpcProfile.keepAliveEnfP = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // Min time a client should wait before sending a keepalive ping.
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	bslnrCtxt.std.grpcProfile.keepAliveClnt = keepalive.ClientParameters{
		Time:                10 * time.Second, // Send pings every 10 seconds if there is no activity
		Timeout:             2 * time.Second,  // wait 2 seconds for ping ack before considering the connection dead
		PermitWithoutStream: true,             // send pings even without active streams
	}

	log.Println("args: ", (len(os.Args) - 1))

	flag.BoolVar(&help, "help", false, "Display help & exit")
	flag.BoolVar(&bslnrCtxt.cfg.debug, "debug", true, "Enable debugging")
	flag.BoolVar(&bslnrCtxt.cfg.secure, "secure", false, "Enable secure transport")
	flag.BoolVar(&bslnrCtxt.cfg.testMode, "test", false, "Enable dev test mode")

	flag.Parse()

	if help {
		flag.PrintDefaults()
		return
	}

	log.Println("debug: ", bslnrCtxt.cfg.debug)

	// XXX - fix path later
	if bslnrCtxt.cfg.secure {
		bslnrCtxt.cfg.secure = false
		certPath := "/root/osync-certs/"
		_, err := os.Stat(certPath + "peer.crt")
		if err == nil {
			_, err = os.Stat(certPath + "peer.key")
			if err == nil {
				bslnrCtxt.cfg.secure = true
				bslnrCtxt.cfg.grpcSrvrCert = certPath + "peer.crt"
				bslnrCtxt.cfg.grpcSrvrKey = certPath + "peer.key"
			}
		}

		if !bslnrCtxt.cfg.secure {
			log.Println("secure cert & key path validation failed")
			log.Println("secure: ", bslnrCtxt.cfg.secure)
		}
	}
	bslnrCtxt.cfg.cfgIP = "0.0.0.0:9010"
}

func main() {
	var bslnrCtxt baselinerContext

	fmt.Println("Starting baseliner svc")

	parseArgs(&bslnrCtxt)
	baselinerSetup(&bslnrCtxt)

	baselinerStart(&bslnrCtxt)
}
