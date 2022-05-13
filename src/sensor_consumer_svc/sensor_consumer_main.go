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
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	_ "strconv"
	"sync"
	_ "sync/atomic"
	"time"
	"bytes"


	_ "github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	pb "sensor_consumer_svc/proto_out/emr_consumer_proto"

	//credentials "google.golang.org/grpc/credentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	_ "google.golang.org/grpc/metadata"
	_ "google.golang.org/grpc/peer"
)

const emrLogPath = "/var/log/sensor_consumer_live247.log"

type grpcConnProfile struct {
	tmOutSec           time.Duration
	rcvMsgSizMax       uint32
	sndMsgSizMax       uint32
	localKeepAliveSrvr keepalive.ServerParameters
	keepAliveSrvr      keepalive.ServerParameters
	keepAliveClnt      keepalive.ClientParameters
	keepAliveEnfP      keepalive.EnforcementPolicy
}

type emrT1Std struct {
	dataPort    string
	ctrlPort    string
	agntPort    string
	pubChnlSiz  uint32
	grpcProfile grpcConnProfile
}

type emrTier1Config struct {
	debug        bool
	secure       bool
	testMode     bool
	cfgIP        string
	grpcSrvrCert string
	grpcSrvrKey  string
}

type PatientData struct {
	uuidPatient string
	bps int32
	bpd int32
	sugar int32
}

type emrTier1Context struct {
	log              *zap.SugaredLogger
	cfg              emrTier1Config
	wg               sync.WaitGroup
	std              emrT1Std
	deepAnalyserconn *grpc.ClientConn
	myClient         *http.Client
	httpClientInfluxCached         *http.Client
	alertChannel     [40]chan *bytes.Buffer
	baselineChannel  [20]chan *pb.BaselinesPatient
	influxAuthToken  string
	patientMapLock	 sync.RWMutex
	patientMap 		map[string] *PatientData

}

func getLogEncoder() zapcore.Encoder {
	return zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	//return zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
}

func getLogWriter() zapcore.WriteSyncer {

	return zapcore.AddSync(&lumberjack.Logger{
		Filename:   emrLogPath,
		MaxSize:    50, // megabytes
		MaxBackups: 10,
		MaxAge:     20, // days
	  })
	//  file, err := os.OpenFile(emrLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	//if err != nil {
	//	log.Println("Unable to open log file ", err)
	//}
	//return zapcore.AddSync(file)
}

func initLogger() *zap.SugaredLogger {
	writerSyncer := getLogWriter()
	encoder := getLogEncoder()
	core := zapcore.NewCore(encoder, writerSyncer, zapcore.DebugLevel)
	logger := zap.New(core, zap.AddCaller())
	return logger.Sugar()
}

func emrTier1Setup(t1Ctxt *emrTier1Context) {

	t1Ctxt.log = initLogger()
	t1Ctxt.patientMap = make(map[string]*PatientData)

	if t1Ctxt.patientMap == nil {
		t1Ctxt.log.Errorf("Failed to allocate patient Map. Exiting")
		os.Exit(1)
	}
	//osCtxt.log = initLogger()

	log.Println("emr Tier1 setup done...")
}

func emrTier1Start(t1Ctxt *emrTier1Context) {
	t1Ctxt.log.Infof("Starting Listening Server Tier1...")

	log.Println("setting up lstng ctxt")
	t1Ctxt.startGoRoutine(func() {
		var err error
		for err == nil {
			err = t1Ctxt.grpcServerSetup(t1Ctxt.cfg.cfgIP, t1Ctxt.cfg.secure)
		}
		t1Ctxt.wg.Done()
	})

	/*
	// Customize the Transport to have larger connection pool
	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	if !ok {
		panic(fmt.Sprintf("defaultRoundTripper not an *http.Transport"))
	}
	defaultTransport := *defaultTransportPointer // dereference it to get a copy of the struct that the pointer points to
	defaultTransport.MaxIdleConns = 100
	defaultTransport.MaxIdleConnsPerHost = 100
*/

	// ***************************************** //
	// Create HTTP client for influxdb1
	// ***************************************** //
	httpclientTmp := &http.Client{
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
	t1Ctxt.httpClientInfluxCached = httpclientTmp
	for i := 0; i < 40; i++ {
		t1Ctxt.alertChannel[i] = make(chan *bytes.Buffer, 4096)
		t1Ctxt.processAlertChannel(i)
		time.Sleep(300* time.Millisecond)
	}

	for i := 0; i < 20; i++ {
		t1Ctxt.baselineChannel[i] = make(chan *pb.BaselinesPatient, 4096)
		t1Ctxt.processBaselineChannel(i)
		time.Sleep(300* time.Millisecond)
	}



	t1Ctxt.dialDeepAnalyser()
	go t1Ctxt.listenOnPatientDiscoveryChannel()

	for {
		if t1Ctxt.readInfluxAuthFile() == true {
			break
		}
		log.Println("Trying to read influxAuthfile")
		time.Sleep(time.Duration( 2* time.Second))
	}
	t1Ctxt.readRegisterTenants()
	// pprof
	log.Println(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", 7777), nil))

	t1Ctxt.wg.Wait()
}

func parseArgs(t1Ctxt *emrTier1Context) {
	help := false

	/* Setup constants */
	t1Ctxt.std.ctrlPort = "9001"
	t1Ctxt.std.pubChnlSiz = 2048
	t1Ctxt.std.grpcProfile.tmOutSec = time.Duration(5) * time.Second

	/*
		MaxConnectionIdle:     10 * time.Second, // If a client is idle for 10 seconds, send a GOAWAY
		MaxConnectionAge:      30 * time.Second, // If any connection is alive for more than 30 seconds, send a GOAWAY
	*/
	t1Ctxt.std.grpcProfile.localKeepAliveSrvr = keepalive.ServerParameters{
		//MaxConnectionAgeGrace: 10 * time.Second, // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
		Time:    30 * time.Second, // Ping the client if it is idle for 30 seconds to ensure the connection is still active
		Timeout: 20 * time.Second, // Wait 20 seconds for the ping ack before assuming the connection is dead
	}

	t1Ctxt.std.grpcProfile.keepAliveSrvr = keepalive.ServerParameters{
		MaxConnectionAgeGrace: 5 * time.Second, // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
		Time:    2 * time.Second, // Ping the client if it is idle for 5 seconds to ensure the connection is still active
		Timeout: 1 * time.Second, // Wait 2 seconds for the ping ack before assuming the connection is dead
	}

	t1Ctxt.std.grpcProfile.keepAliveEnfP = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // Min time a client should wait before sending a keepalive ping.
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	t1Ctxt.std.grpcProfile.keepAliveClnt = keepalive.ClientParameters{
		Time:                10 * time.Second, // Send pings every 10 seconds if there is no activity
		Timeout:             2 * time.Second,  // wait 2 seconds for ping ack before considering the connection dead
		PermitWithoutStream: true,             // send pings even without active streams
	}

	log.Println("args: ", (len(os.Args) - 1))

	flag.BoolVar(&help, "help", false, "Display help & exit")
	flag.BoolVar(&t1Ctxt.cfg.debug, "debug", true, "Enable debugging")
	flag.BoolVar(&t1Ctxt.cfg.secure, "secure", false, "Enable secure transport")
	flag.BoolVar(&t1Ctxt.cfg.testMode, "test", false, "Enable dev test mode")

	flag.Parse()

	if help {
		flag.PrintDefaults()
		return
	}

	log.Println("debug: ", t1Ctxt.cfg.debug)

	// XXX - fix path later
	if t1Ctxt.cfg.secure {
		t1Ctxt.cfg.secure = false
		certPath := "/root/osync-certs/"
		_, err := os.Stat(certPath + "peer.crt")
		if err == nil {
			_, err = os.Stat(certPath + "peer.key")
			if err == nil {
				t1Ctxt.cfg.secure = true
				t1Ctxt.cfg.grpcSrvrCert = certPath + "peer.crt"
				t1Ctxt.cfg.grpcSrvrKey = certPath + "peer.key"
			}
		}

		if !t1Ctxt.cfg.secure {
			log.Println("secure cert & key path validation failed")
			log.Println("secure: ", t1Ctxt.cfg.secure)
		}
	}
	t1Ctxt.cfg.cfgIP = "0.0.0.0:9001"
}

func main() {
	var t1Ctxt emrTier1Context

	fmt.Println("Starting emrTier1")

	parseArgs(&t1Ctxt)
	emrTier1Setup(&t1Ctxt)
	emrTier1Start(&t1Ctxt)
}
