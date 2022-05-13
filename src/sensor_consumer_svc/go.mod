module sensor_consumer_svc

go 1.15

require (
	github.com/Shopify/sarama v1.28.0
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/golang/protobuf v1.5.2
	github.com/influxdata/influxdb-client-go/v2 v2.3.0
	github.com/onsi/ginkgo v1.15.2 // indirect
	github.com/onsi/gomega v1.11.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/segmentio/kafka-go v0.4.12
	go.uber.org/zap v1.16.0
	gonum.org/v1/gonum v0.9.1
	google.golang.org/grpc v1.36.1
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)
