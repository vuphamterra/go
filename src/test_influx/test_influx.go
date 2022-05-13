package main

import (
	"context"
	"fmt"
	"time"
	"strings"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	//client "github.com/influxdata/influxdb1-client/v2"
	//client "github.com/influxdata/influxdb1-client/v2"
)

/*
func ExampleClient_Query() {
	host, err := url.Parse(fmt.Sprintf("http://%s:%d", "localhost", 8086))
	if err != nil {
		log.Fatal(err)
	}
	con, err := client.NewClient(client.Config{URL: *host})
	if err != nil {
		log.Fatal(err)
	}

	q := client.Query{
		Command:  "select count(value) from shapes",
		Database: "square_holes",
	}
	if response, err := con.Query(q); err == nil && response.Error() == nil {
		log.Println(response.Results)
	}
}
*/

/*

func ExampleClient_Write2() {
	host, err := url.Parse(fmt.Sprintf("http://%s:%d", "10.0.0.4", 8086))
	if err != nil {
		log.Fatal(err)
	}
	con, err := client.NewClient(client.Config{URL: *host})
	if err != nil {
		log.Fatal(err)
	}

	var (
		shapes     = []string{"circle", "rectangle", "square", "triangle"}
		colors     = []string{"red", "blue", "green"}
		sampleSize = 100
		pts        = make([]client.Point, sampleSize)
	)

	rand.Seed(42)
	//recordTime, _ := client.EpochToTime(1602010203424, "1")
	for i := 0; i < sampleSize; i++ {
		pts[i] = client.Point{
			Measurement: "test",
			Tags: map[string]string{
				"color": strconv.Itoa(rand.Intn(len(colors))),
				"shape": strconv.Itoa(rand.Intn(len(shapes))),
			},
			Fields: map[string]interface{}{
				"value": rand.Intn(sampleSize),
			},
			Time:      time.Unix(1602010203424, 0),
			Precision: "s",
		}
	}

	bps := client.BatchPoints{
		Points:          pts,
		Database:        "example",
		RetentionPolicy: "autogen",
	}
	_, err = con.Write(bps)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_Write() {

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://10.0.0.4:8086",
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}
	defer c.Close()

	var (
		shapes     = []string{"circle", "rectangle", "square", "triangle"}
		colors     = []string{"red", "blue", "green"}
		sampleSize = 1000
		pts        = make([]client.Point, sampleSize)
	)

	rand.Seed(42)

	for i := 0; i < sampleSize; i++ {
		pts[i] = client.Point{
			Name: "shapes",
			Tags: map[string]string{
				"color": strconv.Itoa(rand.Intn(len(colors))),
				"shape": strconv.Itoa(rand.Intn(len(shapes))),
			},
			Fields: map[string]interface{}{
				"value": rand.Intn(sampleSize),
			},
			Time:      time.Now(),
			Precision: "s",
		}
	}

	bps := client.BatchPoints{
		Points:          pts,
		Database:        "example",
		RetentionPolicy: "default",
	}
	_, err = c.Write(bps)
	if err != nil {
		log.Fatal(err)
	}
}
func ExampleClient_query() {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://10.0.0.4:8086",
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}
	defer c.Close()

	q := client.NewQuery("SELECT value FROM cpu_load_short", "example", "")
	if response, err := c.Query(q); err == nil && response.Error() == nil {
		fmt.Println(response.Results)
	}
}

*/

func query1hr() {
	
	
	client1 := influxdb2.NewClient("http://10.0.0.4:8086", "my-token")
	queryAPI := client1.QueryAPI("")
	measrmnt := "patient098c6e4c-78cb-4349-aa51-27d1b8702f2a" + "_ecg"
	
	//valstr := "from(bucket:\"emr_dev/1year\")|> range(start: 2020-10-06T18:53:10Z, stop:2020-10-06T18:53:21Z) |> filter(fn: (r) => r._measurement == "+ measrmnt +")"
	valstr:= `from(bucket:"emr_dev/1year") 
	|> range(start:-1222h) 
	|> filter(fn: (r) => r._measurement == "` + measrmnt +  `" and r._field =~ /ecg|motion|AvgRR|hr/) 
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> filter(fn: (r) => r.motion == 3) |> limit(n:1000)`
	
	
	//result, err := queryAPI.Query(context.Background(),"from(bucket:\"emr_dev/1year\")|> range(start: 2020-10-06T18:53:10Z, stop:2020-10-06T18:53:21Z) |> filter(fn: (r) => r._measurement == \"patientcaa36e30-6f0a-4758-9dbd-e9e4a569d2ea_ecg\")")
	result, _ := queryAPI.QueryRaw(context.Background(),valstr,influxdb2.DefaultDialect())
	//fmt.Println(valstr, result, err)

	resultSplit := strings.Split(result, "\n")
	//fmt.Println("%s", result)

	limit := 300
	if limit > len(resultSplit)-4 {
		limit = len(resultSplit)-4
	}

	fluentStream := make([]string, limit)
	samplesInStream := make([]int, limit)
	
	numflows :=0
	prevTime := time.Now().Unix()
	for i:=4 ;i < limit; i++ {
		resultSubSplit := strings.Split(resultSplit[i], ",")
		resultSubSplit2 := strings.Split(resultSplit[i], "\"")
		udbtime, _ := time.Parse(time.RFC3339, resultSubSplit[3])

		delta := udbtime.Unix() - prevTime
		prevTime = udbtime.Unix()
		if delta > 0 && delta < 5  {
			fluentStream[i] = fluentStream[i] + string(resultSubSplit2[1])
			samplesInStream[numflows]++
		} else {
			//if samplesInStream[numflows] > 60 {
			//	push to influyx
			// break
			//}
			numflows++
			//if num_flows > 50 {
			//	push to influyx
			//}
			fluentStream[i] = string(resultSubSplit2[1])
			samplesInStream[i] = 0
		}
		
		fmt.Println("\n",resultSubSplit[3], udbtime.Unix())
	}
	//fmt.Println("-------%s", fluentStream)
	fmt.Println("-------%s", samplesInStream)

	//finalStr := ""
	/*if err == nil {
		// Use Next() to iterate over query result lines
		for result.Next() {
			// Observe when there is new grouping key producing new table
			//if result.TableChanged() {
			//	t1Ctxt.log.Debugf("table: %s\n", result.TableMetadata().String())
			//}
			// read result
			//tmp := fmt.Sprintf("%s", result.Record().Value());
			//finalStr = finalStr + tmp
			fmt.Printf("value: %v\n", result.Record().Value())
			fmt.Println("row: %s\n", result.Record().Value())
		}
		if result.Err() != nil {
			fmt.Println("Query error: %s\n", result.Err().Error())
		}
	} else {
		fmt.Println("Query error: %v\n", err)
	}
*/
    // Ensures background processes finishes
    client1.Close()
}


func writeV2() {
	// create new client with default option for server url authenticate by token
	client := influxdb2.NewClient("http://10.0.0.5:8086", "my-token")
	// user blocking write client for writes to desired bucket
	writeAPI := client.WriteAPIBlocking("", "emr_dev/90days")

	fmt.Println(writeAPI)
	// create point using full params constructor
	p := influxdb2.NewPoint("stat-abcd323-7bbd",
		map[string]string{"unit": "temperature"},
		map[string]interface{}{"avg": 24.5, "max": 45},
		time.Now())
	// write point immediately
	writeAPI.WritePoint(context.Background(), p)
	// create point using fluent style
	p = influxdb2.NewPointWithMeasurement("stat").
		AddTag("unit", "temperature").
		AddField("avg", 23.2).
		AddField("max", 45).
		SetTime(time.Now())
	writeAPI.WritePoint(context.Background(), p)

	// Or write directly line protocol
	line := fmt.Sprintf("stat,unit=temperature avg=%f,max=%f", 23.5, 45.0)
	writeAPI.WriteRecord(context.Background(), line)
	// Ensures background processes finish
	client.Close()
}

func main() {
	//writeV2()
	query1hr()
}
