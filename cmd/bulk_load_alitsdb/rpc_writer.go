package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	alitsdb_serialization "github.com/caict-benchmark/BDC-TS/alitsdb_serializaition"
	cmap "github.com/orcaman/concurrent-map"

	"github.com/caict-benchmark/BDC-TS/bulk_data_gen/common"

	"google.golang.org/grpc"
)

// MputAttemptsLimit indicates the max attemps which the mput can tries
const MputAttemptsLimit = 4

var (
	fieldNameCache = cmap.New()
)

type RpcWriter struct {
	c      WriterConfig
	url    string
	conn   *grpc.ClientConn
	client alitsdb_serialization.MultiFieldsPutServiceClient
}

// WriteLineProtocol returns the latency in nanoseconds and any error received while sending the data over RPC,
// or it returns a new error if the RPC response isn't as expected.
func (w *RpcWriter) WriteLineProtocol(points []*alitsdb_serialization.MultifieldPoint) (latencyNs int64, err error) {
	start := time.Now()

	//TODO: build the request
	req := new(alitsdb_serialization.MputRequest)
	req.Points, req.Fnames = convertMultifieldPoints2MputPoints(points)

	if doLoad {
		retries := MputAttemptsLimit

		for retries > 0 {
			//TODO: send the write request
			resp, err := w.client.Mput(context.Background(), req)
			if err == nil {
				if !resp.Ret {
					log.Println("[WARN] mput request succeeded but retval is false")
				}
				// request succeeded so no need to retry
				retries = 0
			} else {
				log.Printf("Error request mput interface: %s\n", err.Error())
				retries--

				// wait a while
				time.Sleep(time.Duration((MputAttemptsLimit-retries)*5) * time.Second)
				// then start to retry
				w.conn.Close()

				conn, err := grpc.Dial(w.url, grpc.WithInsecure())
				if err != nil {
					log.Printf("Error connecting: %s\n", err.Error())
				}
				w.conn = conn
				w.client = alitsdb_serialization.NewMultiFieldsPutServiceClient(w.conn)
			}
		}
	}

	lat := time.Since(start).Nanoseconds()

	return lat, err
}

// NewRPCWriter returns a new RPCWriter from the supplied WriterConfig.
func NewRPCWriter(c WriterConfig) LineProtocolWriter {
	writer := &RpcWriter{
		c:   c,
		url: fmt.Sprintf("%s:%d", c.Host, c.Port),
	}

	conn, err := grpc.Dial(writer.url, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error connecting: %s\n", err.Error())
	}

	writer.conn = conn
	writer.client = alitsdb_serialization.NewMultiFieldsPutServiceClient(writer.conn)

	return writer
}

func (w *RpcWriter) close() {
	w.conn.Close()
}

// ProcessBatches read the data from input stream and write by batch
func (w *RpcWriter) ProcessBatches(doLoad bool, bufPool *sync.Pool, wg *sync.WaitGroup, backoff time.Duration, backingOffChan chan bool) {
	defer w.close()

	for batch := range batchPointsChan {
		var err error
		for {
			_, err = w.WriteLineProtocol(batch)
			backingOffChan <- false
			break
		}
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
	}

	wg.Done()
}

func convertMultifieldPoints2MputPoints(mfps []*alitsdb_serialization.MultifieldPoint) ([]*alitsdb_serialization.MputPoint, []string) {
	var batchMeticNname, metric string
	mputPoints := make([]*alitsdb_serialization.MputPoint, 0, len(mfps))
	var fieldNames []string

	for idx, p := range mfps {
		metric = getMetric(p)
		mputPoint := &alitsdb_serialization.MputPoint{
			Timestamp: p.GetTimestamp(),
			Serieskey: p.GetSerieskey(),
			Fvalues:   make([]float64, 0, len(p.GetFields())),
		}

		// try to get the sorted fieldname list
		if idx == 0 {
			batchMeticNname = metric

			if fieldNameCache.Has(metric) {
				tmp, ok := fieldNameCache.Get(metric)
				if !ok {
					log.Fatalf("metric %s lost in the concurrent operations\n", metric)
				}

				fieldNames, ok = tmp.([]string)
				if !ok {
					log.Fatalf("incorrect field name type\n")
				}
			} else {
				fieldNames = make([]string, 0, len(p.GetFields()))

				for fieldName := range p.GetFields() {
					fieldNames = append(fieldNames, fieldName)
				}

				fieldNameCache.SetIfAbsent(metric, fieldNames)
			}
		} else {
			if strings.Compare(batchMeticNname, metric) != 0 {
				log.Fatalf("there is a different metric \"%s\"within the current batch. \"%s\" expected", metric, batchMeticNname)
			}

			tmp, ok := fieldNameCache.Get(metric)
			if !ok {
				log.Fatalf("metric %s lost in the concurrent operations\n", metric)
			}

			fieldNames, ok = tmp.([]string)
			if !ok {
				log.Fatalf("incorrect field name type\n")
			}
		}

		// map cannot garantee the order, so we have to set the value like this
		for _, fname := range fieldNames {
			mputPoint.Fvalues = append(mputPoint.Fvalues, p.Fields[fname])
		}

		mputPoints = append(mputPoints, mputPoint)
	}

	return mputPoints, fieldNames
}

func getMetric(mp *alitsdb_serialization.MultifieldPoint) string {
	firstDeli := strings.IndexByte(mp.GetSerieskey(), byte(common.SerieskeyDelimeter))
	var metric string
	if firstDeli < 0 {
		//not found
		metric = mp.Serieskey
	} else {
		metric = mp.Serieskey[:firstDeli]
	}

	return metric
}
