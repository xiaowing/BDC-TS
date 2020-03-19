package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	alitsdb_serialization "github.com/caict-benchmark/BDC-TS/alitsdb_serializaition"

	"google.golang.org/grpc"
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
	req.Points = points

	//TODO: send the write request
	resp, err := w.client.Mput(context.Background(), req)
	if err == nil {
		if !resp.Ret {
			log.Println("[WARN] mput request succeeded but retval is false")
		}
	} else {
		log.Printf("Error request mput interface: %s\n", err.Error())
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
		if doLoad {
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
	}

	wg.Done()
}
