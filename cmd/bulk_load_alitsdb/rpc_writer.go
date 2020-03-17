package main

import (
	"bytes"
	"sync"
	"time"
)

// RpcWriterConfig is the configuration used to create an RpcWriter.
type RpcWriterConfig struct {
}

type RpcWriter struct {
	c RpcWriterConfig
}

// It returns the latency in nanoseconds and any error received while sending the data over RPC,
// or it returns a new error if the RPC response isn't as expected.
func (w *RpcWriter) WriteLineProtocol([]byte) (latencyNs int64, err error) {
	start := time.Now()

	//TODO: send the write request

	lat := time.Since(start).Nanoseconds()

	return lat, nil
}

// ProcessBatches read the data from input stream and write by batch
func (w *RpcWriter) ProcessBatches(doLoad bool, bufPool *sync.Pool, wg *sync.WaitGroup, batchChan chan *bytes.Buffer, backoff time.Duration, backingOffChan chan bool) {

}
