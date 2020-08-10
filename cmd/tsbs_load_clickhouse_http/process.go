package main

import (
	"fmt"

	"github.com/timescale/tsbs/load"
)

// allows for testing
var printFn = fmt.Printf

type processor struct {
	httpWriter *HTTPWriter
}

func (p *processor) Init(numWorker int, _ bool) {
	daemonURL := daemonURLs[numWorker%len(daemonURLs)]
	cfg := HTTPWriterConfig{
		DebugInfo: fmt.Sprintf("worker #%d, dest url: %s", numWorker, daemonURL),
		Host:      daemonURL,
		Database:  loader.DatabaseName(),
	}
	w := NewHTTPWriter(cfg)
	p.initWithHTTPWriter(numWorker, w)
}

func (p *processor) initWithHTTPWriter(numWorker int, w *HTTPWriter) {
	p.httpWriter = w
}

func (p *processor) Close(_ bool) {
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if doLoad {
		var err error
		_, err = p.httpWriter.InsertRows(batch.buf.Bytes())
		if err != nil {
			fatal("Error writing: %s\n", err.Error())
		}
	}
	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return metricCnt, rowCnt
}
