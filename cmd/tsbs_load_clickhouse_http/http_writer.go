package main

// This file lifted wholesale from mountainflux by Mark Rushakoff.

import (
	"fmt"
	"time"

	"github.com/valyala/fasthttp"
)

const (
	httpClientName        = "tsbs_load_clickhouse_http"
	headerContentEncoding = "Content-Encoding"
)

// HTTPWriterConfig is the configuration used to create an HTTPWriter.
type HTTPWriterConfig struct {
	// URL of the host, in form "http://example.com:8123"
	Host string

	// Name of the target database into which points will be written.
	Database string

	// Debug label for more informative errors.
	DebugInfo string
}

// HTTPWriter is a Writer that writes to an InfluxDB HTTP server.
type HTTPWriter struct {
	client fasthttp.Client

	c   HTTPWriterConfig
	url []byte
}

// NewHTTPWriter returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewHTTPWriter(c HTTPWriterConfig) *HTTPWriter {
	return &HTTPWriter{
		client: fasthttp.Client{
			Name: httpClientName,
		},

		c:   c,
		url: []byte(c.Host),
	}
}

var (
	methodPost = []byte("POST")
	textPlain  = []byte("text/plain")
)

func (w *HTTPWriter) initializeReq(req *fasthttp.Request, body []byte) {
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(methodPost)
	req.Header.SetRequestURIBytes(w.url)
	req.SetBody(body)
}

func (w *HTTPWriter) executeReq(req *fasthttp.Request, resp *fasthttp.Response) (int64, error) {
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		if sc == 500 {
			err = fmt.Errorf("Failed to insert rows to ClickHouse (status %d): %s", sc, resp.Body())
		}
	}
	return lat, err
}

// InsertRows writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *HTTPWriter) InsertRows(body []byte) (int64, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	w.initializeReq(req, body)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	return w.executeReq(req, resp)
}
