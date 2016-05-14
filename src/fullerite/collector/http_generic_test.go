package collector

import (
	"fullerite/metric"

	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var testLog = l.WithFields(l.Fields{"testing": "http_generic"})

func buildBaseHTTPCollector(endpoint string) *baseHTTPCollector {
	col := new(baseHTTPCollector)
	col.endpoint = endpoint
	col.log = testLog
	col.channel = make(chan metric.Metric)
	return col
}

func buildServer(response string) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, rsp *http.Request) {
		fmt.Fprint(writer, response)
	}))

	return server
}

func ensureEmpty(c chan metric.Metric) bool {
	close(c)

	for m := range c {
		var _ = m
		return false
	}

	return true
}

func TestMisConfig(t *testing.T) {
	col := buildBaseHTTPCollector("")
	col.errHandler = func(err error) {
		t.FailNow()
	}
	col.rspHandler = func(rsp *http.Response) []metric.Metric {
		t.FailNow()
		return nil
	}

	go col.Collect()
}

func TestWorkingGenericHTTP(t *testing.T) {
	// setup the server
	expectedResponse := "This should come back to me"
	server := buildServer(expectedResponse)
	defer server.Close()

	col := buildBaseHTTPCollector(server.URL)
	col.errHandler = func(err error) {
		testLog.Error("Should not have caused an error")
		t.FailNow()
	}

	col.rspHandler = func(rsp *http.Response) []metric.Metric {
		txt, _ := ioutil.ReadAll(rsp.Body)
		rsp.Body.Close()

		assert.Equal(t, expectedResponse, string(txt))
		return []metric.Metric{metric.New("junk")}
	}

	go col.Collect()
	m := <-col.Channel()

	assert.NotNil(t, m, "should have produced a single metric")
	assert.True(t, ensureEmpty(col.Channel()), "There should have only been a single metric")
}
