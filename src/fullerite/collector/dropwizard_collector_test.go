package collector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDropwizardNtesting(t *testing.T) {
	rawData := []byte(`
{
  "jetty": {
     "percent": {
         "foo": {
            "active-requests": {
              "count": 0,
              "type": "counter"
            }
         }
     }
   }
}
        `)

	metrics, err := parseUWSGIMetrics(&rawData)
	assert.Nil(t, err)
	t.Log(metrics)
}
