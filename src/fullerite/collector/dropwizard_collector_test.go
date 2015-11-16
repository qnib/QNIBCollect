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
	assert.Equal(t, 1, len(metrics))
}

func TestInvalidMetricSkilled(t *testing.T) {
	rawData := []byte(`
{
        "meters": {
            "pyramid_uwsgi_metrics.tweens.2xx-responses": {
                "units": "events/second"
            }
        }
}
        `)

	metrics, err := parseUWSGIMetrics(&rawData)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(metrics))
}
