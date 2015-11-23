package collector

import (
	"net/url"
	"strings"
	"time"

	"github.com/andygrunwald/megos"
)

// Make sure that the interface contracts are met
var (
	_ MesosLeaderElectInterface = (*MesosLeaderElect)(nil)
)

// DI
var (
	createMesos     = megos.NewClient
	determineLeader = (*megos.Client).DetermineLeader
)

// MesosLeaderElectInterface Interface to allow injecting MesosLeaderElect, for easier testing
type MesosLeaderElectInterface interface {
	Configure(string, time.Duration)
	Get() string
	set()
}

// MesosLeaderElect Encapsulation for the mesos leader given a set of masters. Cache this so that we don't spend too much time determining the leader every *collector.Collect() call, which happens every 10 seconds.
type MesosLeaderElect struct {
	leader string
	mesos  *megos.Client
	ttl    time.Duration
	expire time.Time
}

// Configure Provide the set of masters like so ("http://1.2.3.4:5050/,http://5.6.7.8:5050/") and the desired TTL for the cache.
func (mle *MesosLeaderElect) Configure(nodes string, ttl time.Duration) {
	hosts := mle.parseUrls(nodes)

	mle.ttl = ttl
	mle.mesos = createMesos(hosts)
}

// Get get the IP of the leader; calls *MesosLeaderElect.set() on the first call or if the TTL has expired.
func (mle *MesosLeaderElect) Get() string {
	if len(mle.leader) == 0 || time.Since(mle.expire) > mle.ttl {
		mle.set()
	}

	return mle.leader
}

// parseUrls Conver the provided string of masters ("http://1.2.3.4:5050/,http://5.6.7.8:5050/") via *MesosLeaderElect.Configure() into an array of url.URLs, which is understood by the megos package.
func (mle *MesosLeaderElect) parseUrls(nodes string) []*url.URL {
	n := strings.Split(nodes, ",")
	hosts := make([]*url.URL, len(n), len(n))

	for k, v := range n {
		nodeURL, _ := url.Parse(v)
		hosts[k] = nodeURL
	}

	return hosts
}

// set Calls megos.client.DetermineLeader.
func (mle *MesosLeaderElect) set() {
	defer func() { mle.expire = time.Now().Add(mle.ttl) }()

	leader, _ := determineLeader(mle.mesos)

	mle.leader = leader.Host
}
