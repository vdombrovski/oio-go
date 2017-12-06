package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

const (
	BytesRead    = iota
	BytesWritten = iota

	Hits2XX = iota
	Hits403 = iota
	Hits404 = iota
	Hits4XX = iota
	Hits5XX = iota

	HitsPut   = iota
	HitsGet   = iota
	HitsDel   = iota
	HitsStat  = iota
	HitsOther = iota
	HitsTotal = iota

	TimePut   = iota
	TimeGet   = iota
	TimeDel   = iota
	TimeStat  = iota
	TimeOther = iota
	TimeTotal = iota

	LastStat = iota
)

var statNames = [LastStat]string{
	"rep.bread",
	"rep.bwritten",

	"rep.hits.2xx",
	"rep.hits.403",
	"rep.hits.404",
	"rep.hits.4xx",
	"rep.hits.5xx",

	"rep.hits.put",
	"rep.hits.get",
	"rep.hits.del",
	"rep.hits.stat",
	"rep.hits.other",
	"rep.hits",

	"rep.time.put",
	"rep.time.get",
	"rep.time.del",
	"rep.time.stat",
	"rep.time.other",
	"rep.time",
}

type StatSet struct {
	lock   sync.RWMutex
	values [LastStat]uint64
}

var counters, timers StatSet

func (ss *StatSet) Increment(which int) {
	ss.lock.Lock()
	defer ss.lock.Unlock()

	if which < 0 || which >= LastStat {
		panic("BUG: stat does not exist")
	}
	ss.values[which]++
}

func (ss *StatSet) Add(which int, inc uint64) {
	ss.lock.Lock()
	defer ss.lock.Unlock()

	if which < 0 || which >= LastStat {
		panic("BUG: stat does not exist")
	}
	ss.values[which] += inc
}

func (ss *StatSet) Get() [LastStat]uint64 {
	ss.lock.RLock()
	defer ss.lock.RUnlock()

	var tab [LastStat]uint64
	tab = ss.values
	return tab
}

type statHandler struct {
	rawx *rawxService
}

func (self *statHandler) doGetStats(rep http.ResponseWriter, req *http.Request) {
	allCounters := counters.Get()
	allTimers := timers.Get()

	rep.WriteHeader(200)
	for i, n := range statNames {
		rep.Write([]byte(fmt.Sprintf("timer.%s %v\n", n, allTimers[i])))
		rep.Write([]byte(fmt.Sprintf("counter.%s %v\n", n, allCounters[i])))
	}
}

func (self *statHandler) ServeHTTP(rep http.ResponseWriter, req *http.Request) {
	var stats_hits, stats_time int

	pre := time.Now()
	switch req.Method {
	case "GET":
		stats_time = TimeStat
		stats_hits = HitsStat
		self.doGetStats(rep, req)
	default:
		stats_time = TimeOther
		stats_hits = HitsOther
		rep.WriteHeader(http.StatusMethodNotAllowed)
	}
	spent := uint64(time.Since(pre).Nanoseconds() / 1000)

	counters.Increment(HitsTotal)
	counters.Increment(stats_hits)
	counters.Add(TimeTotal, spent)
	counters.Add(stats_time, spent)
	log.Println("ACCESS", req)
}
