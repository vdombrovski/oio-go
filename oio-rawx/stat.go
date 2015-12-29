package main

import (
	"sync"
)

const (
	Unexpected = iota
	StatSlotGetStats = iota
	StatSlotPut = iota
	StatSlotGet = iota
	StatSlotDel = iota
	LastStat = iota
)

var names = [LastStat]string{
	"Unexpected",
	"GetStats",
	"Put",
	"Get",
	"Del",
}

type StatSet struct {
	lock sync.RWMutex
	values [LastStat]uint64
}

var counters, timers StatSet

func (ss *StatSet) Increment (which int) {
	ss.lock.Lock()
	defer ss.lock.Unlock()

	if which < 0 || which >= LastStat {
		panic("BUG: stat does not exist")
	}
	ss.values[which] ++
}

func (ss *StatSet) Addcount (which int, inc uint64) {
	ss.lock.Lock()
	defer ss.lock.Unlock()

	if which < 0 || which >= LastStat {
		panic("BUG: stat does not exist")
	}
	ss.values[which] += inc
}

func (ss *StatSet) Get () [LastStat]uint64 {
	ss.lock.RLock()
	defer ss.lock.RUnlock()

	var tab [LastStat]uint64
	tab = ss.values
	return tab
}

