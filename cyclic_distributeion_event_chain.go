package routine

import (
	"errors"
	"sync"
)

/**
* golang-sample源代码，版权归锦翰科技（深圳）有限公司所有。
* <p>
* 文件名称 : writepool
* 文件路径 :
* 作者 : DavidLiu
× Email: david.liu@ginghan.com
*
* 创建日期 : 2022/6/7 10:47
* 修改历史 : 1. [2022/6/7 10:47] 创建文件 by LongYong
*/

var (
	NotValidJobChan = errors.New("not valid job chan now")
	NullJobChan     = errors.New("null job chan")
)

type EventChannel[T any] struct {
	c  chan *T
	id int
}

func (jc *EventChannel[T]) ID() int {
	return jc.id
}

func (jc *EventChannel[T]) AddEvent(one *T) {
	if jc.c != nil {
		jc.c <- one
	}
}

func (jc *EventChannel[T]) reset() {
	var n *T
	jc.AddEvent(n)

	close(jc.c)
	jc.c = nil
}

type cyclicEventChainReportMetric struct {
	PoolSize          int    `json:"pool_size,omitempty"`
	Actives           []bool `json:"actives,omitempty"`
	NumClients        []int  `json:"num_clients,omitempty"`
	CurrentCyclicCode uint64 `json:"current_cyclic_code,omitempty"`
	Started           bool   `json:"started,omitempty"`
}

func (tp *cyclicDistributionEventChain[T]) ReportMetrics() cyclicEventChainReportMetric {
	Actives := make([]bool, tp.poolSize)

	for i, j := range tp.jobChan {
		Actives[i] = j.c != nil
	}

	return cyclicEventChainReportMetric{
		PoolSize:          tp.poolSize,
		Actives:           Actives,
		NumClients:        tp.chanClient[0:],
		CurrentCyclicCode: tp.currentHashCode,
		Started:           tp.started,
	}
}

func NewCyclicDistributionEventChain[T any](cyclicSize int) *cyclicDistributionEventChain[T] {
	if cyclicSize <= 0 {
		cyclicSize = 1000

	}
	return &cyclicDistributionEventChain[T]{
		poolSize: cyclicSize,
	}
}

type cyclicDistributionEventChain[T any] struct {
	mu              sync.Mutex
	poolSize        int
	jobChan         []EventChannel[T]
	chanClient      []int
	currentHashCode uint64
	started         bool
}

type EventHander[T any] func(job EventChannel[T], t *T) error

func (tp *cyclicDistributionEventChain[T]) Start(eventHandler EventHander[T]) {
	tp.Stop()

	tp.mu.Lock()
	defer tp.mu.Unlock()

	poolSize := tp.poolSize

	jc := make([]EventChannel[T], poolSize)
	chanClient := make([]int, poolSize)

	for idx := 0; idx < poolSize; idx++ {
		job := EventChannel[T]{id: idx, c: make(chan *T)}

		chanClient[idx] = 0
		jc[idx] = job

		go func(id int) {
			for {
				writeUnit := <-job.c
				if writeUnit == nil { // if stop
					break
				}
				if eventHandler != nil {
					err := eventHandler(job, writeUnit)

					if err != nil {
						Logger.Warning("Event error : ", err)
					}
				}
			}
			job.c = nil
			//chanClient[id] = -1
		}(idx)
	}

	tp.chanClient = chanClient
	tp.jobChan = jc
	tp.poolSize = poolSize
	tp.currentHashCode = 0
	tp.started = true
}

func (tp *cyclicDistributionEventChain[T]) findMaxAndMin() (int, int) {
	var max int = 0
	var min int = len(tp.chanClient)

	tp.mu.Lock()

	maxCount := 0
	minCount := 0

	for idx, count := range tp.chanClient {
		if count < 0 {
			continue
		}

		if maxCount < count {
			maxCount = count
			max = idx
		}
		if minCount > count {
			minCount = count
			min = idx
		}
	}

	tp.mu.Unlock()

	return max, min
}

func (tp *cyclicDistributionEventChain[T]) Stop() {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	var jc []EventChannel[T] = tp.jobChan
	if len(jc) > 0 {
		for _, job := range tp.jobChan {
			job.reset()
		}
	}
	tp.started = false
}

func (tp *cyclicDistributionEventChain[T]) BorrowOne() (*EventChannel[T], error) {
	tp.mu.Lock()

	var rtn *EventChannel[T]
	n := uint64(tp.poolSize)

	for true {
		idx := tp.currentHashCode % n
		tp.currentHashCode++

		one := tp.jobChan[idx]

		if one.c == nil {
			continue
		} else {
			rtn = &one
			tp.chanClient[idx] = tp.chanClient[idx] + 1
			break
		}
	}

	//for _, idx := range tp.sortLine {
	//	one := tp.EventChannel[idx]
	//	if one.c == nil {
	//		continue
	//	} else {
	//		tp.chanClient[idx] = tp.chanClient[idx] + 1
	//		rtn = &one
	//	}
	//}

	if rtn == nil {
		tp.mu.Unlock()
		return nil, NotValidJobChan
	}

	tp.mu.Unlock()
	return rtn, nil
}

func (tp *cyclicDistributionEventChain[T]) ReturnOne(jc *EventChannel[T]) error {
	if jc == nil {
		return NullJobChan
	}

	id := jc.id

	if id < 0 {
		return NullJobChan
	}

	if id >= tp.poolSize {
		return NullJobChan
	}

	tp.mu.Lock()

	tp.chanClient[id] = tp.chanClient[id] - 1

	tp.mu.Unlock()

	return nil
}
