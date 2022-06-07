package routine

import (
	"github.com/gohutool/log4go"
	"runtime"
	"sync"
	"time"
)

/**
* golang-sample源代码，版权归锦翰科技（深圳）有限公司所有。
* <p>
* 文件名称 : workpool.go
* 文件路径 :
* 作者 : DavidLiu
× Email: david.liu@ginghan.com
*
* 创建日期 : 2022/5/31 20:28
* 修改历史 : 1. [2022/5/31 20:28] 创建文件 by LongYong
*/

const DefaultConcurrency = 256 * 1024
const DefaultCleanIntervalTime = time.Second

var Logger = log4go.LoggerManager.GetLogger("gohutool.boot4go.routine")

type WorkerFunc func() error

type workerChan[T any] struct {
	lastUseTime time.Time
	ch          chan WorkerFunc
}

type WorkPoolMetrics struct {
	MaxWorkersCount       uint32        `json:"max_workers_count,omitempty"`
	LogAllErrors          bool          `json:"log_all_errors,omitempty"`
	MaxIdleWorkerDuration time.Duration `json:"max_idle_worker_duration,omitempty"`
	workersCount          uint32        `json:"workers_count,omitempty"`
	Ready                 int           `json:"ready,omitempty"`
	GetTimes              uint64        `json:"get_times,omitempty"`
	PurgeTimes            uint64        `json:"purge_times,omitempty"`
}

func (wp *workerPool[T]) ReportMetrics() WorkPoolMetrics {

	var ready []*workerChan[T] = wp.ready

	return WorkPoolMetrics{
		MaxWorkersCount:       wp.MaxWorkersCount,
		LogAllErrors:          wp.LogAllErrors,
		MaxIdleWorkerDuration: wp.MaxIdleWorkerDuration,
		workersCount:          wp.workersCount,
		Ready:                 len(ready),
		GetTimes:              wp.getTimes,
		PurgeTimes:            wp.purgeTimes,
	}
}

// Such a scheme keeps CPU caches hot (in theory).
type workerPool[T any] struct {
	MaxWorkersCount       uint32
	LogAllErrors          bool
	MaxIdleWorkerDuration time.Duration

	lock         sync.Mutex
	workersCount uint32
	mustStop     bool

	ready  []*workerChan[T]
	stopCh chan struct{}

	workerChanPool sync.Pool

	getTimes   uint64
	purgeTimes uint64
}

var workerChanCap = func() int {
	// Use blocking workerChan if GOMAXPROCS=1.
	// This immediately switches Serve to WorkerFunc, which results
	// in higher performance (under go1.5 at least).
	if runtime.GOMAXPROCS(0) == 1 {
		return 0
	}

	// Use non-blocking workerChan if GOMAXPROCS>1,
	// since otherwise the Serve caller (Acceptor) may lag accepting
	// new connections if WorkerFunc is CPU-bound.
	return 1
}()

func NewPool[T any](options ...Option) (*workerPool[T], error) {
	opts := LoadOptions(options...)

	if expiry := opts.MaxIdleWorkerDuration; expiry < 0 {
		//return nil, ErrInvalidMaxIdle
	} else if expiry == 0 {
		opts.MaxIdleWorkerDuration = DefaultCleanIntervalTime
	}

	if workerCount := opts.MaxWorkersCount; workerCount <= 0 {
		opts.MaxWorkersCount = DefaultConcurrency
	}

	wp := &workerPool[T]{
		MaxWorkersCount:       opts.MaxWorkersCount,
		LogAllErrors:          opts.LogAllErrors,
		MaxIdleWorkerDuration: opts.MaxIdleWorkerDuration,
	}

	wp.workerChanPool.New = func() interface{} {
		return &workerChan[T]{
			ch: make(chan WorkerFunc, workerChanCap),
		}
	}

	return wp, nil
}

func (wp *workerPool[T]) GetMaxIdleWorkerDuration() time.Duration {
	if wp.MaxIdleWorkerDuration <= 0 {
		return 10 * time.Second
	}
	return wp.MaxIdleWorkerDuration
}

func (wp *workerPool[T]) Submit(workerFunc WorkerFunc) bool {
	ch := wp.getCh()
	if ch == nil {
		return false
	}
	ch.ch <- workerFunc
	return true
}

func (wp *workerPool[T]) Start() {
	if wp.stopCh != nil {
		panic("BUG: workerPool already started")
	}
	wp.stopCh = make(chan struct{})

	go wp.purgePeriodically()
}

func (wp *workerPool[T]) Stop() {
	if wp.stopCh == nil {
		panic("BUG: workerPool wasn't started")
	}
	close(wp.stopCh)
	wp.stopCh = nil

	// Stop all the workers waiting for incoming connections.
	// Do not wait for busy workers - they will stop after
	// serving the connection and noticing wp.mustStop = true.
	wp.lock.Lock()
	ready := wp.ready

	// Stop will send nil to chan to stop workFun
	for i := range ready {
		ready[i].ch <- nil
		ready[i] = nil
	}

	wp.ready = ready[:0]
	wp.mustStop = true
	wp.lock.Unlock()

	Logger.Debug("Stop work pool")
}

func (wp *workerPool[T]) clean(scratch *[]*workerChan[T]) {
	maxIdleWorkerDuration := wp.GetMaxIdleWorkerDuration()

	// Clean least recently used workers if they didn't serve connections
	// for more than maxIdleWorkerDuration.
	criticalTime := time.Now().Add(-maxIdleWorkerDuration)

	wp.lock.Lock()
	var ready []*workerChan[T] = wp.ready
	n := len(ready)

	// Use binary-search algorithm to find out the index of the least recently worker which can be cleaned up.
	l, r, mid := 0, n-1, 0
	for l <= r {
		mid = (l + r) / 2
		if criticalTime.After(wp.ready[mid].lastUseTime) {
			l = mid + 1
		} else {
			r = mid - 1
		}
	}
	i := r
	if i == -1 {
		wp.lock.Unlock()
		return
	}

	*scratch = append((*scratch)[:0], ready[:i+1]...)
	m := copy(ready, ready[i+1:])
	for i = m; i < n; i++ {
		ready[i] = nil
	}
	wp.ready = ready[:m]
	wp.purgeTimes = wp.purgeTimes + uint64(len(*scratch))
	wp.lock.Unlock()

	// Notify obsolete workers to stop.
	// This notification must be outside the wp.lock, since ch.ch
	// may be blocking and may consume a lot of time if many workers
	// are located on non-local CPUs.
	tmp := *scratch
	for i := range tmp {
		tmp[i].ch <- nil
		tmp[i] = nil
	}
}

func (wp *workerPool[T]) purgePeriodically() {
	if wp.MaxIdleWorkerDuration > 0 {

		heartbeat := time.NewTicker(wp.GetMaxIdleWorkerDuration())
		defer heartbeat.Stop()

		for range heartbeat.C {
			if wp.stopCh == nil {
				break
			}
			var expired []*workerChan[T]
			wp.clean(&expired)
		}

	}
}

func (wp *workerPool[T]) release(ch *workerChan[T]) bool {
	ch.lastUseTime = time.Now()
	wp.lock.Lock()
	if wp.mustStop {
		wp.lock.Unlock()
		return false
	}
	var ready []*workerChan[T] = wp.ready
	wp.ready = append(ready, ch)
	wp.lock.Unlock()
	return true
}

func (wp *workerPool[T]) getCh() *workerChan[T] {
	var ch *workerChan[T]
	createWorker := false

	wp.lock.Lock()
	wp.getTimes = wp.getTimes + 1
	var ready []*workerChan[T] = wp.ready
	n := len(ready) - 1
	if n < 0 {
		if wp.workersCount < wp.MaxWorkersCount {
			createWorker = true
			wp.workersCount++
		}
	} else {
		ch = ready[n]
		ready[n] = nil
		wp.ready = ready[:n]
	}

	wp.lock.Unlock()

	if ch == nil {
		if !createWorker {
			return nil
		}
		vch := wp.workerChanPool.Get()
		ch = vch.(*workerChan[T])
		go func() {
			wp.workerFunc(ch)
			wp.workerChanPool.Put(vch)
		}()
	}

	//fmt.Printf("workChain's address %p\n", ch)

	return ch
}

func (wp *workerPool[T]) workerFunc(ch *workerChan[T]) {
	var workerfunc WorkerFunc

	var err error
	for workerfunc = range ch.ch {
		// when stop workpool will put nil to chan
		if workerfunc == nil || wp.mustStop {
			Logger.Debug("Be stopped in work pool")
			break
		}

		if err = workerfunc(); err != nil {
			errStr := err.Error()

			if wp.LogAllErrors {
				Logger.Error("error when serving connection %q<->%q: %v", errStr)
			}
		}

		if !wp.release(ch) {
			break
		}
	}

	wp.lock.Lock()
	wp.workersCount--
	wp.lock.Unlock()
}
