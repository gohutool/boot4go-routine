package routine

import (
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

type WorkerFunc func()

type workerChan struct {
	lastUseTime time.Time
	ch          chan WorkerFunc
}

// Such a scheme keeps CPU caches hot (in theory).
type workerPool struct {
	MaxWorkersCount int

	LogAllErrors bool

	MaxIdleWorkerDuration time.Duration

	lock         sync.Mutex
	workersCount int
	mustStop     bool

	ready []*workerChan

	stopCh chan struct{}

	workerChanPool sync.Pool
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

func NewPool(options ...Option) (*workerPool, error) {
	opts := LoadOptions(options...)

	if expiry := opts.MaxIdleWorkerDuration; expiry < 0 {
		return nil, ErrInvalidMaxIdle
	} else if expiry == 0 {
		opts.MaxIdleWorkerDuration = DefaultCleanIntervalTime
	}

	if workerCount := opts.MaxWorkersCount; workerCount < 0 {
		opts.MaxWorkersCount = -1
	} else if workerCount == 0 {
		opts.MaxWorkersCount = DefaultConcurrency
	}

	wp := &workerPool{
		MaxWorkersCount:       opts.MaxWorkersCount,
		LogAllErrors:          opts.LogAllErrors,
		MaxIdleWorkerDuration: opts.MaxIdleWorkerDuration,
	}

	wp.workerChanPool.New = func() interface{} {
		return &workerChan{
			ch: make(chan WorkerFunc, workerChanCap),
		}
	}

	return wp, nil
}
