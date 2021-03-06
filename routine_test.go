package routine

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

/**
* golang-sample源代码，版权归锦翰科技（深圳）有限公司所有。
* <p>
* 文件名称 : routine_test.go
* 文件路径 :
* 作者 : DavidLiu
× Email: david.liu@ginghan.com
*
* 创建日期 : 2022/6/1 13:03
* 修改历史 : 1. [2022/6/1 13:03] 创建文件 by LongYong
*/

func TestRoutine(t *testing.T) {

	var done = 0
	var lock sync.RWMutex
	var doneMap = make(map[int]int)
	var orderMap = make(map[int]int)

	var wg sync.WaitGroup
	taskCount := 100
	wg.Add(taskCount)

	wp, err := NewPool[int]()

	if err != nil {
		Logger.Info("%v", err)
	}

	wp.Start()
	Logger.Info("Start")

	for idx := 0; idx < taskCount; idx++ {
		go func(i int) {
			wp.Submit(WorkerFunc(func() error {
				fmt.Println(i)
				done++
				lock.Lock()
				order := len(doneMap)
				doneMap[i] = order + 1
				orderMap[order+1] = i
				lock.Unlock()
				wg.Done()
				return nil
			}))
		}(idx)
	}

	wg.Wait()

	time.Sleep(5 * time.Second)
	wp.Stop()
	Logger.Info("Stop")

	if done != taskCount {
		panic("Done is " + strconv.Itoa(done))
	}

	if len(doneMap) != taskCount {
		panic("DoneMap is " + strconv.Itoa(len(doneMap)))
	}

	fmt.Println("Index Map")

	for k, v := range doneMap {
		fmt.Printf("%v=%v\n", k, v)
	}

	fmt.Println("Order Map")

	for k, v := range orderMap {
		fmt.Printf("%v=%v\n", k, v)
	}

	time.Sleep(1 * time.Second)
}

func TestCyclicDistributionEventChain(t *testing.T) {
	var n uint64 = 18446744073709551615
	fmt.Println(n + 1)

	var wg sync.WaitGroup
	cyclic := 10

	doLoop := 10
	clientCount := 100
	taskCount := doLoop * clientCount

	wg.Add(taskCount)

	c := NewCyclicDistributionEventChain[string](cyclic)

	c.Start(EventHander[string](func(ch EventChannel[string], t *string) error {
		fmt.Printf("EventChannel[%v] %v\n", ch.ID(), *t)
		wg.Done()
		return nil
	}))

	//channelMap := make(map[int]*EventChannel[int])

	go func() {
		for idx := 0; idx < clientCount; idx++ {

			go func(clientId int) {
				channel, _ := c.BorrowOne()

				for j := 0; j < doLoop; j++ {
					str := fmt.Sprintf("ClientID[%v]==>%v", idx, j)

					channel.AddEvent(&str)
				}

				c.ReturnOne(channel)

			}(idx)

		}

		reportMetrics := c.ReportMetrics()
		fmt.Printf("After Metrics: \n %+v\n", reportMetrics)
	}()

	reportMetrics := c.ReportMetrics()
	fmt.Printf("Metrics: \n %+v\n", reportMetrics)
	wg.Wait()

	reportMetrics = c.ReportMetrics()
	fmt.Printf("Metrics: \n %+v\n", reportMetrics)
	c.Stop()
	time.Sleep(1 * time.Second)

	reportMetrics = c.ReportMetrics()
	fmt.Printf("Metrics: \n %+v\n", reportMetrics)

	fmt.Println("Over")
}
