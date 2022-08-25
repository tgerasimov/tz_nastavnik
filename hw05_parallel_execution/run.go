package hw05parallelexecution

import (
	"errors"
	"sync"
	"sync/atomic"
)

var ErrErrorsLimitExceeded = errors.New("errors limit exceeded") //можно было бы кастомно собирать ошибку и в нее вставлять инфу по количеству ошибок

type Task func() error //а че если у нас таски другую сигнатуру имеют? можно под дженерики подвести этот тип и его принимать

func worker(wg *sync.WaitGroup, in chan Task, errCounter *int32, maxError int) {
	defer wg.Done()
	for task := range in /*у нас из-за буфера будет ренджиться до тех пора не считает все, у нас одна горутина может по неск задач делать */ { //я бы добавил силект и передавал бы контекст который можно errgroup отменить, который отменял бы если errRate > limit
		err := task()
		if err != nil {
			atomic.AddInt32(errCounter, 1)
		}
		if int(atomic.LoadInt32(errCounter)) >= maxError { //у нас так-то из-за паралельного выполнения некоторые таски будут выполняться
			return //нахера кастовать тип, можно сразу int32 сделать и се
		}
	}
}

// Run starts tasks in n goroutines and stops its work when receiving m errors from tasks.
func Run(taskList []Task, n, m int) error {
	if m < 1 {
		m = 1
	}
	wg := &sync.WaitGroup{}                //в контексте задачи можно было бы errgroup испольщовать golang.org/x/sync/errgroup
	in := make(chan Task, len(taskList)+1) //зачем +1? и зачем вообще буфер
	var errCounter int32
	wg.Add(n)

	for i := 0; i < len(taskList); i++ {
		in <- taskList[i]
	}
	close(in)
	for i := 0; i < n; i++ {
		go worker(wg, in, &errCounter, m)
	}
	var result error //зачем пустую ошибку ошибку объявлять
	wg.Wait()
	if int(errCounter) >= m { //прикастуй блядь m в начале
		result = ErrErrorsLimitExceeded
	}
	return result
}
