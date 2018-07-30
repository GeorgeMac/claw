package queue

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/georgemac/claw/task"
	"github.com/stretchr/testify/assert"
)

const testNs = "github.com/georgemac/claw/test/"

func TestQueue(t *testing.T) {
	var (
		ctxt, cancel = context.WithCancel(context.Background())
		cli, err     = clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
		kv           = namespace.NewKV(cli.KV, testNs)
		watcher      = namespace.NewWatcher(cli.Watcher, testNs)
		lease        = namespace.NewLease(cli.Lease, testNs)
		tasks        = make(chan task.Task, 10)

		data = map[string][]int{
			"queue_one":   []int{1, 2, 3, 4, 5, 6},
			"queue_two":   []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			"queue_three": []int{1, 2},
			"queue_four":  []int{1, 2, 3, 4, 5, 6, 7},
			"queue_five":  []int{1, 2, 3},
		}

		recv = map[string][]int{}
		wg   sync.WaitGroup
		seen = make(chan task.Task)
		done = make(chan struct{})
	)

	go func() {
		var count, max int
		for _, vs := range data {
			max += len(vs)
		}

		for task := range seen {
			count++

			val, err := strconv.ParseInt(string(task.Payload), 10, 16)
			if err != nil {
				t.Fatal(err)
			}

			// update received
			recv[task.QueueID] = append(recv[task.QueueID], int(val))

			if count == max {
				close(done)
				close(seen)
			}
		}
	}()

	if err != nil {
		t.Fatal(err)
	}

	defer cancel()

	go WatchRunning(ctxt, watcher, tasks)

	// start 3 consumers
	for i := 0; i < 3; i++ {
		go Consume(ctxt, kv, lease, tasks, WorkerFunc(func(task task.Task) error {

			// notify seen
			seen <- task

			return nil
		}))
	}

	// schedule work
	for queue, values := range data {
		wg.Add(1)
		go func(queue string, values []int) {
			defer wg.Done()

			for _, value := range values {
				if err := Schedule(ctxt, kv, task.New(queue, []byte(fmt.Sprintf("%d", value)))); err != nil {
					t.Fatal(err)
				}
			}
		}(queue, values)
	}

	wg.Wait()

	// wait for expected number of dones or timeout
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	assert.Equal(t, data, recv)
}
