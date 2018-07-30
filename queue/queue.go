package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/georgemac/claw/task"
)

const (
	queuedNS    = "queued/"
	runningNS   = "running/"
	countNS     = "count/"
	claimNS     = "claim/"
	reqTimeout  = 5 * time.Second
	noOfWorkers = 5
)

var (
	// ErrKeyNotFound is returned when a key is not found in the backing etcd
	ErrKeyNotFound = errors.New("key not found")
	// ErrTaskAlreadyClaimed is returned when an attempt to claim an already claimed task occurs
	ErrTaskAlreadyClaimed = errors.New("task already claimed")
)

// Worker is a type which can perform an action on a provided Task
type Worker interface {
	Do(task.Task) error
}

// WorkerFunc is a function which can do a task
type WorkerFunc func(task.Task) error

// Do delegates down to the callee WorkerFunc
func (w WorkerFunc) Do(t task.Task) error { return w(t) }

// queuePrefix is /queued/{{ .QueueID }}
func queuePrefix(t task.Task) string {
	return path.Join(queuedNS, t.QueueID)
}

// queuedKey is /queued/{{ .QueueID }}/{{ .ID }}
func queuedKey(t task.Task) string {
	return path.Join(queuePrefix(t), t.ID)
}

// runningKey is /running/{{ .QueueID }}/{{ .ID }}
func runningKey(t task.Task) string {
	return path.Join(runningNS, t.QueueID, t.ID)
}

// countKey is /count/{{ .QueueID }}
func countKey(t task.Task) string {
	return path.Join(countNS, t.QueueID)
}

// claimKey is /claim/{{ .QueueID }}/{{ .ID }}
func claimKey(t task.Task) string {
	return path.Join(claimNS, t.QueueID, t.ID)
}

type ListOption func(*ListOptionConfig)

type ListOptions []ListOption

func (l ListOptions) Apply(c *ListOptionConfig) {
	for _, opt := range l {
		opt(c)
	}
}

type ListOptionConfig struct {
	queueName *string
}

func WithQueue(queue string) ListOption {
	return func(conf *ListOptionConfig) { conf.queueName = &queue }
}

type Stats struct {
	QueueName    string
	QueuedCount  int
	RunningCount int
	ClaimedCount int
}

func List(ctxt context.Context, kv clientv3.KV, options ...ListOption) (map[string]Stats, error) {
	var conf ListOptionConfig

	ListOptions(options).Apply(&conf)

	if conf.queueName == nil {
		res, err := kv.Get(ctxt, "github.com/georgemac/claw/", clientv3.WithPrefix(), clientv3.WithLimit(0))
		if err != nil {
			return nil, err
		}

		all := map[string]Stats{}

		for _, kv := range res.Kvs {
			var (
				key   = strings.TrimPrefix(string(kv.Key), "github.com/georgemac/claw/")
				parts = strings.Split(key, "/")
				// {{ state }}
				state = parts[0]
				// {{ queue }}
				queue = parts[1]
				s, _  = all[queue]
			)

			// set queue name
			s.QueueName = queue

			// update state counts
			switch state {
			case "queued":
				s.QueuedCount += 1
			case "running":
				s.RunningCount += 1
			case "claim":
				s.ClaimedCount += 1
			}

			all[queue] = s
		}

		return all, nil
	}

	return nil, errors.New("not implemented")
}

// Schedule enqueues a provided task to be performed by a worker
func Schedule(ctxt context.Context, kv clientv3.KV, task task.Task) error {
	data, err := json.Marshal(&task)
	if err != nil {
		return err
	}

	var (
		queueIsEmpty  = clientv3.Compare(clientv3.Version(queuePrefix(task)).WithPrefix(), "=", 0)
		queueHasTasks = clientv3.Compare(clientv3.Version(queuePrefix(task)).WithPrefix(), "!=", 0)
		enqueueTask   = clientv3.OpPut(queuedKey(task), string(data))
	)

	queued, err := kv.Get(ctxt, queuePrefix(task), clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		return err
	}

	// given the queue has tasks already, then enqueue this task behind the others
	if queued.Count > 0 {
		result, err := kv.Txn(ctxt).
			If(queueHasTasks).
			Then(enqueueTask).
			Commit()

		if err != nil {
			return err
		}

		if result.Succeeded {
			// task has been enqueued
			return nil
		}
	}

	// queue is currently deemed empty

	// get the current number of running tasks in the same queue
	runningCount, version, err := getRunningCount(ctxt, kv, task)
	if err != nil {
		return err
	}

	runningCountUnchanged := clientv3.Compare(clientv3.Version(countKey(task)), "=", version)

	// if all the slots are currently taken
	if task.Concurrency <= int(runningCount) {
		// enqueue the task
		result, err := kv.Txn(ctxt).
			If(runningCountUnchanged).
			Then(enqueueTask).
			Commit()

		if err != nil {
			return err
		}

		if !result.Succeeded {
			// attempt to schedule the task again
			return Schedule(ctxt, kv, task)
		}

		return nil
	}

	// operations for setting a task in a running state
	setTaskRunning := []clientv3.Op{
		// set task in running state
		clientv3.OpPut(runningKey(task), string(data)),
		// update running counter
		clientv3.OpPut(countKey(task), fmt.Sprintf("%d", runningCount+1)),
	}

	result, err := kv.Txn(ctxt).
		If(runningCountUnchanged, queueIsEmpty).
		// set the task as running
		Then(setTaskRunning...).
		Commit()
	if err != nil {
		return err
	}

	if !result.Succeeded {
		// try again
		return Schedule(ctxt, kv, task)
	}

	return nil
}

// Consume receives tasks from the provided tasks channel and attempts to claim them
// If a successful claim occurs the work is delegated to the provided worker
// Once work has finished the task is released in the backing etcd and subsequent enqueued tasks are scheduled to run
func Consume(ctxt context.Context, kv clientv3.KV, lease clientv3.Lease, tasks <-chan task.Task, worker Worker) {
	for task := range tasks {
		tctxt, cancel := context.WithCancel(ctxt)

		// mark the task as claimed in etcd
		if err := claim(tctxt, kv, lease, task); err != nil {
			log.Println("claiming", task, err)

			// free any task associated resources
			cancel()
			continue
		}

		// execute work on task
		if err := worker.Do(task); err != nil {
			log.Println("doing", task, err)

			// mark tasks as failed in etcd
			fail(tctxt, kv, task)

			// free any task associated resources
			cancel()
			continue
		}

		// mark the task as finished and schedule next task
		if err := finish(tctxt, kv, task); err != nil {
			log.Println("finishing", task, err)
		}

		// free any task associated resources
		cancel()
	}
}

func claim(ctxt context.Context, kv clientv3.KV, lease clientv3.Lease, task task.Task) error {
	timeoutCtxt, cancel := context.WithTimeout(ctxt, reqTimeout)
	defer cancel()

	// create a 10 second lease on claim key
	leaseResp, err := lease.Grant(timeoutCtxt, 10)
	if err != nil {
		return err
	}

	var (
		claimDoesNotExist = clientv3.Compare(clientv3.Version(claimKey(task)), "=", 0)
		createNewClaim    = clientv3.OpPut(claimKey(task), "", clientv3.WithLease(leaseResp.ID))
	)

	resp, err := kv.Txn(timeoutCtxt).
		// given the claim has not been created yet
		If(claimDoesNotExist).
		// construct claim for task using lease
		Then(createNewClaim).
		Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		// revoke the lease
		if _, err := lease.Revoke(ctxt, leaseResp.ID); err != nil {
			return err
		}

		return ErrTaskAlreadyClaimed
	}

	go func() {
		// keep lease alive until root context is cancelled
		ch, err := lease.KeepAlive(ctxt, leaseResp.ID)
		if err != nil {
			log.Println("keeping lease alive", leaseResp.ID, err)
			return
		}

		for range ch {
			// consume until closed
			// sleep to avoid hammering etcd with keep-alives
			time.Sleep(time.Second)
		}

		log.Println("lease cancelled")
	}()

	return nil
}

func fail(ctxt context.Context, kv clientv3.KV, task task.Task) error { return nil }

func finish(rootCtxt context.Context, kv clientv3.KV, t task.Task) error {
	ctxt, cancel := context.WithTimeout(rootCtxt, reqTimeout)
	defer cancel()

	count, version, err := getRunningCount(ctxt, kv, t)
	if err != nil {
		return err
	}

	var (
		// initial operations include removal of current keys
		operations = []clientv3.Op{
			// remove claim on running key
			clientv3.OpDelete(claimKey(t)),
			// remove running key
			clientv3.OpDelete(runningKey(t)),
		}
		comparisons = []clientv3.Cmp{
			// given the running count has NOT changed
			clientv3.Compare(clientv3.Version(countKey(t)), "=", version),
		}
	)

	queued, err := kv.Get(ctxt, queuePrefix(t), clientv3.WithPrefix(), clientv3.WithLimit(1), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if queued.Count == 0 {
		// ensure queue remains empty during transaction
		queue := queuePrefix(t)
		comparisons = append(comparisons, clientv3.Compare(clientv3.Version(queue).WithPrefix(), "=", 0))

		// decrement running count if greater than zero
		if count > 0 {
			// decrement running count
			operations = append(operations, clientv3.OpPut(countKey(t), fmt.Sprintf("%d", count-1)))
		} else if version > 0 {
			// delete count (if it exists) as it is now zero
			operations = append(operations, clientv3.OpDelete(countKey(t)))
		}
	} else {
		next := queued.Kvs[0]
		// unmarshall next task
		var task task.Task
		if err := json.Unmarshal(next.Value, &task); err != nil {
			log.Println("unmarshalling task", err)
			return err
		}

		// enforce concurrency limit of queued task
		if count <= uint64(task.Concurrency) {
			// allocate the next task
			// if the next task has NOT changed
			comparisons = append(comparisons, clientv3.Compare(clientv3.Version(queuedKey(task)), "=", next.Version))
			// delete next task from queued
			operations = append(operations, clientv3.OpDelete(queuedKey(task)))
			// put next task in running
			operations = append(operations, clientv3.OpPut(runningKey(task), string(next.Value)))
		}
	}

	result, err := kv.Txn(ctxt).
		If(comparisons...).
		Then(operations...).
		Commit()
	if err != nil {
		return err
	}

	// if transaction failes
	if !result.Succeeded {
		// retry until success
		return finish(rootCtxt, kv, t)
	}

	return nil
}

// GetWating watches for tasks tasks being scheduled in the backing etcd and feeds
// them into the provided tasks channel
func GetWaiting(ctxt context.Context, kv clientv3.KV, tasks chan<- task.Task) error {
	resp, err := kv.Get(ctxt, runningNS, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, running := range resp.Kvs {
		// found task in running state
		var task task.Task
		if err := json.Unmarshal(running.Value, &task); err != nil {
			log.Println("error unmarshalling task", err)
			continue
		}

		// attempt to fetch claim for task
		claim, err := kv.Get(ctxt, claimKey(task))
		if err != nil {
			return err
		}

		// if a claim cannot be found
		if claim.Count < 1 {
			// send it for claiming
			tasks <- task
		}
	}

	return nil
}

// WatchRunning watches for tasks tasks being scheduled in the backing etcd and feeds
// them into the provided tasks channel
func WatchRunning(ctxt context.Context, watcher clientv3.Watcher, tasks chan<- task.Task) {
	log.Println("begin watching")
	ch := watcher.Watch(ctxt, runningNS, clientv3.WithPrefix())
	for resp := range ch {
		if resp.Err() != nil {
			log.Println("error from watch response", resp.Err())
			continue
		}

		if len(resp.Events) > 0 {
			for _, ev := range resp.Events {
				if ev.IsCreate() {
					// new running job!
					var task task.Task
					if err := json.Unmarshal(ev.Kv.Value, &task); err != nil {
						log.Println("error unmarshalling task", err)
						continue
					}

					tasks <- task
				}
			}
		}
	}
}

func getRunningCount(ctxt context.Context, kv clientv3.KV, task task.Task) (uint64, int64, error) {
	count, version, err := getUInt64(ctxt, kv, countKey(task))
	if err != nil {
		if err == ErrKeyNotFound {
			return 0, 0, nil
		}

		return 0, 0, err
	}

	return count, version, nil
}

func getUInt64(ctxt context.Context, kv clientv3.KV, key string) (uint64, int64, error) {
	resp, err := kv.Get(ctxt, key)
	if err != nil {
		return 0, 0, err
	}

	if len(resp.Kvs) > 0 {
		var (
			item     = resp.Kvs[0]
			val, err = strconv.ParseUint(string(item.Value), 10, 64)
		)

		return val, item.Version, err
	}

	return 0, 0, ErrKeyNotFound
}
