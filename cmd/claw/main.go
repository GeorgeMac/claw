package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/georgemac/claw/queue"
	"github.com/georgemac/claw/task"
	"github.com/oklog/ulid"
)

const clawNS = "theclaw.run/"

func main() {
	var (
		ctxt, cancel = context.WithCancel(context.Background())
		cli, err     = clientv3.New(clientv3.Config{Endpoints: []string{os.Getenv("ETCD_ADDR")}})
		tasks        = make(chan task.Task, 10)
	)
	defer cancel()

	if err != nil {
		log.Fatal(err)
	}

	cli.KV = namespace.NewKV(cli.KV, clawNS)
	cli.Lease = namespace.NewLease(cli.Lease, clawNS)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, clawNS)

	if len(os.Args) > 1 {
		if len(os.Args) < 4 && os.Args[1] != "put" {
			log.Println("usage: put [queue_name] [concurrency]")
			os.Exit(1)
		}

		var (
			t       = time.Now()
			entropy = rand.New(rand.NewSource(t.UnixNano()))
			ulid    = ulid.MustNew(ulid.Timestamp(t), entropy)
		)

		conc, err := strconv.ParseInt(os.Args[3], 10, 32)
		if err != nil {
			log.Fatal(err)
		}

		if err := queue.Schedule(ctxt, cli.KV, task.Task{
			ID:          ulid.String(),
			QueueID:     os.Args[2],
			Concurrency: int(conc),
		}); err != nil {
			log.Fatal(err)
		}

		return
	}

	go queue.FeedRunning(ctxt, cli.Watcher, tasks)

	// start 1 consumers
	for i := 0; i < 1; i++ {
		go queue.Consume(ctxt, cli.KV, cli.Lease, tasks, queue.WorkerFunc(func(task task.Task) error {
			for i := 0; i < 10; i++ {
				time.Sleep(1 * time.Second)
				fmt.Println(task.QueueID+"/"+task.ID+":", "slept for", i+1, "second(s)")
			}

			fmt.Println(task.ID+":", "done")

			return nil
		}))
	}

	log.Println("workers started, waiting for tasks...")

	select {}
}
