package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/coreos/etcd/clientv3"
	etcdnamespace "github.com/coreos/etcd/clientv3/namespace"
	"github.com/georgemac/claw/queue"
	"github.com/georgemac/claw/task"
	"github.com/oklog/ulid"
)

const namespace = "github.com/georgemac/claw/"

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

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "put":
			if len(os.Args) < 4 {
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

			if err := queue.Schedule(ctxt, etcdnamespace.NewKV(cli.KV, namespace), task.Task{
				ID:          ulid.String(),
				QueueID:     os.Args[2],
				Concurrency: int(conc),
			}); err != nil {
				log.Fatal(err)
			}

			return
		case "ls":
			stats, err := queue.List(ctxt, cli.KV)
			if err != nil {
				log.Fatal(err)
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.AlignRight|tabwriter.Debug)
			fmt.Fprintln(w, "queue name\tqueued tasks\tready tasks\trunning tasks\t")
			for _, stat := range stats {
				fmt.Fprintf(w, "%s\t%d\t%d\t%d\t\n", stat.QueueName, stat.QueuedCount, stat.RunningCount-stat.ClaimedCount, stat.ClaimedCount)
			}
			w.Flush()

			return
		default:
			log.Println("usage: claw <cmd> [queue_name] [concurrency]")
			os.Exit(1)
		}

	}

	var (
		kv      = etcdnamespace.NewKV(cli.KV, namespace)
		watcher = etcdnamespace.NewWatcher(cli.Watcher, namespace)
		lease   = etcdnamespace.NewLease(cli.Lease, namespace)
	)

	// begin listening for newly scheduled tasks
	go queue.WatchRunning(ctxt, watcher, tasks)

	go func() {
		// get any tasks stuck in a ready state
		if err := queue.GetWaiting(ctxt, kv, tasks); err != nil {
			log.Println(err)
		}
	}()

	// start 1 consumers
	for i := 0; i < 1; i++ {
		go queue.Consume(ctxt, kv, lease, tasks, queue.WorkerFunc(func(task task.Task) error {
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
