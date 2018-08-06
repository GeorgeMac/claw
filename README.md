_The Claw :point_up: ..._
--------

An etcd backed worker queue. Currently built as a library for projects written in the Go programming language.

Please note that I consider this project very much an experiment. It will likely remain volitile or eventually dissapear. If it solves something for you that is super amazing and I would love to talk to you about what that problem is and how it solves it.

## Design

```
   PRODUCERS                  "QUEUES"                         WORKERS

                                                          +--------------+
                                                          |              |
                                                       +-->  worker one  |
                                                       |  |              |
                             +-----------------+       |  +--------------+
+--------------+     +------->QQQQQQQQQQQQQQQ|R+--+    |
|              |     |       +-----------------+  |    |
|  schedule()  +-----+                            |    |  +--------------+
|              |     |          +--------------+  |    |  |              |
+--------------+     +---------->QQQQQQ|RRRRRRR+--+    +-->  worker two  |
                     |          +--------------+  |    |  |              |
+--------------+     |                            |    |  +--------------+
|              |     |  +----------------------+  |    |
|  schedule()  +-------->QQQQQQQQQQQQQQQQQQ|RRR+-------+
|              |     |  +----------------------+  |    |  +--------------+
+--------------+     |                            |    |  |              |
                     |              +----------+  |    +--> worker three |
+--------------+     +-------------->QQ|RRRRRRR+--+    |  |              |
|              |     |              +----------+  |    |  +--------------+
|  schedule()  +-----+                            |    |
|              |     |     +-------------------+  |    |         ...
+--------------+     +----->RRRRRRRRRRRRRRRRRRR+--+    |
                           +-------------------+  |    |  +--------------+
                                                  |    |  |              |
               +-+       +-+                      |    +-->   worker n   |
               |Q|ueued  |R|unning                +       |              |
               +-+       +-+                     ...      +--------------+
```

**Claw** is a work / task scheduling technology built in Go on Etcd V3. It has a lot of conceptual similarities to technologies such as Sidekiq or Resque in the Ruby and Redis world.

The key feature of Claw is flexibility to coordinate concurrency limits across _any_ number of "queues", where work is consumed by _any_ number of workers. This pool of workers may be consuming from 1, many or all the "queues" in the Etcd namespace. The aim is for workers to be able to scale horizontally or even fail and be recovered safely. While groups of tasks with concurrency limits ensure a fixed number of tasks can only be inflight at any one time.

For this to work, the concept of a "queue" may seem a little skewed. A "queue" in claw (note the quotes) is more a group of tasks which enforces to downstream consumers how many tasks can be in a _running_ state in any one moment. A task remains in a _queued_ state until it is suitable to be promoted to _running_ and actually actuated by a worker. This is in contrast to a more conventional purpose of a queue, such as to logically order tasks all for a given type of work or payload. Once in a _running_ state a consumer essentially just recieves a `[]byte` payload which can be handled whichever way the consumer wishes.

The final design decision to note is that the "queues" themselves are FIFO.
