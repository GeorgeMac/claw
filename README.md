The Claw
--------

An etcd backed worker queue. Initially built solely for use in Go, but could be adapted in the future.

## Design

The claw uses etcd keys/value pairs to orchestrate a protocol of work between consumers and producers. Producers "schedule" tasks to be executed by workers. Tasks contain:

1. Unique time-ordered keys.
2. A queue on which to be published.
3. A concurrency, which describes the number of tasks which can be inflight inclusive of this one.
4. A payload which is a slice of bytes.

Consumers currently subscribe `queue.Worker` onto tasks publish into the "running" keyspace. Consumers make claims on tasks to ensure that no two workers claim the same task. Once a consumer finishes a task, the claim and the running key is removed. Any waiting tasks in the "queued" keyspace is promoted into running,given it does not violate any concurrency constraints.

Because of this, scheduling needs to be more cooperative then a simple "put" into the "queued" or "running" keyspaces. When a producers schedules a task, it fist ensures that no existing tasks are "queued". If there are "queued" tasks, it simply enqueues the current task at the back of the respective queue. If there are no "queued" tasks, it promotes the task being scheduled straight into the "running" queue, given concurrency constraints are met.
