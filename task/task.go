package task

import (
	"math/rand"
	"time"

	"github.com/oklog/ulid"
)

var (
	entropy = rand.New(rand.NewSource(time.Now().UnixNano()))
)

type Task struct {
	ID          string `json:"id"`
	QueueID     string `json:"qid"`
	Concurrency int    `json:"concurrency"`
	Payload     []byte `json:"payload"`
}

type Option func(*Task)

type Options []Option

func (o Options) Apply(t *Task) {
	for _, opt := range o {
		opt(t)
	}
}

func WithConcurrency(n int) Option {
	return func(t *Task) { t.Concurrency = n }
}

func New(queue string, payload []byte, opts ...Option) (t Task) {
	t.ID = ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
	t.QueueID = queue
	t.Payload = payload
	t.Concurrency = 1

	Options(opts).Apply(&t)

	return
}
