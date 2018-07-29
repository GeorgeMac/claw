package task

type Task struct {
	ID          string `json:"id"`
	QueueID     string `json:"qid"`
	Concurrency int    `json:"concurrency"`
	Payload     []byte `json:"payload"`
}
