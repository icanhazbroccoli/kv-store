package runner

import "log"

type StoredMap map[string]string

type TaskType uint8

const (
	TaskSet TaskType = iota
	TaskGet
)

type Task struct {
	TaskType TaskType  `json:"task"`
	Args     [2]string `json:"args"`
}

type Result struct {
	Res string `json:"res"`
	Ok  bool   `json:"ok"`
}

func Run(in <-chan Task, out chan<- Result) {
	m := make(StoredMap)
	for task := range in {
		var res Result
		k, v := task.Args[0], task.Args[1]
		switch task.TaskType {
		case TaskGet:
			log.Printf("map runner received a get task: {key: %s}", k)
			lookup, ok := m[k]
			res.Res = lookup
			res.Ok = ok
		case TaskSet:
			log.Printf("map runner received a set task: {key: %s, val: %s}", k, v)
			m[k] = v
			res.Ok = true
		}
		out <- res
	}
}
