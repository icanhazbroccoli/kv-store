package runner

import (
	"reflect"
	"testing"
)

func TestRun(t *testing.T) {
	// The tests effect is cumulative: they are running against a persistent
	// instance of a runner
	tests := []struct {
		task Task
		res  Result
	}{
		{
			task: Task{TaskType: TaskGet, Args: [2]string{"foo"}},
			res:  Result{Ok: false, Res: ""},
		},
		{
			task: Task{TaskType: TaskSet, Args: [2]string{"foo", "bar"}},
			res:  Result{Ok: true, Res: ""},
		},
		{
			task: Task{TaskType: TaskGet, Args: [2]string{"foo"}},
			res:  Result{Ok: true, Res: "bar"},
		},
	}

	t.Parallel()

	in := make(chan Task)
	out := make(chan Result)

	go Run(in, out)

	for _, tt := range tests {
		in <- tt.task
		res := <-out
		if !reflect.DeepEqual(tt.res, res) {
			t.Fatalf("unexpected result: want: %#v, got: %#v", tt.res, res)
		}
	}

	close(in)
	close(out)
}
