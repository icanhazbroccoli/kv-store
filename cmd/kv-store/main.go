package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/icanhazbroccoli/kv-store/pkg/runner"
)

var numBuckets int

func extractKV(r *http.Request, expectVal bool) (string, string, error) {
	key, ok := r.URL.Query()["k"]
	if !ok {
		return "", "", fmt.Errorf("invalid request: `k` argument is missing")
	}
	if !expectVal {
		return key[0], "", nil
	}
	val, ok := r.URL.Query()["v"]
	if !ok {
		return "", "", fmt.Errorf("invalid request: `v` argument is missing")
	}
	return key[0], val[0], nil
}

func httpServeGetFunc(in chan<- runner.Task, out <-chan runner.Result) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		k, _, err := extractKV(r, false)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		req := runner.Task{
			TaskType: runner.TaskGet,
			Args:     [2]string{k, ""},
		}
		in <- req
		resp := <-out
		data, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		w.Header().Add("Content-Type", "application/json")
		fmt.Fprintf(w, string(data))
	}
}

func httpServeSetFunc(in chan<- runner.Task, out <-chan runner.Result) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		k, v, err := extractKV(r, true)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		req := runner.Task{
			TaskType: runner.TaskSet,
			Args:     [2]string{k, v},
		}
		in <- req
		resp := <-out
		data, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		w.Header().Add("Content-Type", "application/json")
		if resp.Ok {
			fmt.Fprintf(w, string(data))
		}
	}
}

func hashBucket(key string, buckets int) int {
	h := 0
	for _, ch := range key {
		h += int(ch)
	}
	return h % buckets
}

func main() {
	flag.IntVar(&numBuckets, "buckets", 1, "Number of buckets")
	flag.Parse()

	in := make(chan runner.Task)
	out := make(chan runner.Result)

	go runner.Run(in, out)

	http.HandleFunc("/get", httpServeGetFunc(in, out))
	http.HandleFunc("/set", httpServeSetFunc(in, out))

	log.Fatal(http.ListenAndServe(":14000", nil))
}
