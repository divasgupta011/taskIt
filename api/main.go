package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	rdb *redis.Client
	ctx = context.Background()
)

type Task struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}

func enqueueHandler(w http.ResponseWriter, r *http.Request) {
	var task Task

	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	task.ID = fmt.Sprintf("task-%d", time.Now().UnixNano())

	data, err := json.Marshal(task)
	if err != nil {
		http.Error(w, "Failed to serialize task", http.StatusInternalServerError)
		return
	}

	err = rdb.LPush(ctx, "task_queue", data).Err()
	if err != nil {
		http.Error(w, "Failed to enqueue task", http.StatusInternalServerError)
		return
	}

	log.Printf("Enqueued task: %s", task.ID)
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(fmt.Sprintf("Task %s enqueued", task.ID)))
}

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	http.HandleFunc("/enqueue", enqueueHandler)

	log.Println("API listening on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
