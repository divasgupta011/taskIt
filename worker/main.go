package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ctx = context.Background()
)

type Task struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	log.Println("Worker started, waiting for tasks...")

	for {
		result, err := rdb.BRPop(ctx, 5*time.Second, "task_queue").Result()
		if err == redis.Nil {
			// No task, queue is empty
			continue
		} else if err != nil {
			log.Printf("Error fetching task: %v", err)
			continue
		}

		// BRPOP returns [queueName, value]
		if len(result) != 2 {
			continue
		}

		var task Task
		err = json.Unmarshal([]byte(result[1]), &task)
		if err != nil {
			log.Printf("Invalid task JSON: %v", err)
			continue
		}

		// Simulate processing
		log.Printf("Processing task %s: %s", task.ID, task.Payload)
		time.Sleep(1 * time.Second)
	}
}
