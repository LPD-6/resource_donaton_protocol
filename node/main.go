package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/LPD-6/resource_donaton_protocol/m/utils"
)

const (
	coordinatorAddress = "localhost:5000"
)

func generateNodeID() string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return fmt.Sprintf("node-%d", rng.Intn(10000)) // Unique random node ID
}

var (
	shutdown = make(chan struct{})
)

func main() {
	nodeID := generateNodeID()
	conn, err := net.Dial("tcp", coordinatorAddress)
	if err != nil {
		log.Fatalf("Failed to connect to coordinator: %v", err)
	}
	defer conn.Close()

	log.Printf("Node %s connected to coordinator", nodeID)

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	// Send node ID to coordinator
	if err := encoder.Encode(nodeID); err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// Signal handler for graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		close(shutdown)
		conn.Close()
		log.Printf("Node %s shutting down gracefully", nodeID)
		wg.Done()
	}()

	// Continuously receive tasks and process them
	go func() {
		for {
			select {
			case <-shutdown:
				return
			default:
				var task utils.Task
				if err := decoder.Decode(&task); err != nil {
					log.Printf("Failed to receive task: %v", err)
					return
				}

				log.Printf("Processing task %s", task.ID)
				sorted := utils.MergeSort(task.Array)

				// Send result back to coordinator
				result := utils.Result{TaskID: task.ID, SortedChunk: sorted}
				if err := encoder.Encode(result); err != nil {
					log.Printf("Failed to send result: %v", err)
					return
				}

				log.Printf("Completed task %s", task.ID)
			}
		}
	}()

	wg.Wait()
}
