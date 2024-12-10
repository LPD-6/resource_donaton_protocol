// node main
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
	maxConcurrentTasks = 3
)

func generateNodeID() string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return fmt.Sprintf("node-%d", rng.Intn(10000)) // Unique random node ID
}

var (
	shutdown = make(chan struct{})
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	nodeID := generateNodeID()
	conn, err := net.Dial("tcp", coordinatorAddress)
	if err != nil {
		log.Printf("ERROR: failed to connect to coordinator: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("INFO: node %s connected to coordinator", nodeID)

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(nodeID); err != nil {
		log.Printf("ERROR: failed to register node: %v", err)
		return
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
		log.Printf("INFO: node %s shutting down gracefully", nodeID)
		wg.Done()
	}()
	// Continuously listen for tasks
	go func() {
		defer wg.Done()
		for {
			var task utils.Task
			if err := decoder.Decode(&task); err != nil {
				if err.Error() == "EOF" {
					log.Printf("Node %s received termination message. Shutting down.", nodeID)
					return
				} else if task.ID == utils.TerminationMessage {
					log.Printf("Node %s received termination message. Shutting down.", nodeID)
					return
				}
				log.Printf("Error receiving task: %v", err)
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
	}()

	// Receive tasks
	decoder := json.NewDecoder(conn)
	for {
		var task utils.Task
		if err := decoder.Decode(&task); err != nil {
			log.Printf("ERROR: error receiving task: %v", err)
			continue
		}

		// Assign task to processing channel
		select {
		case taskChan <- task:
			log.Printf("INFO: received task %s", task.ID)
		default:
			log.Printf("WARN: task %s dropped due to full task queue", task.ID)
		}
	}
}
