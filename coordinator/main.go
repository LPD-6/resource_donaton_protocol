package main

import (
	"encoding/json"
	"errors"
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
	port          = ":5000"
	taskChunkSize = 10 // Number of elements per task
)

var (
	nodes    = make(map[string]net.Conn) // Active nodes
	tasks    = make([]utils.Task, 0)     // Pending tasks
	results  = make([][]int, 0)          // Results from nodes
	taskLock sync.Mutex
	shutdown = make(chan struct{})
)

func main() {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to start coordinator: %v", err)
	}
	defer listener.Close()

	log.Printf("Coordinator listening on %s", port)

	// Signal handling for graceful shutdown
	go handleShutdown(listener)

	// Accept connections
	go acceptConnections(listener)

	// Create example tasks
	createExampleTasks(50)

	// Periodically assign tasks to nodes
	go assignTasks()

	// Wait for shutdown signal
	<-shutdown
	log.Println("Coordinator shutting down gracefully")
}

func handleShutdown(listener net.Listener) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	<-sig
	close(shutdown)
	listener.Close()

	// Close all node connections
	taskLock.Lock()
	defer taskLock.Unlock()
	for _, conn := range nodes {
		conn.Close()
	}
}

func createExampleTasks(size int) {
	array := generateRandomArray(size)

	// Split the array into chunks and create tasks
	for i := 0; i < len(array); i += taskChunkSize {
		end := i + taskChunkSize
		if end > len(array) {
			end = len(array)
		}
		task := utils.Task{
			ID:    fmt.Sprintf("task-%d", i/taskChunkSize),
			Array: array[i:end],
		}
		tasks = append(tasks, task)
	}

	log.Printf("Created %d tasks", len(tasks))
}

func generateRandomArray(size int) []int {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	array := make([]int, size)
	for i := range array {
		array[i] = rng.Intn(1000) // Random numbers between 0 and 999
	}
	return array
}

func acceptConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("Listener closed")
				return
			}
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		// Register the node
		go handleNode(conn)
	}
}

func handleNode(conn net.Conn) {
	defer conn.Close()

	var nodeID string
	decoder := json.NewDecoder(conn)

	// Initial handshake: Receive node ID
	if err := decoder.Decode(&nodeID); err != nil {
		log.Printf("Error: %v", utils.ErrNodeRegistration)
		return
	}

	taskLock.Lock()
	nodes[nodeID] = conn
	taskLock.Unlock()

	log.Printf("Node %s registered", nodeID)

	// Continuously receive results from the node
	for {
		var result utils.Result
		if err := decoder.Decode(&result); err != nil {
			log.Printf("Error: %v for node %s: %v", utils.ErrNodeDisconnected, nodeID, err)
			taskLock.Lock()
			delete(nodes, nodeID)
			taskLock.Unlock()
			break
		}

		// Save result and log
		taskLock.Lock()
		results = append(results, result.SortedChunk)
		taskLock.Unlock()
		log.Printf("Received result for task %s from node %s", result.TaskID, nodeID)
	}
}

func assignTasks() {
	for {
		select {
		case <-shutdown:
			return
		default:
			time.Sleep(2 * time.Second)

			taskLock.Lock()
			defer taskLock.Unlock()

			if len(tasks) == 0 {
				continue
			}

			for nodeID, conn := range nodes {
				if len(tasks) == 0 {
					break
				}

				// Send a task
				task := tasks[0]
				tasks = tasks[1:]
				encoder := json.NewEncoder(conn)
				if err := encoder.Encode(task); err != nil {
					log.Printf("Error: %v for node %s: %v", utils.ErrTaskAssignment, nodeID, err)
					tasks = append(tasks, task) // Re-add task if sending fails
				} else {
					log.Printf("Assigned task %s to node %s", task.ID, nodeID)
				}
			}
		}
	}
}

func aggregateResults() ([]int, error) {
	if len(results) == 0 {
		return nil, utils.ErrResultAggregation
	}

	finalArray := make([]int, 0)
	for _, chunk := range results {
		finalArray = utils.Merge(finalArray, chunk)
	}
	return finalArray, nil
}
