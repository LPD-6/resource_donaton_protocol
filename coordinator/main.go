// coordinator main
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/LPD-6/resource_donaton_protocol/m/utils"
)

type Node struct {
	Conn net.Conn
	Busy bool
}

const (
	port          = ":5000"
	taskChunkSize = 10 // Number of elements per task
)

var (
	nodes        = make(map[string]Node) // Active nodes
	tasks        = make([]utils.Task, 0) // Pending tasks
	results      = make([][]int, 0)      // Results from nodes
	taskLock     sync.Mutex
	nodeLock     sync.Mutex
	shutdown     = make(chan struct{})
	allTasksDone = make(chan struct{}, 1) // Notify when all tasks are completed
)

func main() {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to start coordinator: %v", err)
	}
	defer listener.Close()

	log.Printf("Coordinator listening on %s", port)

	// Create tasks
	createExampleTasks(500)

	// Accept connections
	go acceptConnections(listener)

	// Start task assignment loop
	go assignTasks()

	// Wait for shutdown or all tasks to complete
	select {
	case <-shutdown:
		log.Println("Coordinator shutting down gracefully")
		handleShutdown(listener, false) // Attempt to aggregate partial results
	case <-allTasksDone:
		log.Println("All tasks completed. Aggregating results.")
		finalArray, err := aggregateResults()
		if err != nil {
			log.Printf("Error during result aggregation: %v", err)
		} else {
			log.Printf("Final Sorted Array: %v", finalArray)
		}
		handleShutdown(listener, true)
	}
}

func handleShutdown(listener net.Listener, tasksComplete bool) {
	// Close the listener to prevent new connections
	listener.Close()

	// Close all node connections
	nodeLock.Lock()
	for nodeID, node := range nodes {
		log.Printf("Closing connection for node %s", nodeID)
		node.Conn.Close()
		delete(nodes, nodeID)
	}
	nodeLock.Unlock()

	if !tasksComplete {
		log.Println("Attempting to aggregate partial results during shutdown.")
		finalArray, err := aggregateResults()
		if err != nil {
			log.Printf("Error aggregating partial results: %v", err)
		} else {
			log.Printf("Partially Sorted Array: %v", finalArray)
		}
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

	// Receive node ID
	if err := decoder.Decode(&nodeID); err != nil {
		log.Printf("Error registering node: %v", utils.ErrNodeRegistration)
		return
	}

	nodeLock.Lock()
	nodes[nodeID] = Node{Conn: conn, Busy: false}
	nodeLock.Unlock()

	log.Printf("Node %s registered", nodeID)

	// Continuously listen for results
	for {
		var result utils.Result
		if err := decoder.Decode(&result); err != nil {
			log.Printf("Node %s disconnected: %v", nodeID, utils.ErrNodeDisconnected)
			nodeLock.Lock()
			delete(nodes, nodeID)
			nodeLock.Unlock()
			break
		}

		taskLock.Lock()
		for i := range tasks {
			if tasks[i].ID == result.TaskID {
				tasks[i].Completed = true
				break
			}
		}
		results = append(results, result.SortedChunk)
		taskLock.Unlock()

		nodeLock.Lock()
		node := nodes[nodeID]
		node.Busy = false
		nodeLock.Unlock()

		log.Printf("Received result for task %s from node %s", result.TaskID, nodeID)

		// Check if all tasks are completed
		allCompleted := true
		taskLock.Lock()
		for _, task := range tasks {
			if !task.Completed {
				allCompleted = false
				break
			}
		}
		taskLock.Unlock()
		if allCompleted {
			allTasksDone <- struct{}{}

			// Send termination message to the node
			encoder := json.NewEncoder(conn)
			if err := encoder.Encode(utils.TerminationMessage); err != nil {
				log.Printf("Error sending termination message to node %s: %v", nodeID, err)
			}
			break
		}
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
			nodeLock.Lock()

			// Find an available node and an uncompleted, unassigned task
			for nodeID, node := range nodes {
				if !node.Busy {
					for i := range tasks {
						if !tasks[i].Completed && !tasks[i].Assigned {
							tasks[i].Assigned = true
							node := nodes[nodeID]
							node.Busy = true
							encoder := json.NewEncoder(node.Conn)
							if err := encoder.Encode(tasks[i]); err != nil {
								log.Printf("Error assigning task to node %s: %v", nodeID, utils.ErrTaskAssignment)
							} else {
								log.Printf("Assigned task %s to node %s", tasks[i].ID, nodeID)
							}
							goto NextIteration
						}
					}
				}
			NextIteration:
			}

			nodeLock.Unlock()
			taskLock.Unlock()
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
	log.Printf("Final Sorted Array: %v", finalArray)
	return finalArray, nil
}
