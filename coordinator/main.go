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

const (
	port          = ":5000"
	taskChunkSize = 10 // Number of elements per task
)

var (
	nodes          = make(map[string]net.Conn) // Active nodes
	tasks          = make([]utils.Task, 0)     // Pending tasks
	results        = make([][]int, 0)          // Results from nodes
	taskLock       sync.Mutex
	nodeLock       sync.Mutex
	shutdown       = make(chan struct{})
	allTasksDone   = make(chan struct{}, 1) // Notify when all tasks are completed
	nodeHeartbeats = make(map[string]time.Time)
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("ERROR: failed to start coordinator: %v", err)
	}
	defer listener.Close()

	log.Printf("Coordinator listening on %s", port)

	// Create tasks
	createExampleTasks(50)

	// Accept connections
	go acceptConnections(listener)

	// Start task assignment loop
	go assignTasks()

	// Heartbeat checker
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				checkNodeHeartbeats()
			case <-shutdown:
				return
			}
		}
	}()

	// Wait for shutdown or all tasks to complete
	select {
	case <-shutdown:
		log.Println("INFO: coordinator shutting down gracefully")
		handleShutdown(listener, false)
	case <-allTasksDone:
		log.Println("INFO: all tasks completed. aggregating results.")
		finalArray, err := aggregateResults()
		if err != nil {
			log.Printf("ERROR: error during result aggregation: %v", err)
		} else {
			log.Printf("INFO: final sorted array: %v", finalArray)
		}
		handleShutdown(listener, true)
	}
}

func handleShutdown(listener net.Listener, tasksComplete bool) {
	// Cleanup listener and node connections
	listener.Close()
	taskLock.Lock()
	defer taskLock.Unlock()
	for _, conn := range nodes {
		conn.Close()
	}

	if !tasksComplete {
		log.Println("INFO: attempting to aggregate partial results during shutdown.")
		finalArray, err := aggregateResults()
		if err != nil {
			log.Printf("ERROR: error aggregating partial results: %v", err)
		} else {
			log.Printf("INFO: partially sorted array: %v", finalArray)
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

	log.Printf("INFO: created %d tasks", len(tasks))
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
				log.Println("INFO: listener closed")
				return
			}
			log.Printf("ERROR: error accepting connection: %v", err)
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
		log.Printf("ERROR: error registering node: %v", utils.ErrNodeRegistration)
		return
	}

	nodeLock.Lock()
	nodes[nodeID] = conn
	nodeHeartbeats[nodeID] = time.Now()
	nodeLock.Unlock()

	log.Printf("INFO: node %s registered", nodeID)

	// Continuously listen for results and heartbeats
	for {
		var msg struct {
			Type string
			Data interface{}
		}
		if err := decoder.Decode(&msg); err != nil {
			log.Printf("ERROR: node %s disconnected: %v", nodeID, utils.ErrNodeDisconnected)
			nodeLock.Lock()
			delete(nodes, nodeID)
			delete(nodeHeartbeats, nodeID)
			nodeLock.Unlock()
			break
		}

		switch msg.Type {
		case "result":
			var result utils.Result
			if err := json.Unmarshal(msg.Data.([]byte), &result); err != nil {
				log.Printf("ERROR: error unmarshaling result from node %s: %v", nodeID, err)
				continue
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

			log.Printf("INFO: received result for task %s from node %s", result.TaskID, nodeID)

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
			}
		case "heartbeat":
			nodeLock.Lock()
			nodeHeartbeats[nodeID] = time.Now()
			nodeLock.Unlock()
			log.Printf("INFO: received heartbeat from node %s", nodeID)
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
			if len(tasks) == 0 {
				taskLock.Unlock()
				continue
			}

			nodeLock.Lock()
			for nodeID, conn := range nodes {
				if len(tasks) == 0 {
					break
				}

				// Find the first uncompleted task
				var taskToSend *utils.Task
				for i := range tasks {
					if !tasks[i].Completed {
						taskToSend = &tasks[i]
						break
					}
				}

				// If no uncompleted task is found, skip assignment
				if taskToSend == nil {
					break
				}

				// Assign task
                encoder := json.NewEncoder(conn)
                if err := encoder.Encode(*taskToSend); err!= nil {
                    log.Printf("ERROR: error assigning task to node %s: %v", nodeID, utils.ErrTaskAssignment)
                } else {
                    log.Printf("INFO: assigned task %s to node %s", taskToSend.ID, nodeID)
                }
                tasks = tasks[1:]
            }
            nodeLock.Unlock()
            taskLock.Unlock()
		}
	}
}

func checkNodeHeartbeats() {
    nodeLock.Lock()
    currentTime := time.Now()
    for nodeID, lastHeartbeat := range nodeHeartbeats {
        if currentTime.Sub(lastHeartbeat) > 30*time.Second {
            log.Printf("WARN: node %s missed heartbeat, removing...", nodeID)
            delete(nodes, nodeID)
            delete(nodeHeartbeats, nodeID)
        }
    }
    nodeLock.Unlock()
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
