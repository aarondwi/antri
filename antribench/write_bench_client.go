package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/aarondwi/antri/proto"
)

// AntriWriteBenchClient bursts our server
// with AddTasks requests
type AntriWriteBenchClient struct {
	c                     proto.AntriClient
	messages              [][]byte
	minSecondsFromNow     uint32
	maxSecondsFromNow     uint32
	successCount          int
	failedCount           int
	processedCount        int
	messageProcessedCount int
	timeTaken             []float64
}

// NewAntriWriteBenchClient creates our new benchmark writer object
func NewAntriWriteBenchClient(
	c proto.AntriClient,
	messageSize uint,
	batchNumPerWrite uint,
	minSecondsFromNow uint32,
	maxSecondsFromNow uint32) *AntriWriteBenchClient {

	// pre-made the messages content
	//
	// We do not want to allocate too much client-side
	messages := make([][]byte, int(batchNumPerWrite))
	for i := 0; i < int(batchNumPerWrite); i++ {
		messages[i] = randStringBytes(int(messageSize))
	}

	awbc := &AntriWriteBenchClient{
		c:                 c,
		messages:          messages,
		minSecondsFromNow: minSecondsFromNow,
		maxSecondsFromNow: maxSecondsFromNow,

		// we do not know how much would be needed
		// just prepare quite a bit for the beginning
		timeTaken: make([]float64, 0, 8192),
	}

	return awbc
}

// Run our benchmark writer, until ctx is Done, expired, or cancelled
func (awbc *AntriWriteBenchClient) Run(ctx context.Context) {
	// initialize once
	// reduce allocation during the test
	tasks := make([]*proto.NewTask, 0, len(awbc.messages))

	// run the function till context is timeout/done
	for {
		select {
		case <-ctx.Done():
			return
		default:
			startTime := time.Now()

			// the main work
			for i := 0; i < len(awbc.messages); i++ {
				tasks = append(tasks, &proto.NewTask{
					Content: awbc.messages[i],
					SecondsFromNow: awbc.minSecondsFromNow +
						uint32(rand.Intn(int(awbc.maxSecondsFromNow-awbc.minSecondsFromNow))),
				})
			}
			resp, err := awbc.c.AddTasks(ctx, &proto.AddTasksRequest{Tasks: tasks})

			// result tracking
			awbc.timeTaken = append(awbc.timeTaken, float64(time.Now().Sub(startTime)))
			awbc.processedCount++

			if err != nil {
				log.Println(err)
				awbc.failedCount++
				continue
			}
			if !resp.Result {
				awbc.failedCount++
				continue
			}
			awbc.successCount++
			awbc.messageProcessedCount += len(tasks)

			// reset tasks' content
			tasks = tasks[:0]
		}
	}
}
