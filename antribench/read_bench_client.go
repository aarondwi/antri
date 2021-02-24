package main

import (
	"context"
	"log"
	"time"

	"github.com/aarondwi/antri/proto"
)

// AntriReadBenchClient bursts our server
// with GetTasks requests, directly followed by CommitTasks requests
type AntriReadBenchClient struct {
	c                     proto.AntriClient
	batchNumPerRead       uint
	successCount          int
	failedCount           int
	processedCount        int
	messageProcessedCount int
	timeTaken             []float64
}

// NewAntriReadBenchClient creates our new benchmark reader object
func NewAntriReadBenchClient(
	c proto.AntriClient,
	batchNumPerRead uint,
) *AntriReadBenchClient {
	return &AntriReadBenchClient{
		c:               c,
		batchNumPerRead: batchNumPerRead,
		// we do not know how much would be needed
		// just prepare quite a bit for the beginning
		timeTaken: make([]float64, 0, 8192),
	}
}

// Run our benchmark reader, until ctx is Done, expired, or cancelled
func (arbc *AntriReadBenchClient) Run(ctx context.Context) {
	// run the function till context is timeout/done
	for {
		select {
		case <-ctx.Done():
			return
		default:
			startTime := time.Now()

			// the main work
			getResp, err := arbc.c.GetTasks(ctx, &proto.GetTasksRequest{
				MaxN: uint32(batchNumPerRead),
			})
			if err != nil {
				arbc.failedCount++
				continue
			}

			keys := make([]string, 0, len(getResp.Tasks))
			for _, t := range getResp.Tasks {
				keys = append(keys, t.Key)
			}
			commitResp, err := arbc.c.CommitTasks(ctx, &proto.CommitTasksRequest{
				Keys: keys,
			})

			// result tracking
			arbc.timeTaken = append(arbc.timeTaken, float64(time.Now().Sub(startTime)))
			arbc.processedCount++

			if err != nil {
				log.Println(err)
				arbc.failedCount++
				continue
			}
			if len(getResp.Tasks) != len(commitResp.Keys) {
				arbc.failedCount++
				continue
			}
			arbc.successCount++
			arbc.messageProcessedCount += len(commitResp.Keys)
		}
	}
}
