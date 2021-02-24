package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/aarondwi/antri/proto"
	"google.golang.org/grpc"
)

var writeRatio uint
var workerNumber uint
var messageSize uint
var batchNumPerWrite uint
var batchNumPerRead uint
var minSecondsFromNow uint
var maxSecondsFromNow uint
var duration uint

var address string

func init() {
	flag.UintVar(&writeRatio, "r", 50, "Ratio of Write (out of 100)")
	flag.UintVar(&workerNumber, "n", 100, "Number of worker (For both read and write)")
	flag.UintVar(&messageSize, "s", 1024, "Size for each message sent, in bytes")
	flag.UintVar(&batchNumPerWrite, "bw", 10, "Number of message put inside 1 AddTasksMessage")
	flag.UintVar(&batchNumPerRead, "br", 10, "Number of MaxN in the GetTasksRequest")
	flag.UintVar(&minSecondsFromNow, "min-ms", 0, "Min secondsFromNow to apply to message")
	flag.UintVar(&maxSecondsFromNow, "max-ms", 5, "Max secondsFromNow to apply to message")
	flag.UintVar(&duration, "d", 60, "Test Duration, in seconds")
	flag.StringVar(&address, "h", "127.0.0.1:15026", "AntriServer gRPC address")
}

func main() {
	flag.Parse()

	// a barrier to signal all goroutines
	// that they can start the main work
	wgStartRunTest := sync.WaitGroup{}
	wgStartRunTest.Add(1)

	// a barrier used by worker goroutines
	// that they are ready for the main work
	wgAllWriterReady := sync.WaitGroup{}
	wgAllReaderReady := sync.WaitGroup{}

	// a barrier to indicate that all the main work are done
	// after the timeout, of course
	wgAllWriterWorkDone := sync.WaitGroup{}
	wgAllReaderWorkDone := sync.WaitGroup{}

	// compute the worker's number
	writerNumber := uint(workerNumber * writeRatio / 100)
	readerNumber := workerNumber - writerNumber

	// using with cancel,
	// because we want to add some log indicating it is done
	finishContext, cancelFunc := context.WithCancel(context.Background())

	// writer setup
	writers := make([]*AntriWriteBenchClient, int(writerNumber))
	for i := 0; i < int(writerNumber); i++ {
		wgAllWriterReady.Add(1)
		wgAllWriterWorkDone.Add(1)

		go func(i int) {
			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()
			c := proto.NewAntriClient(conn)
			awbc := NewAntriWriteBenchClient(
				c,
				messageSize,
				batchNumPerWrite,
				uint32(minSecondsFromNow),
				uint32(maxSecondsFromNow),
			)
			writers[i] = awbc

			wgAllWriterReady.Done()
			wgStartRunTest.Wait()

			awbc.Run(finishContext)

			// do some stats
			wgAllWriterWorkDone.Done()
		}(i)
	}

	// reader setup
	readers := make([]*AntriReadBenchClient, int(readerNumber))
	for i := 0; i < int(readerNumber); i++ {
		wgAllReaderReady.Add(1)
		wgAllReaderWorkDone.Add(1)
		go func(i int) {
			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()
			c := proto.NewAntriClient(conn)

			arbc := NewAntriReadBenchClient(
				c,
				batchNumPerRead,
			)
			readers[i] = arbc

			wgAllReaderReady.Done()
			wgStartRunTest.Wait()

			arbc.Run(finishContext)

			// do some stats
			wgAllReaderWorkDone.Done()
		}(i)
	}

	// wait all writers & readers to be ready
	// we do not want the clients trying to connect to be counted in the duration
	wgAllReaderReady.Wait()
	wgAllWriterReady.Wait()

	// starting test
	go func() {
		time.Sleep(time.Duration(duration) * time.Second)
		log.Println("Test duration elapsed, stopping...")
		cancelFunc()
	}()
	log.Println("Starting test...")
	wgStartRunTest.Done()

	// barrier to wait for all workers to be done,
	// after the timeout is elapsed
	wgAllWriterWorkDone.Wait()
	wgAllReaderWorkDone.Wait()
	log.Println("All workers have shut down")

	/*
	 * Calculating statistics goes below here.
	 * For now, we do so manually.
	 */
	log.Println("Calculating statistics, please wait")

	var totalWrite, totalWriteSuccess, totalWriteFailed, totalWriteProcessedMessage int64
	for i := 0; i < int(writerNumber); i++ {
		totalWrite += int64(writers[i].processedCount)
		totalWriteSuccess += int64(writers[i].successCount)
		totalWriteFailed += int64(writers[i].failedCount)
		totalWriteProcessedMessage += int64(writers[i].messageProcessedCount)
	}
	totalWritePerSec := totalWrite / int64(duration)
	totalMsgWritePerSec := totalWriteProcessedMessage / int64(duration)

	var totalReadAndCommitted, totalRnCSuccess, totalRnCFailed, totalRnCProcessedMessage int64
	for i := 0; i < int(readerNumber); i++ {
		totalReadAndCommitted += int64(readers[i].processedCount)
		totalRnCSuccess += int64(readers[i].successCount)
		totalRnCFailed += int64(readers[i].failedCount)
		totalRnCProcessedMessage += int64(readers[i].messageProcessedCount)
	}
	totalRncPerSec := totalReadAndCommitted / int64(duration)
	totalMsgRnCPerSec := totalRnCProcessedMessage / int64(duration)

	// percentile calculation goes here
	//
	// percentile is calculated from all response, no matter success or not

	// AddTasks
	allWriteRequestDuration := make([]float64, 0, totalWrite)
	for i := 0; i < int(writerNumber); i++ {
		allWriteRequestDuration = append(allWriteRequestDuration, writers[i].timeTaken...)
	}
	sort.Float64s(allWriteRequestDuration)

	writep50 := calculatePercentileInMsec(allWriteRequestDuration, 50)
	writep75 := calculatePercentileInMsec(allWriteRequestDuration, 75)
	writep90 := calculatePercentileInMsec(allWriteRequestDuration, 90)
	writep95 := calculatePercentileInMsec(allWriteRequestDuration, 95)
	writep99 := calculatePercentileInMsec(allWriteRequestDuration, 99)

	// GetTasks + CommitTasks
	allReadRequestDuration := make([]float64, 0, int(totalReadAndCommitted))
	for i := 0; i < int(writerNumber); i++ {
		allReadRequestDuration = append(allReadRequestDuration, readers[i].timeTaken...)
	}
	sort.Float64s(allReadRequestDuration)
	readp50 := calculatePercentileInMsec(allReadRequestDuration, 50)
	readp75 := calculatePercentileInMsec(allReadRequestDuration, 75)
	readp90 := calculatePercentileInMsec(allReadRequestDuration, 90)
	readp95 := calculatePercentileInMsec(allReadRequestDuration, 95)
	readp99 := calculatePercentileInMsec(allReadRequestDuration, 99)

	/*
	 * report all result
	 */

	fmt.Println()
	fmt.Println("WRITES")
	fmt.Printf("Total All Writes: %d\n", totalWrite)
	fmt.Printf("Total All Write Messages: %d\n", totalWriteProcessedMessage)
	fmt.Printf("Average write/s: %d\n", totalWritePerSec)
	fmt.Printf("Average message write/s: %d\n", totalMsgWritePerSec)
	fmt.Printf("Total Successful Writes: %d\n", totalWriteSuccess)
	fmt.Printf("Total Failed Writes: %d\n", totalWriteFailed)
	fmt.Printf("p50: %f ms\n", writep50)
	fmt.Printf("p75: %f ms\n", writep75)
	fmt.Printf("p90: %f ms\n", writep90)
	fmt.Printf("p95: %f ms\n", writep95)
	fmt.Printf("p99: %f ms\n", writep99)

	fmt.Println()
	fmt.Println("GET + COMMIT")
	fmt.Printf("Total All Get + Commit: %d\n", totalReadAndCommitted)
	fmt.Printf("Total All Get + Commit Messages: %d\n", totalRnCProcessedMessage)
	fmt.Printf("Average get + commit/s: %d\n", totalRncPerSec)
	fmt.Printf("Average message get + commit/s: %d\n", totalMsgRnCPerSec)
	fmt.Printf("Total Successful Get + Commit: %d\n", totalRnCSuccess)
	fmt.Printf("Total Failed Get + Commit: %d\n", totalRnCFailed)
	fmt.Printf("p50: %f ms\n", readp50)
	fmt.Printf("p75: %f ms\n", readp75)
	fmt.Printf("p90: %f ms\n", readp90)
	fmt.Printf("p95: %f ms\n", readp95)
	fmt.Printf("p99: %f ms\n", readp99)
}
