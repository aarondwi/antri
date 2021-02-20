package main

import (
	"bytes"
	"context"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aarondwi/antri/proto"
	"google.golang.org/grpc"
)

var addr = "127.0.0.1:3000"

func TestAntriServerParameter(t *testing.T) {
	_, err := New(-1, 1)
	if err == nil {
		log.Fatalf("taskTimeout negative value should be error, but it is not")
	}

	_, err = New(1, -1)
	if err == nil {
		log.Fatalf("checkpointDuration negative value should be error, but it is not")
	}
}

func TestAddRetrieveCommitMultipleTask(t *testing.T) {
	err := os.RemoveAll("data")
	if err != nil {
		log.Fatal(err)
	}

	as, err := New(1, 1)
	if err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	gs := grpc.NewServer()
	go as.Run(gs, lis)

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	c := proto.NewAntriClient(conn)

	tasks := []*proto.NewTask{
		{
			Content:        []byte("HelloWorld1"),
			SecondsFromNow: 2,
		},
		{
			Content:        []byte("HelloWorld2"),
			SecondsFromNow: 0,
		},
		{
			Content:        []byte("HelloWorld3"),
			SecondsFromNow: 0,
		},
	}
	addTaskResp, err := c.AddTasks(
		context.Background(),
		&proto.AddTasksRequest{
			Tasks: tasks,
		})

	if err != nil {
		log.Fatalf("AddTasks should not return an error, but we got: %v", err)
	}
	if !addTaskResp.Result {
		log.Fatal("AddTasks resp.Result should be `true`, but it is not")
	}

	retrievedTasks, err := c.GetTasks(context.Background(),
		&proto.GetTasksRequest{MaxN: 2})
	if err != nil {
		log.Fatalf("GetTasks should not return an error, but we got: %v", err)
	}

	if len(retrievedTasks.Tasks) != 2 {
		t.Fatalf("It should return 2 tasks, but instead we got %v", retrievedTasks.Tasks)
	}
	if !bytes.Equal(retrievedTasks.Tasks[0].Content, tasks[1].Content) {
		t.Fatalf("Should be matched, but instead we got %v and %v",
			retrievedTasks.Tasks[0].Content, tasks[1].Content)
	}
	if !bytes.Equal(retrievedTasks.Tasks[1].Content, tasks[2].Content) {
		t.Fatalf("Should be matched, but instead we got %v and %v",
			retrievedTasks.Tasks[1].Content, tasks[2].Content)
	}

	keysToCommit := make([]string, 10)
	keysToCommit = append(keysToCommit, retrievedTasks.Tasks[0].Key)
	keysToCommit = append(keysToCommit, retrievedTasks.Tasks[1].Key)
	keysToCommit = append(keysToCommit, "Non Existent Key")

	commitResult, err := c.CommitTasks(context.Background(), &proto.CommitTasksRequest{
		Keys: keysToCommit,
	})
	if err != nil {
		log.Fatalf("CommitTasks should not return an error, but we got: %v", err)
	}
	if len(commitResult.Keys) != 2 {
		log.Fatalf("commitResult resp.Result should be 2, as 1 of them is non-existent, but instead we got %d",
			len(commitResult.Keys))
	}

	as.Close()
	time.Sleep(1 * time.Second) // give time for the system to shutdown
}

func TestAddRetrieveTimeoutReretrieveCommit(t *testing.T) {
	err := os.RemoveAll("data")
	if err != nil {
		log.Fatal(err)
	}

	as, err := New(1, 1)
	if err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	gs := grpc.NewServer()
	go as.Run(gs, lis)

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	c := proto.NewAntriClient(conn)

	addTaskResp, err := c.AddTasks(context.Background(), &proto.AddTasksRequest{
		Tasks: []*proto.NewTask{
			{
				Content:        []byte("anothercontent"),
				SecondsFromNow: 0,
			},
		},
	})
	if !addTaskResp.Result {
		log.Fatal("AddTasks resp.Result should be `true`, but it is not")
	}

	retrievedTasks, err := c.GetTasks(context.Background(),
		&proto.GetTasksRequest{MaxN: 1})
	if len(retrievedTasks.Tasks) != 1 {
		t.Fatalf("It should return 1 task, but instead we got %v", retrievedTasks.Tasks)
	}

	time.Sleep(2 * time.Second)

	retrievedTimeoutTasks, err := c.GetTasks(context.Background(),
		&proto.GetTasksRequest{MaxN: 1})
	if len(retrievedTimeoutTasks.Tasks) != 1 {
		t.Fatalf("It should return 1 task, but instead we got %v", retrievedTasks.Tasks)
	}
	if retrievedTasks.Tasks[0].Key != retrievedTimeoutTasks.Tasks[0].Key {
		t.Fatalf("Should be the same key, but instead we got %s and %s",
			retrievedTasks.Tasks[0].Key, retrievedTimeoutTasks.Tasks[0].Key)
	}

	keysToCommit := make([]string, 10)
	keysToCommit = append(keysToCommit, retrievedTasks.Tasks[0].Key)

	commitResult, err := c.CommitTasks(context.Background(), &proto.CommitTasksRequest{
		Keys: keysToCommit,
	})
	if len(commitResult.Keys) != 1 {
		log.Fatalf("commitResult resp.Result should be 1, but instead we got %d",
			len(commitResult.Keys))
	}

	as.Close()
	time.Sleep(1 * time.Second) // give time for the system to shutdown
}

func TestValueNotProvided(t *testing.T) {
	err := os.RemoveAll("data")
	if err != nil {
		log.Fatal(err)
	}

	as, err := New(1, 1)
	if err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	gs := grpc.NewServer()
	go as.Run(gs, lis)

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	c := proto.NewAntriClient(conn)

	tasks := make([]*proto.NewTask, 5)
	tasks = append(tasks, &proto.NewTask{
		Content:        []byte(""),
		SecondsFromNow: 0,
	})

	_, err = c.AddTasks(context.Background(), &proto.AddTasksRequest{
		Tasks: tasks,
	})
	if err == nil {
		log.Fatalf("AddTasks should return error because empty Content, but it is not")
	}

	as.Close()
	time.Sleep(1 * time.Second) // give time for the system to shutdown
}

func TestLockWaitFlow(t *testing.T) {
	err := os.RemoveAll("data")
	if err != nil {
		log.Fatal(err)
	}

	as, err := New(1, 1)
	if err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	gs := grpc.NewServer()
	go as.Run(gs, lis)

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	c := proto.NewAntriClient(conn)

	keyRetrieved := ""
	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		retrievedTasks, err := c.GetTasks(context.Background(), &proto.GetTasksRequest{
			MaxN: 1,
		})
		if err != nil {
			log.Fatalf("It should block and not fail, but it does fail: %v", err)
		}
		keyRetrieved = retrievedTasks.Tasks[0].Key
		wg.Done()
	}()

	time.Sleep(1 * time.Second)

	tasks := []*proto.NewTask{
		{
			Content:        []byte("hello_antri_server"),
			SecondsFromNow: 0,
		},
	}
	_, err = c.AddTasks(context.Background(),
		&proto.AddTasksRequest{
			Tasks: tasks,
		})
	if err != nil {
		log.Fatalf("AddTasks should succeed, but it does not, with error: %v", err)
	}

	wg.Wait()
	if keyRetrieved == "" {
		t.Fatal("keyRetrieved should have changed to non-empty string")
	}

	as.Close()
	time.Sleep(1 * time.Second) // give time for the system to shutdown
}
