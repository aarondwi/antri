package main

import (
	"math/rand"
	"time"
)

var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
var l = len(letterBytes)

func randStringBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(l)]
	}
	return b
}

func calculatePercentileInMsec(arr []float64, p int) float64 {
	return arr[int(float64(len(arr))*float64(p)/100.0)] / float64(time.Millisecond)
}
