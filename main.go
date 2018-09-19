package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flowerinthenight/sqs-lengthy-consume/longsubsqs"
)

func processCallback(v []byte) error {
	defer func(begin time.Time) {
		log.Printf("fn=processCallback, duration=%v", time.Since(begin))
	}(time.Now())

	log.Printf("raw=%v", string(v))

	return nil
}

func run(quit, done chan error) {
	var err error

	subquit := make(chan error)
	subdone := make(chan error)
	queue := "testlengthysqs"

	// Start listening to queue message.
	go func() {
		ls := longsubsqs.NewSqsLengthySubscriber(queue, processCallback)
		err = ls.Start(subquit, subdone)
		if err != nil {
			log.Fatalf("start long processing for %v failed, err=%v", queue, err)
		}
	}()

	subquit <- <-quit
	done <- <-subdone
}

func main() {
	quit := make(chan error)
	done := make(chan error)

	go run(quit, done)

	// Interrupt handler.
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		quit <- fmt.Errorf("%s", <-c)
	}()

	err := <-done
	if err != nil {
		log.Fatal(err)
	}
}
