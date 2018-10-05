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
	log.Printf("simulate lengthy work (1min)")
	time.Sleep(time.Second * 60)

	return nil
}

func main() {
	quit := make(chan error)
	done := make(chan error)
	queue := "testlengthyprocessing"

	go func() {
		ls := longsubsqs.NewSqsLengthySubscriber(queue, processCallback)
		ls.Start(quit, done) // error is propagated to 'done' channel.
	}()

	// Interrupt handler.
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		quit <- fmt.Errorf("%s", <-c)
	}()

	err := <-done
	if err != nil {
		log.Println(err)
	}
}
