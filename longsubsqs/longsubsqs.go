package longsubsqs

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SqsMessageCallback func(data []byte) error

type SqsLengthySubscriberOption interface {
	Apply(*SqsLengthySubscriber)
}

type withRegion string

func (w withRegion) Apply(o *SqsLengthySubscriber) {
	o.region = string(w)
}

func WithRegion(v string) SqsLengthySubscriberOption {
	return withRegion(v)
}

type withAccessKeyId string

func (w withAccessKeyId) Apply(o *SqsLengthySubscriber) {
	o.accessKeyId = string(w)
}

func WithAccessKeyId(v string) SqsLengthySubscriberOption {
	return withAccessKeyId(v)
}

type withSecretAccessKey string

func (w withSecretAccessKey) Apply(o *SqsLengthySubscriber) {
	o.secretAccessKey = string(w)
}

func WithSecretAccessKey(v string) SqsLengthySubscriberOption {
	return withSecretAccessKey(v)
}

type withTimeout int64

func (w withTimeout) Apply(o *SqsLengthySubscriber) {
	o.timeout = int64(w)
}

func WithTimeout(v int64) SqsLengthySubscriberOption {
	return withTimeout(v)
}

type SqsLengthySubscriber struct {
	queue           string
	region          string
	accessKeyId     string
	secretAccessKey string
	timeout         int64
	callback        SqsMessageCallback
}

func (l *SqsLengthySubscriber) Start(quit, done chan error) error {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(l.region),
	})

	if l.timeout < 3 {
		err := fmt.Errorf("timeout should be >= 3s")
		log.Println(err)
		return err
	}

	var timeout int64 = l.timeout

	queueName := l.queue
	vistm := "VisibilityTimeout"

	svc := sqs.New(sess)
	resultURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		log.Printf("get queue url failed, err=%v", err)
		return err
	}

	attrOut, err := svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{&vistm},
		QueueUrl:       resultURL.QueueUrl,
	})

	if err != nil {
		log.Printf("get queue attr failed, err=%v", err)
		return err
	}

	vis, err := strconv.Atoi(*attrOut.Attributes[vistm])
	if err != nil {
		log.Printf("visibility conv failed, err=%v", err)
		return err
	}

	var wg sync.WaitGroup
	var term int32
	wg.Add(1)

	// Monitor if we are requested to quit. Note that ReceiveMessage will block based on timeout
	// value so we could still be waiting for some time before we can actually quit gracefully.
	go func() {
		defer wg.Done()
		<-quit
		atomic.StoreInt32(&term, 1)
	}()

	log.Printf("start listen, queue=%v, visibility=%vs", queueName, vis)

	for {
		// Should we terminate via quit?
		if atomic.LoadInt32(&term) > 0 {
			log.Printf("request termination")
			break
		}

		result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl: resultURL.QueueUrl,
			AttributeNames: aws.StringSlice([]string{
				"SentTimestamp",
			}),
			MaxNumberOfMessages: aws.Int64(1),
			MessageAttributeNames: aws.StringSlice([]string{
				"All",
			}),
			// If running in k8s, don't forget that default grace period for shutdown is 30s.
			// If you set 'WaitTimeSeconds' to more than that, this service will be
			// SIGKILL'ed everytime there is an update.
			WaitTimeSeconds: aws.Int64(timeout),
		})

		if err != nil {
			log.Printf("get queue url failed, err=%v", err)
			continue
		}

		if len(result.Messages) == 0 {
			continue
		}

		var extend = (vis / 3) * 2
		var ticker = time.NewTicker(time.Duration(extend) * time.Second)
		var end = make(chan error)
		var w sync.WaitGroup

		w.Add(1)

		go func(queueUrl, receiptHandle string) {
			defer func() {
				log.Printf("visibility timeout extender done for [%v]", receiptHandle)
				w.Done()
			}()

			extend := true

			for {
				select {
				case <-end:
					return
				case <-ticker.C:
					if extend {
						_, err := svc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
							ReceiptHandle:     aws.String(receiptHandle),
							QueueUrl:          aws.String(queueUrl),
							VisibilityTimeout: aws.Int64(int64(vis)),
						})

						if err != nil {
							log.Printf("extend visibility timeout for [%v] failed, err=%v", receiptHandle, err)
							extend = false
							continue
						}

						log.Printf("visibility timeout for [%v] updated to %v", receiptHandle, vis)
					}
				}
			}
		}(*resultURL.QueueUrl, *result.Messages[0].ReceiptHandle)

		// Call message processing callback.
		_ = l.callback([]byte(*result.Messages[0].Body))

		_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      resultURL.QueueUrl,
			ReceiptHandle: result.Messages[0].ReceiptHandle,
		})

		if err != nil {
			log.Printf("delete message failed, err=%v", err)
		}

		// Terminate our extender goroutine and wait.
		end <- nil
		ticker.Stop()
		w.Wait()
	}

	wg.Wait()
	done <- nil

	return nil
}

func NewSqsLengthySubscriber(queue string, callback SqsMessageCallback, o ...SqsLengthySubscriberOption) *SqsLengthySubscriber {
	s := &SqsLengthySubscriber{
		queue:           queue,
		region:          os.Getenv("AWS_REGION"),
		accessKeyId:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		timeout:         5,
		callback:        callback,
	}

	for _, opt := range o {
		opt.Apply(s)
	}

	return s
}
