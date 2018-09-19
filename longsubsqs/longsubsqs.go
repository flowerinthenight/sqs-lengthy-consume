package longsubsqs

import (
	"log"
	"os"
	"strconv"
	"time"

	gzawscfg "github.com/NYTimes/gizmo/config/aws"
	awspubsub "github.com/NYTimes/gizmo/pubsub/aws"
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

type withAcctId string

func (w withAcctId) Apply(o *SqsLengthySubscriber) {
	o.acctId = string(w)
}

func WithAcctId(v string) SqsLengthySubscriberOption {
	return withAcctId(v)
}

type withBase64 bool

func (w withBase64) Apply(o *SqsLengthySubscriber) {
	o.base64 = bool(w)
}

func WithBase64(v bool) SqsLengthySubscriberOption {
	return withBase64(v)
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
	acctId          string
	base64          bool
	timeout         int64
	callback        SqsMessageCallback
}

func (l *SqsLengthySubscriber) Start(quit, done chan error) error {
	if l.
	// Get the queue's visibility timeout so we can do a visibility extender goroutine.
	var visibilityTm int = -1
	var err error

	for once := 1; once > 0; once-- {
		var vistm string = "VisibilityTimeout"

		sess, err := session.NewSession()
		if err != nil {
			log.Printf("new aws session failed, err=%v", err)
			break
		}

		svc := sqs.New(sess, &aws.Config{
			Region: aws.String(l.region),
		})

		urlResp, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName:              &l.queue,
			QueueOwnerAWSAccountId: &l.acctId,
		})

		if err != nil {
			log.Printf("get queue url failed, err=%v", err)
			break
		}

		attrOut, err := svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
			AttributeNames: []*string{&vistm},
			QueueUrl:       urlResp.QueueUrl,
		})

		if err != nil {
			log.Printf("get queue url failed, err=%v", err)
			break
		}

		val, err := strconv.Atoi(*attrOut.Attributes[vistm])
		if err != nil {
			log.Printf("atoi failed, err=%v", err)
			break
		}

		visibilityTm = val
	}

	if visibilityTm < 0 {
		log.Printf("visibility timeout get failed, err=%v", err)
		return err
	}

	sub, err := awspubsub.NewSubscriber(awspubsub.SQSConfig{
		Config: gzawscfg.Config{
			Region:    l.region,
			AccessKey: l.accessKeyId,
			SecretKey: l.secretAccessKey,
		},
		QueueOwnerAccountID: l.acctId,
		QueueName:           l.queue,
		TimeoutSeconds:      &l.timeout, // long polling
		ConsumeBase64:       &l.base64,  // we want raw
	})

	if err != nil {
		log.Printf("create sns/sqs sub failed, err=%v", err)
		return err
	}

	finish := make(chan error)
	pipe := sub.Start()
	defer sub.Stop()

	log.Printf("start listen, subscription=%v", l.queue)

	go func() {
		for {
			select {
			case m := <-pipe:
				if m != nil {
					var ticker = time.NewTicker(time.Duration(visibilityTm/2) * time.Second)
					var end1 = make(chan error)
					var end2 = make(chan error)

					go func() {
						defer func() {
							log.Printf("ack extender done for %v", m)
							end2 <- nil
						}()

						extend := true

						for {
							select {
							case <-end1:
								return
							case <-ticker.C:
								if extend {
									log.Printf("modify ack deadline for %v to %v", m, visibilityTm)
									err = m.ExtendDoneDeadline(time.Duration(visibilityTm))
									if err != nil {
										log.Printf("extend deadline for %v failed, err=%v", m, err)
										extend = false
									}
								}
							}
						}
					}()

					_ = l.callback(m.Message())

					// Close our extender goroutine.
					end1 <- nil
					_ = <-end2
					ticker.Stop()

					// Remove message from queue.
					err = m.Done()
					if err != nil {
						log.Printf("remove message from sub failed, err=%v", err)
					}
				}
			case <-quit:
				log.Printf("request termination...")
				finish <- nil
				done <- nil
				return
			}
		}
	}()

	<-finish

	return nil
}

func NewSqsLengthySubscriber(queue string, callback SqsMessageCallback, o ...SqsLengthySubscriberOption) *SqsLengthySubscriber {
	s := &SqsLengthySubscriber{
		queue:           queue,
		region:          os.Getenv("AWS_REGION"),
		accessKeyId:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		acctId:          os.Getenv("AWS_WAVE_ACCT_ID"),
		base64:          false,
		timeout:         5,
		callback:        callback,
	}

	for _, opt := range o {
		opt.Apply(s)
	}

	return s
}
