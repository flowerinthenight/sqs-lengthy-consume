## Overview
This is an example of consuming SQS messages in AWS with the potential of having a very long processing time for each message. It uses the [long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html) method of retrieving messages. Upon message receipt, it will spin up a "visibility timeout extender" goroutine that will keep extending the message's visibility timeout while being processed.

## How to run
```bash
$ export AWS_REGION=<some-region>
$ export AWS_ACCESS_KEY_ID=<some-value>
$ export AWS_SECRET_ACCESS_KEY=<some-value>

# build the sample
$ go build -v
```

Using AWS console, create an SQS queue with `testlengthyprocessing` as name. The default value for "Default Visibility Timeout" should be 30s. Then run the sample, preferably two instances to confirm that only one running instance will process the message even past the queue's visibility timeout.

```bash
# run the sample
$ ./sqs-lengthy-consume
2018/10/05 11:59:39 start listen, queue=testlengthyprocessing, visibility=30s
```

Finally, send a test message using AWS console.
