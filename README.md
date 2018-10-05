## Overview

This is an example of consuming SQS messages in AWS with the potential of having a very long processing time for each message. It uses the [long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html) method of retrieving messages. Upon message receipt, it will spin up a "visibility timeout extender" goroutine that will keep extending the message's visibility timeout while being processed.
