package kss

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/relabs-tech/kurbisio/core/logger"
	"github.com/relabs-tech/kurbisio/core/pointers"
	"github.com/sirupsen/logrus"
)

// S3 is the implementation of the KSSDriver for AWS S3
type S3 struct {
	config               aws.Config
	bucket               string
	baseKeyName          string
	callback             FileUpdatedCallBack
	sqsQueueName         string
	listenToSQS          chan bool
	stopListeningAt      time.Time
	stopListeningAtMutex sync.Mutex
	logger               *logrus.Entry
	usePathStyle         bool
	endpointURL          string
}

// newS3Client returns an S3 client configured with the instance's endpoint and path-style settings.
func (s *S3) newS3Client() *s3.Client {
	return s3.NewFromConfig(s.config, func(o *s3.Options) {
		o.UsePathStyle = s.usePathStyle
		if s.endpointURL != "" {
			o.BaseEndpoint = aws.String(s.endpointURL)
		}
	})
}

// newSQSClient returns an SQS client configured with the instance's endpoint settings.
func (s *S3) newSQSClient() *sqs.Client {
	return sqs.NewFromConfig(s.config, func(o *sqs.Options) {
		if s.endpointURL != "" {
			o.BaseEndpoint = aws.String(s.endpointURL)
		}
	})
}

// NewS3 returns a new S3
func NewS3(kssConfig S3Configuration) (*S3, error) {
	if kssConfig.AWSBucketName == "" {
		return nil, fmt.Errorf("AWSBucketName must not be empty")
	}

	options := []func(*config.LoadOptions) error{config.WithRegion(kssConfig.AWSRegion)}
	if kssConfig.AccessID != "" {
		options = append(options, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(kssConfig.AccessID, kssConfig.AccessKey, "")))
	}
	config, err := config.LoadDefaultConfig(
		context.TODO(),
		options...,
	)

	if err != nil {
		return nil, err
	}

	matched, _ := regexp.Match(`^[a-zA-Z0-9!\-_.*'()/]*$`, []byte(kssConfig.KeyPrefix))
	if !matched {
		return nil, fmt.Errorf("only a-zA-Z0-9!-_.*'()/* characters are allowed in the key prefix '%s', %w", kssConfig.KeyPrefix, err)
	}

	rlog := logger.FromContext(context.TODO())
	rlog.Infoln("KSS S3 enabled with basekey ", kssConfig.KeyPrefix)
	s := S3{
		config:               config,
		bucket:               kssConfig.AWSBucketName,
		baseKeyName:          kssConfig.KeyPrefix,
		sqsQueueName:         kssConfig.SQSNotificationQueue,
		listenToSQS:          make(chan bool),
		stopListeningAtMutex: sync.Mutex{},
		logger:               rlog,
		usePathStyle:         kssConfig.UsePathStyle,
		endpointURL:          kssConfig.EndpointURL,
	}
	if kssConfig.AutoCreateBucket {
		if err := s.ensureBucketAndQueueExist(); err != nil {
			return nil, err
		}
	}
	if s.sqsQueueName != "" {
		s.listenSQS()
	}
	return &s, nil
}

// ensureBucketAndQueueExist creates the S3 bucket and SQS queue if they do not already exist,
// then wires S3→SQS bucket notifications and the queue policy that allows S3 to publish.
// This mirrors the CloudFormation NotificationConfiguration + QueuePolicy resources.
// Useful for local development with LocalStack or MinIO.
func (s *S3) ensureBucketAndQueueExist() error {
	cl := s.newS3Client()

	_, err := cl.HeadBucket(context.TODO(), &s3.HeadBucketInput{Bucket: aws.String(s.bucket)})
	if err != nil {
		s.logger.Infof("Bucket %s not found, creating it", s.bucket)
		input := &s3.CreateBucketInput{Bucket: aws.String(s.bucket)}
		// LocationConstraint is required for all regions except us-east-1
		if region := s.config.Region; region != "" && region != "us-east-1" {
			input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
				LocationConstraint: s3types.BucketLocationConstraint(region),
			}
		}
		if _, err := cl.CreateBucket(context.TODO(), input); err != nil {
			return fmt.Errorf("failed to create bucket %s: %w", s.bucket, err)
		}
		s.logger.Infof("Created bucket %s", s.bucket)
	} else {
		s.logger.Infof("Bucket %s already exists", s.bucket)
	}

	if s.sqsQueueName == "" {
		return nil
	}

	sqsCl := s.newSQSClient()

	// Ensure the queue exists and get its URL.
	var queueURL string
	urlResult, err := sqsCl.GetQueueUrl(context.TODO(), &sqs.GetQueueUrlInput{QueueName: &s.sqsQueueName})
	if err != nil {
		s.logger.Infof("SQS queue %s not found, creating it", s.sqsQueueName)
		createResult, err := sqsCl.CreateQueue(context.TODO(), &sqs.CreateQueueInput{QueueName: &s.sqsQueueName})
		if err != nil {
			return fmt.Errorf("failed to create SQS queue %s: %w", s.sqsQueueName, err)
		}
		queueURL = *createResult.QueueUrl
		s.logger.Infof("Created SQS queue %s", s.sqsQueueName)
	} else {
		queueURL = *urlResult.QueueUrl
		s.logger.Infof("SQS queue %s already exists", s.sqsQueueName)
	}

	// Get the queue ARN — needed for both the queue policy and the bucket notification config.
	attrResult, err := sqsCl.GetQueueAttributes(context.TODO(), &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return fmt.Errorf("failed to get queue ARN for %s: %w", s.sqsQueueName, err)
	}
	queueARN := attrResult.Attributes[string(types.QueueAttributeNameQueueArn)]

	// Set the SQS queue policy to allow S3 to send messages.
	// Equivalent to the CloudFormation QueuePolicy resource.
	bucketARN := "arn:aws:s3:::" + s.bucket
	policy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": { "Service": "s3.amazonaws.com" },
			"Action": "sqs:SendMessage",
			"Resource": %q,
			"Condition": { "ArnLike": { "aws:SourceArn": %q } }
		}]
	}`, queueARN, bucketARN)

	_, err = sqsCl.SetQueueAttributes(context.TODO(), &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		Attributes: map[string]string{
			string(types.QueueAttributeNamePolicy): policy,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to set queue policy for %s: %w", s.sqsQueueName, err)
	}
	s.logger.Infof("Set queue policy for %s", s.sqsQueueName)

	// Wire S3→SQS bucket notifications.
	// Equivalent to the CloudFormation NotificationConfiguration / QueueConfigurations block.
	_, err = cl.PutBucketNotificationConfiguration(context.TODO(), &s3.PutBucketNotificationConfigurationInput{
		Bucket: aws.String(s.bucket),
		NotificationConfiguration: &s3types.NotificationConfiguration{
			QueueConfigurations: []s3types.QueueConfiguration{
				{
					Events:   []s3types.Event{s3types.EventS3ObjectCreated},
					QueueArn: aws.String(queueARN),
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to set bucket notification configuration for %s: %w", s.bucket, err)
	}
	s.logger.Infof("Configured S3→SQS notifications: bucket %s → queue %s", s.bucket, s.sqsQueueName)

	return nil
}

// WithCallBack Replaces the current callback with WithCallBack
func (s *S3) WithCallBack(callback FileUpdatedCallBack) {
	s.callback = callback
}

// Delete deletes a the key file
func (s *S3) Delete(key string) error {
	s.logger.Infoln("Deleting ", s.baseKeyName+key)
	client := s.newS3Client()

	input := &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    pointers.StringPtr(s.baseKeyName + key),
	}

	_, err := client.DeleteObject(context.TODO(), input)
	if err != nil {
		s.logger.Error("Could not delete ", s.baseKeyName+key)
		return err
	}
	s.logger.Infoln("Deleted ", s.baseKeyName+key)

	return nil
}

// DeleteAllWithPrefix all keys starting with
func (s *S3) DeleteAllWithPrefix(key string) error {
	s.logger.Infoln("Deleting all ", s.baseKeyName+key)
	client := s.newS3Client()

	keys, err := s.ListAllWithPrefix(key)
	if err != nil {
		return err
	}
	for _, key := range keys {
		input := &s3.DeleteObjectInput{
			Bucket: &s.bucket,
			Key:    pointers.StringPtr(key),
		}
		s.logger.Infoln("Deleting ", key)
		_, err := client.DeleteObject(context.TODO(), input)
		if err != nil {
			s.logger.Error("Could not delete ", key)
			return err
		}
	}

	logger.Default().Infoln("Deleted all ", s.baseKeyName+key)

	return nil
}

// GetPreSignedURL returns a pre-signed URL that can be used with the given method until expiry time is passed
// key must be a valid file name
func (s *S3) GetPreSignedURL(method Method, key string, expireIn time.Duration) (URL string, err error) {
	logger.Default().Infoln("GetPreSignedURL ", s.baseKeyName+key)

	client := s3.NewPresignClient(s.newS3Client())

	var resp *v4.PresignedHTTPRequest
	switch method {
	case Get:
		resp, err = client.PresignGetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(s.baseKeyName + key),
		}, s3.WithPresignExpires(expireIn))
	case Put:
		resp, err = client.PresignPutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(s.baseKeyName + key),
		}, s3.WithPresignExpires(expireIn))
	default:
		err = fmt.Errorf("%s unsupported method to presign '%s'", method, s.baseKeyName+key)
	}
	if err != nil {
		return "", err
	}

	if s.sqsQueueName != "" {
		s.stopListeningAtMutex.Lock()
		if time.Now().Add(expireIn).After(s.stopListeningAt) {
			s.stopListeningAt = time.Now().Add(expireIn)
		}
		s.stopListeningAtMutex.Unlock()
		select {
		case s.listenToSQS <- true:
		default:
		}
	}
	return resp.URL, nil

}

// UploadData uploads data into a new key object
func (s *S3) UploadData(key string, data []byte) error {
	cl := s.newS3Client()

	_, err := cl.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.baseKeyName + key),
		Body:   bytes.NewReader(data),
	})
	// Upload the file to S3.
	if err != nil {
		return fmt.Errorf("failed to upload file, %v", err)
	}
	return err
}

// DownloadData downloads data from key object
func (s *S3) DownloadData(key string) ([]byte, error) {
	cl := s.newS3Client()

	downloader := manager.NewDownloader(cl)

	w := manager.WriteAtBuffer{}
	_, err := downloader.Download(context.TODO(), &w,
		&s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(s.baseKeyName + key),
		})
	if err != nil {
		return w.Bytes(), fmt.Errorf("failed to download file, %v", err)
	}

	return w.Bytes(), nil
}

// ListAllWithPrefix Lists all keys with prefix
func (s *S3) ListAllWithPrefix(key string) (keys []string, err error) {
	s.logger.Infoln("ListAllWithPrefix all ", s.baseKeyName+key)
	client := s.newS3Client()

	var continuationToken *string
	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            &s.bucket,
			Prefix:            pointers.StringPtr(s.baseKeyName + key),
			ContinuationToken: continuationToken,
		}
		var resp *s3.ListObjectsV2Output
		resp, err = client.ListObjectsV2(context.TODO(), input)
		if err != nil {
			s.logger.Errorf("Could not ListObjectsV2 from %s for key %s", s.bucket, key)
			return
		}
		for _, item := range resp.Contents {
			keys = append(keys, *item.Key)
		}
		continuationToken = resp.NextContinuationToken
		if resp.NextContinuationToken == nil {
			break
		}
	}
	s.logger.Infoln("Deleted all ", s.baseKeyName+key)

	return
}

func (s *S3) listenSQS() {
	s.logger.Infof("Listening to SQS queue %s\n", s.sqsQueueName)
	client := s.newSQSClient()

	// Get URL of queue
	urlResult, err := client.GetQueueUrl(
		context.TODO(),
		&sqs.GetQueueUrlInput{QueueName: &s.sqsQueueName},
	)

	if err != nil {
		s.logger.WithError(err).Error("Could not GetQueueUrl for queue ", s.sqsQueueName)
		return
	}
	go func() {
		for range s.listenToSQS {
			for {

				gMInput := &sqs.ReceiveMessageInput{
					MessageAttributeNames: []string{
						string(types.QueueAttributeNameAll),
					},
					QueueUrl:            urlResult.QueueUrl,
					MaxNumberOfMessages: 10,
					WaitTimeSeconds:     10,
				}

				s.logger.Infoln("Calling ReceiveMessage")
				msgResult, err := client.ReceiveMessage(context.TODO(), gMInput)
				if err != nil {
					s.logger.WithError(err).Errorln("Got an error receiving messages:")
					continue
				}
				s.logger.Infof("Got %d SQS messages\n", len(msgResult.Messages))

				var messages []events.SQSMessage

				// ReceiveMessage gives back types.Message while we want vents.SQSMessage
				for _, m := range msgResult.Messages {
					messages = append(messages, events.SQSMessage{Body: pointers.SafeString(m.Body)})
				}

				s.ProcessIncomingSQSMessageRecords(messages)
				for _, m := range msgResult.Messages {
					if _, err = client.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
						QueueUrl:      urlResult.QueueUrl,
						ReceiptHandle: m.ReceiptHandle,
					}); err != nil {
						s.logger.WithError(err).Error("Could not delete message ", *m.Body)
					}
				}

				s.stopListeningAtMutex.Lock()
				if time.Now().After(s.stopListeningAt) {
					break
				}
				s.stopListeningAtMutex.Unlock()
			}
		}
	}()
}

// ProcessIncomingSQSMessageRecords processes messages coming from the SQS queue connected to the
// S3 bucket that is used by thos S3 instance
func (s *S3) ProcessIncomingSQSMessageRecords(messages []events.SQSMessage) {
	for _, m := range messages {
		if m.Body == "" {
			s.logger.Error("Empty body ")
			continue
		}
		var msg struct {
			Records []events.S3EventRecord `json:"Records"`
		}

		err := json.Unmarshal([]byte(m.Body), &msg)
		if err != nil {
			s.logger.WithError(err).Error("Could not unmarshal ", m.Body)
			continue
		}

		for _, e := range msg.Records {
			if e.EventName != "ObjectCreated:Put" {
				s.logger.Infoln("Got unexpected event name" + e.EventName)
				continue
			}
			if strings.Contains(e.S3.Object.Key, s.baseKeyName) && s.callback != nil {
				s.logger.Infoln("Got Uploaded key " + e.S3.Object.Key)

				if err := s.callback(FileUpdatedEvent{
					Etags: e.S3.Object.ETag,
					Key:   strings.TrimPrefix(e.S3.Object.Key, s.baseKeyName),
					Size:  e.S3.Object.Size,
					Type:  "uploaded",
				}); err != nil {
					s.logger.WithError(err).Errorf("Could not invoke callback %+v", e)
				}
			} else {
				s.logger.Errorln("Got wrong key " + e.S3.Object.Key)
			}
		}
	}
}
