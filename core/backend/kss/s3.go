package kss

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/relabs-tech/backends/core/logger"
	"github.com/relabs-tech/backends/services/fitness/utils"
)

// S3 is the implementation of the KSSDriver for AWS S3
type S3 struct {
	config      aws.Config
	bucket      string
	baseKeyName string
}

// NewS3 returns a new S3
func NewS3(kssConfig S3Configuration) (*S3, error) {
	if kssConfig.AWSBucketName == "" {
		return nil, fmt.Errorf("AWSBucketName must not be empty")
	}

	config, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(kssConfig.AWSRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(kssConfig.AccessID, kssConfig.AccessKey, "")),
	)

	if err != nil {
		return nil, err
	}
	logger.Default().Debugln("KSS S3 enabled")
	s := S3{config, kssConfig.AWSBucketName, kssConfig.KeyPrefix}
	return &s, nil
}

// Delete deletes a the key file
func (s S3) Delete(key string) error {
	logger.Default().Infoln("Deleting ", s.baseKeyName+key)
	client := s3.NewFromConfig(s.config)

	input := &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    utils.StringPtr(s.baseKeyName + key),
	}

	_, err := client.DeleteObject(context.TODO(), input)
	if err != nil {
		logger.Default().Error("Could not delete ", s.baseKeyName+key)
		return err
	}
	logger.Default().Infoln("Deleted ", s.baseKeyName+key)

	return nil
}

// DeleteAllWithPrefix all keys starting with
func (s S3) DeleteAllWithPrefix(key string) error {
	logger.Default().Infoln("Deleting all ", s.baseKeyName+key)
	client := s3.NewFromConfig(s.config)

	keys, err := s.ListAllWithPrefix(key)
	if err != nil {
		return err
	}
	for _, key := range keys {
		input := &s3.DeleteObjectInput{
			Bucket: &s.bucket,
			Key:    utils.StringPtr(key),
		}
		logger.Default().Infoln("Deleting ", key)
		_, err := client.DeleteObject(context.TODO(), input)
		if err != nil {
			logger.Default().Error("Could not delete ", key)
			return err
		}
	}

	logger.Default().Infoln("Deleted all ", s.baseKeyName+key)

	return nil
}

// GetPreSignedURL returns a pre-signed URL that can be used with the given method until expiry time is passed
// key must be a valid file name
func (s S3) GetPreSignedURL(method Method, key string, expireIn time.Duration) (URL string, err error) {
	logger.Default().Infoln("GetPreSignedURL ", s.baseKeyName+key)

	client := s3.NewPresignClient(s3.NewFromConfig(s.config))

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

	return resp.URL, nil

}

// UploadData uploads data into a new key object
func (s S3) UploadData(key string, data []byte) error {
	cl := s3.NewFromConfig(s.config)

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

// ListAllWithPrefix Lists all keys with prefix
func (s S3) ListAllWithPrefix(key string) (keys []string, err error) {
	logger.Default().Infoln("Deleting all ", s.baseKeyName+key)
	client := s3.NewFromConfig(s.config)

	var continuationToken *string
	for {
		logger.Default().Infoln("Deleting all ", s.baseKeyName+key)
		input := &s3.ListObjectsV2Input{
			Bucket:            &s.bucket,
			Prefix:            utils.StringPtr(s.baseKeyName + key),
			ContinuationToken: continuationToken,
		}
		var resp *s3.ListObjectsV2Output
		resp, err = client.ListObjectsV2(context.TODO(), input)
		if err != nil {
			logger.Default().Error("Could not ListObjectsV2 from ", s.bucket)
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
	logger.Default().Infoln("Deleted all ", s.baseKeyName+key)

	return
}
