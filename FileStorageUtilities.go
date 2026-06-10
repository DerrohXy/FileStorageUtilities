package filestorageutilities

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"google.golang.org/api/option"
)

//lint:file-ignore ST1005 ...

// Defines the contract for any file storage backend.
type FileStorage interface {
	SaveFile(reader io.Reader) (string, error)
	UpdateFile(id string, reader io.Reader) error
	RetrieveFile(id string) (io.ReadCloser, error)
	DeleteFile(id string) error
}

// Stores files in a local directory.
type LocalFileStorage struct {
	BaseDir string
}

// Creates a new instance pointing to a given base directory.
func NewLocalFileStorage(baseDir string) *LocalFileStorage {
	return &LocalFileStorage{BaseDir: baseDir}
}

func fileExists_(filePath string) bool {
	_, err := os.Stat(filePath)

	return !errors.Is(err, os.ErrNotExist)
}

func (instance *LocalFileStorage) SaveFile(reader io.Reader) (string, error) {
	id := uuid.New().String()
	filePath := filepath.Join(instance.BaseDir, id)

	file, err := os.Create(filePath)
	if err != nil {
		return "", fmt.Errorf("Failed to create file: %w", err)
	}

	defer file.Close()

	if _, err := io.Copy(file, reader); err != nil {
		return "", fmt.Errorf("Failed to write file: %w", err)
	}

	return id, nil
}

func (instance *LocalFileStorage) UpdateFile(id string, reader io.Reader) error {
	filePath := filepath.Join(instance.BaseDir, id)
	if !fileExists_(filePath) {
		return fmt.Errorf("File not found for id: %s", id)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("Failed to open file for update: %w", err)
	}

	defer file.Close()

	if _, err := io.Copy(file, reader); err != nil {
		return fmt.Errorf("Failed to write updated file: %w", err)
	}

	return nil
}

func (instance *LocalFileStorage) RetrieveFile(id string) (io.ReadCloser, error) {
	filePath := filepath.Join(instance.BaseDir, id)
	if !fileExists_(filePath) {
		return nil, fmt.Errorf("File not found for id: %s", id)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (instance *LocalFileStorage) DeleteFile(id string) error {
	filePath := filepath.Join(instance.BaseDir, id)
	if !fileExists_(filePath) {
		return fmt.Errorf("File not found for id: %s", id)
	}

	return os.Remove(filePath)
}

// Stores files in an AWS S3 bucket.
type S3FileStorage struct {
	Client     *s3.Client
	BucketName string
}

// Holds configuration for AWS credentials and region.
type S3Config struct {
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
}

// Initializes an S3FileStorage using provided config.
func NewS3FileStorage(configuration S3Config) (*S3FileStorage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	creds := aws.NewCredentialsCache(
		credentials.NewStaticCredentialsProvider(configuration.AccessKey, configuration.SecretKey, ""),
	)
	awsCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(configuration.Region),
		config.WithCredentialsProvider(creds),
	)

	if err != nil {
		return nil, fmt.Errorf("Failed to load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg)

	return &S3FileStorage{
		Client:     client,
		BucketName: configuration.Bucket,
	}, nil
}

func (instance *S3FileStorage) SaveFile(reader io.Reader) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	id := uuid.New().String()
	key := id

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, reader); err != nil {
		return "", fmt.Errorf("Failed to read file data: %w", err)
	}

	_, err := instance.Client.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket: aws.String(instance.BucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader(buf.Bytes()),
		})

	if err != nil {
		return "", fmt.Errorf("Failed to upload file: %w", err)
	}

	return id, nil
}

func (instance *S3FileStorage) UpdateFile(id string, reader io.Reader) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	// id := uuid.New().String()
	key := id

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, reader); err != nil {
		return fmt.Errorf("Failed to read file data: %w", err)
	}

	_, err := instance.Client.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket: aws.String(instance.BucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader(buf.Bytes()),
		})

	if err != nil {
		return fmt.Errorf("Failed to upload file: %w", err)
	}

	return nil
}

func (instance *S3FileStorage) RetrieveFile(id string) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	key := id

	obj, err := instance.Client.GetObject(
		ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(instance.BucketName),
			Key:    aws.String(key),
		})

	if err != nil {
		return nil, fmt.Errorf("Failed to retrieve file: %w", err)
	}

	return obj.Body, nil
}

func (instance *S3FileStorage) DeleteFile(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	key := id

	_, err := instance.Client.DeleteObject(
		ctx,
		&s3.DeleteObjectInput{
			Bucket: aws.String(instance.BucketName),
			Key:    aws.String(key),
		})
	if err != nil {
		return fmt.Errorf("Failed to delete file: %w", err)
	}

	return nil
}

// Stores files in a Google Cloud Storage bucket.
type GCSFileStorage struct {
	Client     *storage.Client
	BucketName string
}

// Holds credentials
type GCSCredentials struct {
	Type                    string `json:"type"`
	ProjectID               string `json:"project_id"`
	PrivateKeyID            string `json:"private_key_id"`
	PrivateKey              string `json:"private_key"`
	ClientEmail             string `json:"client_email"`
	ClientID                string `json:"client_id"`
	AuthURI                 string `json:"auth_uri"`
	TokenURI                string `json:"token_uri"`
	AuthProviderX509CertURL string `json:"auth_provider_x509_cert_url"`
	ClientX509CertURL       string `json:"client_x509_cert_url"`
}

// Holds configuration for GCS.
type GCSConfig struct {
	Bucket      string
	Credentials GCSCredentials
}

// Initializes a GCSFileStorage using the given config.
func NewGCSFileStorage(cfg GCSConfig) (*GCSFileStorage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	var (
		client *storage.Client
		err    error
	)

	jsonData, err := json.Marshal(cfg.Credentials)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal credentials struct: %v", err)
	}

	client, err = storage.NewClient(
		ctx,
		option.WithCredentialsJSON(jsonData),
	)

	if err != nil {
		return nil, fmt.Errorf("Failed to create GCS client: %w", err)
	}

	return &GCSFileStorage{
		Client:     client,
		BucketName: cfg.Bucket,
	}, nil
}

func (instance *GCSFileStorage) SaveFile(reader io.Reader) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	id := uuid.New().String()
	key := id

	writer := instance.Client.Bucket(instance.BucketName).Object(key).NewWriter(ctx)

	if _, err := io.Copy(writer, reader); err != nil {
		return "", fmt.Errorf("Failed to upload to GCS: %w", err)
	}

	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("Failed to finalize upload: %w", err)
	}

	return id, nil
}

func (instance *GCSFileStorage) UpdateFile(id string, reader io.Reader) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	// id := uuid.New().String()
	key := id

	writer := instance.Client.Bucket(instance.BucketName).Object(key).NewWriter(ctx)

	if _, err := io.Copy(writer, reader); err != nil {
		return fmt.Errorf("Failed to upload to GCS: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("Failed to finalize upload: %w", err)
	}

	return nil
}

func (instance *GCSFileStorage) RetrieveFile(id string) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	it := instance.Client.Bucket(instance.BucketName).Objects(ctx, &storage.Query{
		Prefix: id,
	})
	objAttrs, err := it.Next()

	if err != nil {
		return nil, fmt.Errorf("File not found for id: %s", id)
	}

	reader, err := instance.Client.Bucket(instance.BucketName).Object(objAttrs.Name).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file: %w", err)
	}

	return reader, nil
}

func (instance *GCSFileStorage) DeleteFile(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	it := instance.Client.Bucket(instance.BucketName).Objects(ctx, &storage.Query{
		Prefix: id,
	})
	objAttrs, err := it.Next()

	if err != nil {
		return fmt.Errorf("file not found for id: %s", id)
	}

	return instance.Client.Bucket(instance.BucketName).Object(objAttrs.Name).Delete(ctx)
}

// Implements FileStorage for Azure Blob Storage.
type AzureBlobStorage struct {
	Client     *azblob.Client
	Container  string
	AccountURL string
}

// Holds Azure connection settings.
type AzureConfig struct {
	AccountName string
	AccountKey  string
	Container   string
}

// Initializes the Azure Blob client.
func NewAzureBlobStorage(configuration AzureConfig) (*AzureBlobStorage, error) {
	url := fmt.Sprintf("https://%s.blob.core.windows.net/", configuration.AccountName)

	cred, err := azblob.NewSharedKeyCredential(configuration.AccountName, configuration.AccountKey)
	if err != nil {
		return nil, fmt.Errorf("Invalid Azure credentials: %w", err)
	}

	client, err := azblob.NewClientWithSharedKeyCredential(url, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to create Azure Blob client: %w", err)
	}

	return &AzureBlobStorage{
		Client:     client,
		Container:  configuration.Container,
		AccountURL: url,
	}, nil
}

func (instance *AzureBlobStorage) SaveFile(reader io.Reader) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	id := uuid.New().String()
	blobName := id

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, reader); err != nil {
		return "", fmt.Errorf("Failed to read file: %w", err)
	}

	_, err := instance.Client.UploadBuffer(ctx, instance.Container, blobName, buf.Bytes(), nil)
	if err != nil {
		return "", fmt.Errorf("Failed to upload blob: %w", err)
	}

	return id, nil
}

func (instance *AzureBlobStorage) UpdateFile(id string, reader io.Reader) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	// id := uuid.New().String()
	blobName := id

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, reader); err != nil {
		return fmt.Errorf("Failed to read file: %w", err)
	}

	_, err := instance.Client.UploadBuffer(ctx, instance.Container, blobName, buf.Bytes(), nil)
	if err != nil {
		return fmt.Errorf("Failed to upload blob: %w", err)
	}

	return nil
}

func (instance *AzureBlobStorage) RetrieveFile(id string) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	prefix := id
	pager := instance.Client.NewListBlobsFlatPager(instance.Container, nil)

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("Failed to list blobs: %w", err)
		}

		for _, blob := range resp.Segment.BlobItems {
			if strings.HasPrefix(*blob.Name, prefix) {
				dl, err := instance.Client.DownloadStream(ctx, instance.Container, *blob.Name, nil)
				if err != nil {
					return nil, fmt.Errorf("Failed to download blob: %w", err)
				}

				return dl.Body, nil
			}
		}
	}

	return nil, fmt.Errorf("File not found for id: %s", id)
}

func (instance *AzureBlobStorage) DeleteFile(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	prefix := id
	pager := instance.Client.NewListBlobsFlatPager(instance.Container, nil)

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("Failed to list blobs: %w", err)
		}

		for _, blob := range resp.Segment.BlobItems {
			if strings.HasPrefix(*blob.Name, prefix) {
				_, err := instance.Client.DeleteBlob(ctx, instance.Container, *blob.Name, nil)

				return err
			}
		}
	}

	return fmt.Errorf("File not found for id: %s", id)
}
