package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/logging"
	"github.com/phuslu/log"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//S3Client handle
var s3Client *s3.Client
var logWriter *log.FileWriter

func main() {
	config, err := loadConfig()

	if err != nil {
		panic(err)
	}

	setupLogger(config)
	setupAWS(config)

	topics := make([]string, len(config.Kafka.Topics))

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.Kafka.Bootstrap,
		"group.id":          config.Kafka.ConsumerGroup,
		"auto.offset.reset": "latest",
	})

	if err != nil {
		panic(err)
	}

	err = consumer.SubscribeTopics(topics, nil)

	if err != nil {
		consumer.Close()
		panic(err)
	}

	channels := make(map[string]chan string, len(config.Kafka.Topics))

	for _, topic := range config.Kafka.Topics {
		channels[topic] = setupRotation(consumer, config, topic)
	}

	for {
		// ReadMessage automatically commits offsets when using consumer groups.
		timeoutDuration := 5 * time.Second
		msg, err := consumer.ReadMessage(timeoutDuration)

		if err != nil {
			log.Error().Err(err).Msg("Consumer error..")
		} else {
			if channel, ok := channels[*msg.TopicPartition.Topic]; ok {
				channel <- string(msg.Value)
			} else {
				log.Error().Msgf("Ignoring kafka message: %s", string(msg.Value))
			}
		}
	}

	// kafkaConsumer.Close()
}

func setupLogger(config *Config) {
	logWriter := &log.FileWriter{
		Filename:   fmt.Sprintf("%s/%s.log", config.Logger.FilePath, config.Logger.Level),
		FileMode:   0600,
		MaxSize:    config.Logger.FileSize,
		MaxBackups: config.Logger.Backups,
		// Cleaner: func(filename string, maxBackups int, matches []os.FileInfo) {
		//      var dir = filepath.Dir(filename)
		//      var total int64
		//      for i := len(matches) - 1; i >= 0; i-- {
		//              total += matches[i].Size()
		//              if total > 10*1024*1024*1024 {
		//                      os.Remove(filepath.Join(dir, matches[i].Name()))
		//              }
		//      }
		// },
		EnsureFolder: true,
		LocalTime:    true,
		HostName:     true,
		ProcessID:    true,
	}

	log.DefaultLogger = log.Logger{
		Level:  log.ParseLevel(config.Logger.Level),
		Writer: logWriter,
	}

	//rotate every hour
	runner := cron.New(cron.WithSeconds(), cron.WithLocation(time.UTC))
	runner.AddFunc("0 0 * * * *", func() {
		logWriter.Rotate()
	})

	go runner.Run()
}

func setupAWS(appConfig *Config) {
	clientLogMode := aws.LogRetries | aws.LogRequest | aws.LogResponse

	s3Cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(appConfig.AWSS3.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(appConfig.AWSS3.AccessKey, appConfig.AWSS3.AccessSecret, "")),
		config.WithLogConfigurationWarnings(true),
		config.WithClientLogMode(clientLogMode),
		config.WithLogger(logging.NewStandardLogger(logWriter)))

	if err != nil {
		fmt.Println("unable to load AWS S3 config", err)
		panic((err))
	}

	s3Client = s3.NewFromConfig(s3Cfg)
}

func setupRotation(consumer *kafka.Consumer, config *Config, topic string) chan string {

	// Get the size in Mebabytes from the env var and convert in int64 bytes
	fileRotateSizeInBytes := int64(config.File.FileSizeInMB * 1024 * 1024)

	positionFile, err := fileWriter(topic, fileRotateSizeInBytes)

	if err != nil {
		panic(err)
	}

	// This channel is used to store kafka messages
	position := make(chan string, config.Kafka.BufferSize)

	// Writes kafka messages to the file
	go func() {
		log.Debug().Msgf("Write goroutine created for topic: %s", topic)

		for pos := range position {
			_, err := positionFile.Write(pos)
			if err != nil {
				log.Error().Err(err)
			}
		}
	}()

	// Rotate and upload the file to S3 if it has reached fileRotateSize
	go func() {
		log.Debug().Msg("Rotate/Upload goroutine created")

		for {
			time.Sleep(5)
			if rotate, err := positionFile.Rotateable(); rotate == true {
				log.Debug().Msg("The file is rotatable")

				if err != nil {
					log.Error().Err(err).Msg("file open")
				}

				rotatedFile, err := positionFile.Rotate()

				log.Debug().Msgf("File rotated: %s", rotatedFile)

				if err != nil {
					log.Error().Err(err).Msg("rotate")
				}

				go func() {
					log.Debug().Msg("Compress/Upload routine started")

					compressed, err := fileCompress(rotatedFile)
					log.Debug().Msgf("File compressed: %s", compressed)

					if err != nil {
						log.Error().Err(err).Msg("compression")
					}

					if err := upload(config, topic, compressed); err != nil {
						log.Error().Err(err).Msg("upload error")
					}

					log.Debug().Msgf("File uploaded: %s", compressed)

					if err = os.Remove(compressed); err != nil {
						log.Error().Err(err).Msg("error removing compressed file")
					}

					log.Debug().Msgf("Rotated file deleted: %s", compressed)
				}()
			}
		}
	}()

	return position
}

// // Close closes the kafka connection
// func (kc *kafka.Consumer) close() {
// 	kc.Close()
// }

func upload(config *Config, topic string, filename string) error {

	file, err := os.Open(filename)
	if err != nil {
		return errors.Wrap(err, "OS File Open")
	}
	defer file.Close()

	awsFileKey := filepath.Join(config.AWSS3.Path, topic, filename)

	log.Info().Msgf("Uploading %s to S3...", filename)

	input := &s3.PutObjectInput{
		Bucket: aws.String(config.AWSS3.Bucket),
		Key:    aws.String(awsFileKey),
		Body:   file,
		ACL:    types.ObjectCannedACLPrivate,
	}

	if _, err := s3Client.PutObject(context.TODO(), input); err != nil {
		log.Error().Err(err).Msg("S3 upload")
		return err
	}

	log.Info().Msgf("Successfully uploaded %s to %s", filename, awsFileKey)
	return nil
}
