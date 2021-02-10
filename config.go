package main

import (
	"github.com/spf13/viper"
)

// KafkaConfig holder
type KafkaConfig struct {
	Bootstrap     string   `mapstructure:"bootstrap"`
	Topics        []string `mapstructure:"topics"`
	ConsumerGroup string   `mapstructure:"consumer_group"`
	BufferSize    int      `mapstructure:"buffer_size"`
}

// AWSS3Config holder
type AWSS3Config struct {
	AccessKey    string `mapstructure:"accessKey"`
	AccessSecret string `mapstructure:"accessSecret"`
	Bucket       string `mapstructure:"bucket"`
	Region       string `mapstructure:"region"`
	Path         string `mapstructure:"path"`
}

// LogConfig holder
type LogConfig struct {
	Level    string `mapstructure:"level"`
	FilePath string `mapstructure:"filePath"`
	FileSize int64  `mapstructure:"fileSize"`
	Backups  int    `mapstructure:"backups"`
}

// FileConfig holder
type FileConfig struct {
	FileSizeInMB int `mapstructure:"size_mb"`
}

// Config is global
type Config struct {
	Kafka  KafkaConfig `mapstructure:"kafka"`
	AWSS3  AWSS3Config `mapstructure:"s3"`
	Logger LogConfig   `mapstructure:"log"`
	File   FileConfig  `mapstructure:"file"`
}

func loadConfig() (config *Config, err error) {
	viper.SetConfigName("config.json") // name of config file (without extension)
	viper.SetConfigType("json")        // REQUIRED if the config file does not have the extension in the name

	viper.AddConfigPath("/opt/go-kafka-to-s3/") // path to look for the config file in
	viper.AddConfigPath("/etc/go-kafka-to-s3/")
	viper.AddConfigPath("$HOME/go-kafka-to-s3/") // call multiple times to add many search paths
	viper.AddConfigPath("../")                   // optionally look for config in the working directory

	err = viper.ReadInConfig()

	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	return
}
