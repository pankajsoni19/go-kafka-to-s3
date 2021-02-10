# go-kafka-to-s3

go-kafka-to-s3 is a kafka consumer written in Go. Reads messages from topics, gathers them in batches (files) of a pre-defined size and stores them in S3. It's useful if you want to have your data in a persistent storage for later process.

## Getting Started

These instructions will get you a copy of the project up and running.

### Installing

A step by step series of examples that tell you how to get a development env running

```
git clone https://github.com/pankajsoni19/go-kafka-to-s3
cd go-kafka-to-s3
GOOS=linux GOARCH=amd64 go build

cp def.config.json config.json

./go-kafka-to-s3
```

Install As Systemd
```
cp kafka2s3.service /etc/systemd/system/kafka2s3.service
systemctl start kafka2s3.service
systemctl enable kafka2s3.service
```

Config file locations

```
* /opt/go-kafka-to-s3
* /etc/go-kafka-to-s3
* $HOME/go-kafka-to-s3
```

## Authors

* **Alexandros Panagiotou** - *[apanagiotou.com](https://apanagiotou.com)* / Twitter: *[@alexapanagiotou](https://twitter.com/alexapanagiotou)*
