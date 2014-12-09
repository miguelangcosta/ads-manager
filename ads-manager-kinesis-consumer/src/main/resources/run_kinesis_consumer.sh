#!/bin/sh

export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=

CONSUMER_PATH=~/ads-manager/ads-manager-kinesis-consumer
cd $CONSUMER_PATH
CONSUMER_PROPERTIES_FILE=$CONSUMER_PATH/src/main/resources/consumer.properties

#RUN KINESIS CONSUMER
java -cp target/ads-manager-kinesis-consumer-1.0-SNAPSHOT.jar:target/lib/* ads.manager.kinesis.consumer.KinesisConsumerDriver $CONSUMER_PROPERTIES_FILE


