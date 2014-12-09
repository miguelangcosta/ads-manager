#!/bin/sh

PRODUCER_PATH=~/ads-manager/ads-manager-kinesis-producer
cd $PRODUCER_PATH
PRODUCER_PROPERTIES_FILE=$PRODUCER_PATH/src/main/resources/producer.properties

#RUN KINESIS PRODUCER
java -cp target/ads-manager-kinesis-producer-1.0-SNAPSHOT.jar:target/lib/* ads.manager.kinesis.producer.KinesisProducerDriver $PRODUCER_PROPERTIES_FILE



