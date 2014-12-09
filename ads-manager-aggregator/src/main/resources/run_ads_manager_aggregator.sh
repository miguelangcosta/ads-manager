#!/bin/sh

AGGREGATOR_PATH=~/ads-manager/ads-manager-aggregator
cd $AGGREGATOR_PATH
AGGREGATOR_PROPERTIES_FILE=$AGGREGATOR_PATH/src/main/resources/emr.properties

#RUN PIG Script to Aggregate the data and send the aggregations to S3
java -cp target/ads-manager-aggregator-1.0-SNAPSHOT.jar:target/lib/* ads.manager.aggregator.AdsManagerAggregatorDriver $AGGREGATOR_PROPERTIES_FILE


