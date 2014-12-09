# ADS Manager Kinesis Producer

The **ADS Manager Kinesis Producer** creates fake ads data and send the data to a Kinesis stream.

## Requirements
 + Run **ads-manager/prerequisites/install.sh**

## STEPS
 1. Edit the file src/main/resources/run_kinesis_producer.sh.
   * (Optional) Change **PRODUCER_PATH** to the ads-manager-kinesis-producer folder.

 2. Edit the file src/main/resources/producer.properties to simulate a different number of events.

 3. Run file src/main/resources/run_kinesis_producer.sh
        sh src/main/resources/run_kinesis_producer.sh


