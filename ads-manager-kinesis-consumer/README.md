# ADS Manager Kinesis Consumer

The **ADS Manager Kinesis Consumer** creates a Kinesis client to read data from a Kinesis stream
and send the data to S3.

## REQUIREMENTS
 + Run **ads-manager/prerequisites/install.sh**

## STEPS
 1. Edit the file src/main/resources/run_kinesis_consumer.sh.
   * (Optional) Change **CONSUMER_PATH** to the ads-manager-kinesis-consumer folder.

 2. Edit the file src/main/resources/consumer.properties to simulate a different number of events.

 3. Run file src/main/resources/run_kinesis_consumer.sh
        sh src/main/resources/run_kinesis_consumer.sh
