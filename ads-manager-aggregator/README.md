# ADS Manager Aggregator

The **ADS Manager AGGREGATOR** aggregates all the data collected by the
**ADS Manager Kinesis Consumer** and generate some basic metrics.
 The results are stored on a single file on S3.

# REQUIREMENTS
 + Run **ads-manager/bashscripts/install.sh**

# STEPS
 1. Edit the file src/main/resources/run_ads_manager_aggregator.sh.
   ..* Insert your **AWS_ACCESS_KEY_ID** and **AWS_SECRET_KEY**.
   ..* (Optional) Change **AGGREGATOR_PATH** to the ads-manager-aggregator folder.

 2. Edit the file src/main/resources/emr.properties to configure the correct properties.

 3. Run th file src/main/resources/run_ads_manager_aggregator.sh
        sh src/main/resources/run_ads_manager_aggregator.sh

 4. The aggregation results will be


# NOTES
   This script should only run if there is data available.
   If the script is processed twice, it will try to create the same file on S3, and the job will fail.
   The script aggregates all the data, but could be on a cron job and aggregate the data
   with a short interval.


