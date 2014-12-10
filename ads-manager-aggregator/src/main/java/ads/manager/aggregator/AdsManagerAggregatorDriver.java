package ads.manager.aggregator;

import ads.manager.common.S3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;


public class AdsManagerAggregatorDriver {

    final static Logger LOG = LoggerFactory.getLogger(AdsManagerAggregatorDriver.class);

    public static void main(String[] args) throws IOException {

            String propertiesFile = args[0];

            InputStream in = new FileInputStream(new File(propertiesFile));

            Properties properties = new Properties();
            try {
                properties.load(in);
            }
            finally {
                in.close();
            }

            S3 s3 = new S3(properties.getProperty("aws.regionName"));

            String pigScriptPath = new File(new File(propertiesFile).getParentFile() + "/banner_apps_aggregations.pig").getAbsolutePath();

            //Upload pig script to ads.manager.common.S3
            s3.uploadFile(properties.getProperty("s3.bucket"),
                          properties.getProperty("s3.pigScriptPath"),
                    pigScriptPath);

        //Create ads.manager.aggregator.EMR cluster and run PIG script
        EMR emr = new EMR();
        emr.runPigAdsAggregator(properties);
    }
}
