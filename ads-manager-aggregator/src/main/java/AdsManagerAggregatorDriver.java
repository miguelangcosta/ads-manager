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

            /*String propertiesFile = "/emr.properties";
            InputStream in = AdsManagerAggregatorDriver.class.getResourceAsStream(propertiesFile);
            */

            Properties properties = new Properties();
            try {
                properties.load(in);
            }
            finally {
                in.close();
            }

            S3 s3 = new S3();
            URL resource = AdsManagerAggregatorDriver.class.getResource("pig_scripts/banner_apps_aggregations.pig");

        try {
            //Upload pig script to ads.manager.common.S3
            s3.uploadFile(properties.getProperty("s3.bucket"),
                          properties.getProperty("s3.pigScriptPath"),
                    new File(resource.toURI()).getAbsolutePath());
        } catch (URISyntaxException e) {
           LOG.error(e.getMessage());
            System.exit(-1);
        }
        //Create EMR cluster and run PIG script
        EMR emr = new EMR();
        emr.runPigAdsAggregator(properties);
    }
}
