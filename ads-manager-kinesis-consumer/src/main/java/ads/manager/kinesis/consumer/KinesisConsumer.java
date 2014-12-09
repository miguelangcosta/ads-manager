package ads.manager.kinesis.consumer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Properties;
import java.util.UUID;
import ads.manager.common.AWS;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;


/**
 *  Kinesis Consumer Application.
 */
public final class KinesisConsumer {

    //Default values if there is no properties file.
    private static final String DEFAULT_APP_NAME = "KinesisAdsManagerApplication";
    private static final String DEFAULT_STREAM_NAME = "AdsStream";
    private static final String DEFAULT_KINESIS_ENDPOINT = "kinesis.eu-west-1.amazonaws.com";
    private static final String DEFAULT_KINESIS_REGION_NAME = "eu-west-1";

    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final InitialPositionInStream DEFAULT_INITIAL_POSITION = InitialPositionInStream.TRIM_HORIZON;

    private static String applicationName = DEFAULT_APP_NAME;
    private static String streamName = DEFAULT_STREAM_NAME;
    private static String kinesisEndpoint = DEFAULT_KINESIS_ENDPOINT;
    private static String regionName = DEFAULT_KINESIS_REGION_NAME;
    private static InitialPositionInStream initialPositionInStream = DEFAULT_INITIAL_POSITION;

    private static KinesisClientLibConfiguration kinesisClientLibConfiguration;

    private static final Log LOG = LogFactory.getLog(KinesisConsumer.class);

    /**
     * Constructor
     */
    private KinesisConsumer() {
        super();
    }

    /**
     * @param args Property file with config overrides (e.g. application name, stream name)
     * @throws java.io.IOException Thrown if we can't read properties from the specified properties file
     */
    public static void main(String[] args) throws IOException {

        InputStream in;
        String propertiesFilePath = "/consumer.properties";
        if(args.length == 0){
            LOG.warn("Consumer properties not passed as argument, using default properties file");
            in = KinesisConsumer.class.getResourceAsStream(propertiesFilePath);
        }else{
            propertiesFilePath = args[0];
            in = new FileInputStream(new File(propertiesFilePath));
        }
        configure(in);

        LOG.info("Starting " + applicationName);
        LOG.info("Running " + applicationName + " to process stream " + streamName);

        IRecordProcessorFactory recordProcessorFactory = new KinesisConsumerRecordProcessorFactory(propertiesFilePath);
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    /**
     * Configure Kinesis client.
     * @param propertiesFile The properties file path.
     * @throws java.io.IOException Thrown when we run into issues reading properties
     * */
    private static void configure(InputStream propertiesFile) throws IOException {

        if (propertiesFile != null) {
            loadProperties(propertiesFile);
        }

        // ensure the JVM will refresh the cached IP values of ads.manager.common.AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        LOG.info("Using workerId: " + workerId);

        // Get credentials.
        AWSCredentialsProvider credentialsProvider = AWS.getAWSCredentialsProvider();

        LOG.info("Using credentials with access key id: " + credentialsProvider.getCredentials().getAWSAccessKeyId());

        kinesisClientLibConfiguration = new KinesisClientLibConfiguration(applicationName, streamName,
                credentialsProvider, workerId).withInitialPositionInStream(initialPositionInStream).withKinesisEndpoint(kinesisEndpoint).withRegionName(regionName);
    }

    /**
     * Load properties from file and override the default ones.
     * @param propertiesFile The properties file path.
     * @throws java.io.IOException Thrown when we run into issues reading properties
     */
    private static void loadProperties(InputStream propertiesFile) throws IOException {

        Properties properties = new Properties();
        try {
            properties.load(propertiesFile);
        } finally {
            propertiesFile.close();
        }

        String appNameOverride = properties.getProperty(ConfigKeys.APPLICATION_NAME_KEY);
        if (appNameOverride != null) {
            applicationName = appNameOverride;
        }
        LOG.info("Using application name " + applicationName);

        String streamNameOverride = properties.getProperty(ConfigKeys.STREAM_NAME_KEY);
        if (streamNameOverride != null) {
            streamName = streamNameOverride;
        }
        LOG.info("Using stream name " + streamName);

        String kinesisEndpointOverride = properties.getProperty(ConfigKeys.KINESIS_ENDPOINT_KEY);
        if (kinesisEndpointOverride != null) {
            kinesisEndpoint = kinesisEndpointOverride;
        }
        String initialPositionOverride = properties.getProperty(ConfigKeys.INITIAL_POSITION_IN_STREAM_KEY);
        if (initialPositionOverride != null) {
             initialPositionInStream = InitialPositionInStream.valueOf(initialPositionOverride);
        }
         LOG.info("Using initial position " + initialPositionInStream.toString() + " (if a checkpoint is not found).");

        String regionNameOverride = properties.getProperty(ConfigKeys.KINESIS_REGION_NAME);
        if (regionNameOverride != null) {
            regionName = regionNameOverride;
        }
        LOG.info("Using region name " + regionName);
    }

}
