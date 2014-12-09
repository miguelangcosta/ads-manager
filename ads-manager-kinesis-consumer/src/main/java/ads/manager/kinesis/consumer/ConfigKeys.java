package ads.manager.kinesis.consumer;

/**
 *  Keys for configuration overrides (via properties file).
 */
public class ConfigKeys {
    
    /**
     * Name of the application.
     */
    public static final String APPLICATION_NAME_KEY = "applicationName";

    /**
     * Name of the Kinesis stream.
     */
    public static final String STREAM_NAME_KEY = "streamName";

    /**
     * Kinesis endpoint.
     */
    public static final String KINESIS_ENDPOINT_KEY = "kinesisEndpoint";
    
    /**
     * Initial position in the stream when an application starts up for the first time.
     * Value is one of LATEST (most recent data) or TRIM_HORIZON (oldest available data).
     */
    public static final String INITIAL_POSITION_IN_STREAM_KEY = "initialPositionInStream";

    /**
     * Kinesis region name.
     */
    public static final String KINESIS_REGION_NAME = "regionName";

    private ConfigKeys() {        
    }

}
