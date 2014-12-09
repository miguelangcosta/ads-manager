package ads.manager.kinesis.producer;

import ads.manager.common.AdEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;


/**
 *  Kinesis Producer Driver Application.
 */
public class KinesisProducerDriver {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisProducerDriver.class);
    private static Random RANDOM = new Random(System.nanoTime());
    private final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws IOException {

        InputStream in;

        if(args.length == 0){
            in = KinesisProducerDriver.class.getResourceAsStream("/producer.properties");
        }else{
            in = new FileInputStream(new File(args[0]));
        }

        Properties properties = new Properties();
        try {
            properties.load(in);
        }
        finally {
            in.close();
        }

        String streamName = properties.getProperty("streamName");

        KinesisProducer kinesisProducer = new KinesisProducer();

        kinesisProducer.createKinesisClient(properties.getProperty("endpoint"),
                                            properties.getProperty("serviceName"),
                                            properties.getProperty("region"));
        //Create the Kinesis stream
        kinesisProducer.createStream(streamName, Integer.parseInt(properties.getProperty("shardCount")));

        KinesisProducerDriver driver = new KinesisProducerDriver();
        //Generate and send data to the stream
        driver.createAdsEvents(kinesisProducer, properties);
    }


    /**
     * Creates data to simulate an ads platform.
     * @param kinesisProducer the kinesisProducer client.
     * @param properties the properties parameters for the fake generated dat.
     */
    private void createAdsEvents(KinesisProducer kinesisProducer, Properties properties){
        //TODO: RUN THIS WITH MULTIPLE THREADS

        List<String> messages = new ArrayList();

        for(int i = 1; i <= Integer.parseInt(properties.getProperty("events")); i++){

            Date date = new Date();
            AdEvent adEvent = new AdEvent();
            adEvent.setAppId(RANDOM.nextInt(Integer.parseInt(properties.getProperty("apps"))));
            adEvent.setBannerId(RANDOM.nextInt(Integer.parseInt(properties.getProperty("banners"))));
            adEvent.setUserId(RANDOM.nextInt(Integer.parseInt(properties.getProperty("users"))));
            adEvent.setTimestamp(date.getTime());
            adEvent.setEventType(RANDOM.nextInt(Integer.parseInt(properties.getProperty("eventTypes"))));

            try {
                String message = JSON_MAPPER.writeValueAsString(adEvent);
                messages.add(message);

                LOG.debug(message);

                //500 because of the limit within the PutRequests
                //We are testing to send several messages at the same time
                if(i % 500 == 0){
                    kinesisProducer.putRecords(properties.getProperty("streamName"), messages);
                    LOG.info("Sent Events:{}", i);
                    messages.clear();
                }
            } catch (JsonProcessingException e) {
                LOG.error(e.getMessage());
                System.exit(-1);
            }
        }
    }
}
