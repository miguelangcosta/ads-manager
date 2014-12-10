package ads.manager.kinesis.consumer;

import java.io.*;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import ads.manager.common.S3;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KinesisConsumerRecordProcessor implements IRecordProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisConsumerRecordProcessor.class);
    private String kinesisShardId;
    private String propertiesFilePath;
    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;
    
    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    private int processedRecords = 0;
    private String lastCreatedFile = null;
    private FileWriter writer = null;
    
    /**
     * Constructor.
     */
    public KinesisConsumerRecordProcessor() {
        super();
    }

    /**
     * Constructor
     * @param propertiesFilePath the path of the properties file
     */
    public KinesisConsumerRecordProcessor(String propertiesFilePath) {
        this.propertiesFilePath = propertiesFilePath;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Processing " + records.size() + " records from " + kinesisShardId);
        
        // Process records and perform all exception handling.
        processRecordsWithRetries(records);
        
        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     * The records are first written locally and then upload to S3.
     * @param records The records got from the Kinesis stream.
     */
    private void processRecordsWithRetries(List<Record> records) {

        for (Record record : records) {
            boolean processedSuccessfully = false;
            String data = null;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    // For this app, we interpret the payload as UTF-8 chars.
                    data = decoder.decode(record.getData()).toString();
                    LOG.info(record.getSequenceNumber() + ", " + record.getPartitionKey() + ", " + data);

                    //backup data first to local file and then upload to S3
                    writeToFile(record.getSequenceNumber(), data);

                    processedSuccessfully = true;
                    break;
                } catch (CharacterCodingException e) {
                    LOG.error("Malformed data: " + data, e);
                    break;
                } catch (Throwable t) {
                    LOG.warn("Caught throwable while processing record " + record, t);
                }
                
                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    LOG.debug("Interrupted sleep", e);
                }
            }
            processedRecords++;
            if (!processedSuccessfully) {
                processedRecords--;
                LOG.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }

        // TODO: This can be improved
        try{
            writer.flush();
            writer.close();
           }catch (IOException e){
                LOG.error(e.getMessage());
                System.exit(-1);
           }
        //upload last file to S3
        uploadToS3();
        processedRecords = 0;
        lastCreatedFile = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }

    /** Checkpoint with retries.
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
            }
        }
    }

    /**
     * Write to local file the data processed from the Kinesis client
     * and upload each to S3 after 5000 registries.
     * @param recordSequenceNumber First sequence number from the Kinesis stream.
     * @param data The data to process.
     * */
    private void writeToFile(String recordSequenceNumber, String data) {
        //This is implemented while amazon-kinesis-connectors does not put the project on a MAVEN repo
        //https://github.com/awslabs/amazon-kinesis-connectors/issues/23
        //TODO: Deal with repeated data within the files
        try {
            if(lastCreatedFile == null){

                // Update the name of the file to be written.
                updateLastCreatedFile(recordSequenceNumber);
                File file = new File(lastCreatedFile);
                File parentDirectory = file.getParentFile();

                if(!parentDirectory.exists()){
                    parentDirectory.mkdirs();
                }
                writer = new FileWriter(lastCreatedFile, true);
            }

            if (processedRecords % 5000 == 0 && processedRecords > 0) {
                writer.flush();
                writer.close();
                //Upload local file to S3
                uploadToS3();

                // Update the name of the file to be written.
                updateLastCreatedFile(recordSequenceNumber);
                writer = new FileWriter(lastCreatedFile, true);
            }

            writer.write(data + "\n");

        } catch (IOException e) {
            LOG.error("Error Processing records\n {}", e.getMessage());
            System.exit(-1);
        }
    }

    /**
     * Get the properties from the file
     * @throws java.io.IOException If there is a problem reading the properties file.
     * */
    private Properties getProperties() throws IOException{

        InputStream in = new FileInputStream(new File(this.propertiesFilePath));

        Properties properties = new Properties();
        try {
            properties.load(in);
        } finally {
            in.close();
        }

        return properties;
    }

    /**
     * Updates the name for the last created file.
     * @param filename The filename (sequenceNumber), the timestamp of the current date is added.
     * */
    private void updateLastCreatedFile(String filename) {

        Calendar cal = Calendar.getInstance();
        Properties properties = null;
        try{
              properties = getProperties();
        }catch (IOException e){
            LOG.error("Error reading Properties files: {}", e.getMessage());
            System.exit(-1);
        }

        lastCreatedFile = properties.getProperty("localDataFolder") +
                 cal.getTimeInMillis() + "_" +
                 filename + ".txt";
        LOG.info("Created new local file:{}", lastCreatedFile);
    }

    /**
     * Uploads the last processed file to S3 and deletes the file locally.
     * */
    private void uploadToS3(){
        Properties properties = null;
        try{
            properties = getProperties();
        }catch (IOException e){
            LOG.error(e.getMessage());
            System.exit(-1);
        }
        S3 s3 = new S3(properties.getProperty("regionName"));
        Calendar cal = Calendar.getInstance();
        //Uploads the file to S3 and creates a path to be more easy to process.
        s3.uploadFile(properties.getProperty("s3Bucket"),
                properties.getProperty("s3Key") +
                        cal.get(Calendar.YEAR)  + "/" +
                        cal.get(Calendar.MONTH) + "/" +
                        cal.get(Calendar.DATE)  + "/" +
                        cal.get(Calendar.HOUR)  + "/" +
                        lastCreatedFile.substring(lastCreatedFile.lastIndexOf('/') + 1),
                lastCreatedFile);

        //Delete local file after upload to S3
        new File(lastCreatedFile).delete();
    }

}
