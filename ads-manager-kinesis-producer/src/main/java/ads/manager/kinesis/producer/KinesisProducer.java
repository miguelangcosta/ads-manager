package ads.manager.kinesis.producer;

import ads.manager.common.AWS;
import ads.manager.common.AdEvent;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.elastictranscoder.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 *  Kinesis Producer Application.
 */
public class KinesisProducer {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisProducer.class);
    private final ObjectMapper JSON_MAPPER = new ObjectMapper();
    AmazonKinesisClient kinesisClient;

    /**  Creates a Kinesis client.
    * @param endpoint the endpoint where the client is going to be created.
    * @param serviceName by default should be kinesis.
    * @param region the region where the kinesis client is be created.
    * */
    public void createKinesisClient(String endpoint, String serviceName, String region){

        AWSCredentials credentials = AWS.getCredentials();
        AmazonKinesisClient client = new AmazonKinesisClient(credentials);
        client.setEndpoint(endpoint, serviceName, region);
        LOG.info("Kinesis client initiated!");

        this.kinesisClient = client;
    }

    /**  Creates a Kinesis stream.
    *   @param myStreamName the name of the stream to be created
    */
    public void createStream(String myStreamName, int shardCount){

        if(!listStreams().contains(myStreamName)){

            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(myStreamName);

            //Define how many shards we want to use
            createStreamRequest.setShardCount(shardCount);

            //Creates the stream
            this.kinesisClient.createStream(createStreamRequest);

            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
            describeStreamRequest.setStreamName( myStreamName );

            long startTime = System.currentTimeMillis();
            long endTime = startTime + ( 10 * 60 * 1000 );
            while ( System.currentTimeMillis() < endTime ) {
                try {
                    Thread.sleep(20 * 1000);
                }
                catch ( Exception e ) {}

                try {
                    DescribeStreamResult describeStreamResponse = this.kinesisClient.describeStream( describeStreamRequest );
                    String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                    if ( streamStatus.equals( "ACTIVE" ) ) {
                        break;
                    }
                    //
                    // sleep for one second
                    //
                    try {
                        Thread.sleep( 1000 );
                    }
                    catch ( Exception e ) {}
                }
                catch ( ResourceNotFoundException e ) {}
            }
            if ( System.currentTimeMillis() >= endTime ) {
                throw new RuntimeException( "Stream " + myStreamName + " never went active" );
            }
        }else{
            LOG.info("Using already existent stream!");
        }
    }

    /**  List the existing streams in the Kinesis client
     *   @return the existing streams
     */
    public List<String> listStreams(){
        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(20);
        ListStreamsResult listStreamsResult = this.kinesisClient.listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();

        while (listStreamsResult.getHasMoreStreams())
        {
            if (streamNames.size() > 0) {
                listStreamsRequest.setExclusiveStartStreamName(streamNames
                        .get(streamNames.size() - 1));
            }
            listStreamsResult = this.kinesisClient.listStreams(listStreamsRequest);
            streamNames.addAll(listStreamsResult.getStreamNames());
        }

        for(String streamName : streamNames){
            LOG.info("This stream is active:{}", streamName);
        }

        return streamNames;
    }

    /**  Delete a stream from the Kinesis client
     *   @param myStreamName the name of the stream to be deleted
    */
    public void deleteStream(String myStreamName){
        DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
        deleteStreamRequest.setStreamName(myStreamName);
        this.kinesisClient.deleteStream(deleteStreamRequest);
    }

    /**  Send a List of records to Kinesis
     *   @param myStreamName the name of the stream to send the records
     *   @param records a list of records to be sent to Kinesis
     */
    public PutRecordsResult putRecords(String myStreamName, List<String> records){

        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(myStreamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList();

        for(String record : records){

            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap(record.getBytes()));
            try{
                AdEvent adEvent = JSON_MAPPER.readValue(record, AdEvent.class);
                //Assuming that the AppId is the best partition key
                putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", adEvent.getAppId()));
                putRecordsRequestEntryList.add(putRecordsRequestEntry);
            }catch (IOException e) {
                LOG.error(e.getMessage());
                System.exit(-1);
            }
        }
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        //Send the records to Kinesis
        return this.kinesisClient.putRecords(putRecordsRequest);
   }


    /**  Send a single record at a time to Kinesis
     *   @param myStreamName the name of the stream to send the records
     *   @param sequenceNumberOfPreviousRecord the sequence number of the previous record
     *   @param record the record to be sent
     */
    public String putRecord(String myStreamName, String sequenceNumberOfPreviousRecord, String record){
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName( myStreamName );
        putRecordRequest.setData(ByteBuffer.wrap(record.getBytes() ));

        try{
            AdEvent adEvent = JSON_MAPPER.readValue(record, AdEvent.class);
            //Assuming that the AppId is the best partition key
            putRecordRequest.setPartitionKey( String.format("partitionKey-%d", adEvent.getAppId()));
            putRecordRequest.setSequenceNumberForOrdering( sequenceNumberOfPreviousRecord );
        }catch (IOException e) {
            LOG.error(e.getMessage());
            System.exit(-1);
        }

        //Send the record to Kinesis
        PutRecordResult putRecordResult = this.kinesisClient.putRecord(putRecordRequest);
        return putRecordResult.getSequenceNumber();
    }
}
