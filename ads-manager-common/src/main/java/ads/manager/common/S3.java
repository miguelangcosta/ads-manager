package ads.manager.common;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;

public class S3 {

    final static Logger LOG = LoggerFactory.getLogger(S3.class);
    private AmazonS3 s3;
    private Region reg;

    /**
     * Constructor
     */
    public S3() {
        // TODO: Region is hardcoded, must come from properties file.
        this.s3 = new AmazonS3Client(AWS.getCredentials());
        this.reg = Region.getRegion(Regions.EU_WEST_1);
        s3.setRegion(this.reg);
    }

    /**
     * Uploads the file to S3
     * @param bucketName    of the bucket at ads.manager.common.S3
     * @param key           Key (path to the object including its name) of the object at ads.manager.common.S3
     * @param localFileName Name of the local file to upload
     * @return true if upload is successful, false if it fails
     */
    public boolean uploadFile(String bucketName, String key, String localFileName) {
        LOG.info("Uploading file to ads.manager.common.S3 path:" + bucketName + "/" + key + "\n");
        try {
            File localFile = new File(localFileName);
            this.s3.putObject(new PutObjectRequest(bucketName, key, localFile));
        } catch (AmazonServiceException ase) {
            LOG.error(ase.getMessage());
            return false;
        } catch (AmazonClientException ace) {
            LOG.error(ace.getMessage());
            return false;
        }

        return true;
    }
}