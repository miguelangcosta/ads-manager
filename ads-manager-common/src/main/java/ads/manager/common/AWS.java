package ads.manager.common;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AWS {

    final static Logger LOG = LoggerFactory.getLogger(AWS.class);

    /**
     * Gets the AWS credentials
     * @return AWSCredentials The credentials to access AWS.
     * */
    public static AWSCredentials getCredentials(){

        AWSCredentials credentials;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
        return credentials;
    }

    /**
     * Gets the AWSCredentialsProvider
     * @return AWSCredentialsProvider The credentials provider for AWS.
     * */
    public static AWSCredentialsProvider getAWSCredentialsProvider() {

        // Get credentials from IMDS. If unsuccessful, get them from the credential profiles file.
        AWSCredentialsProvider credentialsProvider;
        try {
            credentialsProvider = new InstanceProfileCredentialsProvider();
            // Verify we can fetch credentials from the provider
            credentialsProvider.getCredentials();
            LOG.info("Obtained credentials from the IMDS.");
        } catch (AmazonClientException e) {
            LOG.info("Unable to obtain credentials from the IMDS, trying classpath properties", e);
            credentialsProvider = new ProfileCredentialsProvider();
            // Verify we can fetch credentials from the provider
            credentialsProvider.getCredentials();
            LOG.info("Obtained credentials from the properties file.");
        }
        return credentialsProvider;

    }

}
