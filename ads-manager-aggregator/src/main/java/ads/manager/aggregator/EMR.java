package ads.manager.aggregator;

import ads.manager.common.AWS;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class EMR {

    final static Logger LOG = LoggerFactory.getLogger(EMR.class);

    /**
     * Creates a ads.manager.aggregator.EMR cluster and runs the pig script to aggregate the ads.
     */
    public void runPigAdsAggregator(Properties properties){

        String regionName = properties.getProperty("aws.regionName");
        AWSCredentials credentials = AWS.getCredentials();
        AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(credentials);
        emr.setRegion(Regions.fromName(regionName));
        emr.setEndpoint(properties.getProperty("aws.endpoint"));

        String runnerJar = "s3://" + regionName + ".elasticmapreduce/libs/script-runner/script-runner.jar";
        String actionOnFailure = "TERMINATE_JOB_FLOW";

        HadoopJarStepConfig hadoopConfig = new HadoopJarStepConfig()
                .withJar(runnerJar)
                .withArgs("s3://" + regionName + ".elasticmapreduce/libs/state-pusher/0.1/fetch");

        StepConfig enableDebugging = new StepConfig()
                .withName("Enable debugging")
                .withActionOnFailure(actionOnFailure)
                .withHadoopJarStep(hadoopConfig);

        List<String> args = new ArrayList<>();
        args.add("s3://" + regionName + ".elasticmapreduce/libs/pig/pig-script");
        args.add("--base-path");
        args.add("s3://" + regionName + ".elasticmapreduce/libs/pig/");
        args.add("--install-pig");
        args.add("--pig-versions");
        args.add("latest");
        hadoopConfig = new HadoopJarStepConfig()
                .withJar(runnerJar)
                .withArgs(args.toArray(new String[0]));


        StepConfig installPig = new StepConfig()
                .withName("Install Pig")
                .withActionOnFailure(actionOnFailure)
                .withHadoopJarStep(hadoopConfig);

        args = new ArrayList<>();
        args.add("s3://" + regionName +".elasticmapreduce/libs/pig/pig-script");
        args.add("--base-path");
        args.add("s3://" + regionName + ".elasticmapreduce/libs/pig/");
        args.add("--pig-versions");
        args.add("latest");
        args.add("--run-pig-script");
        args.add("--args");
        args.add("-f");
        args.add("s3://" + properties.getProperty("s3.bucketName") + "/" + properties.getProperty("s3.pigScriptPath"));
        args.add("-p");
        args.add("INPUT_PATH=" + properties.getProperty("pig.scriptInputPath"));
        args.add("-p");
        args.add("OUTPUT_PATH=" + properties.getProperty("pig.scriptOutputPath"));

        hadoopConfig = new HadoopJarStepConfig()
                .withJar(runnerJar)
                .withArgs(args.toArray(new String[0]));

        StepConfig runPigScript = new StepConfig()
                .withName("Run Pig Script")
                .withActionOnFailure(actionOnFailure)
                .withHadoopJarStep(hadoopConfig);

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("Pig Ads Aggregator")
                .withSteps(enableDebugging, installPig, runPigScript)
                .withLogUri(properties.getProperty("emr.logUri"))
                .withAmiVersion(properties.getProperty("emr.amiVersion"))
                .withInstances(new JobFlowInstancesConfig()
                        .withEc2KeyName(properties.getProperty("emr.ec2KeyName"))
                        .withHadoopVersion(properties.getProperty("emr.hadoopVersion"))
                        .withInstanceCount(Integer.parseInt(properties.getProperty("emr.instances")))
                        .withKeepJobFlowAliveWhenNoSteps(false)
                        .withMasterInstanceType(properties.getProperty("emr.masterInstanceType"))
                        .withSlaveInstanceType(properties.getProperty("emr.slaveInstanceType")));

        request.setVisibleToAllUsers(true);

        LOG.info("Creating ads.manager.aggregator.EMR cluster and run PIG script...");
        RunJobFlowResult result = emr.runJobFlow(request);

        if(!waitForCompletion(emr, result).equals("COMPLETED")){
            throw new RuntimeException("Error Running Pig Script or Creating ads.manager.aggregator.EMR Cluster");
        }
    }

    /**
     * Waits for the ads.manager.aggregator.EMR job to complete.
     * @param emr The emr cluster
     * @param result The result of the request to create the cluster.
     * @return The last state of the job.
     * */
    private String waitForCompletion(AmazonElasticMapReduceClient emr, RunJobFlowResult result) {

        //Check the status of the running job
        String lastState = "";
        String state = "";
        try {
            STATUS_LOOP:
            while (true) {
                DescribeJobFlowsRequest desc =
                        new DescribeJobFlowsRequest(
                                Arrays.asList(new String[]{result.getJobFlowId()}));

                DescribeJobFlowsResult descResult = emr.describeJobFlows(desc);

                for (JobFlowDetail detail : descResult.getJobFlows()) {

                    state = detail.getExecutionStatusDetail().getState();
                    if (isDone(state)) {
                        LOG.debug("Job state" + ": " + detail.toString());
                        break STATUS_LOOP;
                    } else if (!lastState.equals(state)) {
                        lastState = state;
                        LOG.debug("Job state" + " at " + new Date().toString());
                    }
                }

                //wait until check the state again
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            System.exit(-1);
        } catch (AmazonServiceException ase) {
            LOG.error(ase.getMessage());
            System.exit(-1);
        }
        return state;
    }

    /**
     * Check if the job is already finished.
     * @param value The current state of the job.
     * @return True if the job already finished.
     */
    public static boolean isDone(String value){

        List<JobFlowExecutionState> doneStates = Arrays
            .asList(new JobFlowExecutionState[] { JobFlowExecutionState.COMPLETED,
                    JobFlowExecutionState.FAILED,
                    JobFlowExecutionState.TERMINATED });

        JobFlowExecutionState state = JobFlowExecutionState.fromValue(value);
        return doneStates.contains(state);
    }
}
