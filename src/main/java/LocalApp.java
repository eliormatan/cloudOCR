import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LocalApp {

    private static Ec2Client ec2;
    private static S3Client s3;
    private static SqsClient sqs;
    private static int filesRatio;
    private static final String amiId = "ami-068dc7ca584573afe";

    private static Logger logger = Logger.getLogger(LocalApp.class.getName());
        //commit
        public static void main(String[] args) {
            try {
                initLogger("LocalAppLogger");
            } catch (IOException e) {
                e.printStackTrace();
            }

        final String input = args[0];
        final String output = args[1];
        filesRatio = Integer.parseInt(args[2]);
        Region region = Region.US_EAST_1;
        final String bucket = "bucket"+System.currentTimeMillis();
        final String key = "input.txt";
        final String local2ManagerQ = "local2ManagerQ";
        final String manager2LocalQ = "manager2LocalQ";
        final String localId = "local"+System.currentTimeMillis();
        final String new_task = "new task";
        final String done_task = "done task";
        final String arguments =
                "The application should be run as follows:\n" +
                        "java -jar yourjar.jar inputFileName outputFileName n terminate(optional)";
        String outputS3Path = null;
        boolean done = false;
        printWithColor("program args are: "+ Arrays.toString(args));
        boolean terminate = args.length > 3 && args[3].equals("terminate");
        printWithColor("TERMINATE = " + terminate);
        if (args.length < 3)
            throw new IllegalArgumentException(arguments);


        try {
            //define ec2
            ec2 = Ec2Client.builder().region(region).build();

            //define s3
            s3 = S3Client.builder().region(region).build();

            uploadJars();
//            deleteBucket("bucket1607447214493");
//            deleteBucket("bucket1607447217045");
//            deleteBucket("bucket1607447226020");

            createBucket(bucket, region);
            printWithColor("created bucket "+bucket);
//            /*

            //define sqs
            sqs = SqsClient.builder().region(region).build();
            String l2m_qUrl = createQueueRequestAndGetUrl(local2ManagerQ);
            String m2l_qUrl = createQueueRequestAndGetUrl(manager2LocalQ+localId);
            printWithColor("created local2manager and manager2local queues: "+l2m_qUrl+" "+m2l_qUrl);

            //upload the input file to s3
            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).acl(ObjectCannedACL.PUBLIC_READ).build(),
                    Paths.get(input));
            printWithColor("uploaded file to s3 https://" + bucket + ".s3.amazonaws.com/" + key);

            //send the location of the file in s3 to the queue (using $ as a delimiter) with terminate if provided
            if(terminate){
                sendMessage(l2m_qUrl, new_task + "$" + bucket + "$" + key + "$" + localId + "$" + filesRatio + "$" + "terminate");
                printWithColor("sent new task to local2manager queue "+new_task + "$" + bucket + "$" + key + "$" + localId + "$" + filesRatio+ " with TERMINATE");

            }
            else{
                sendMessage(l2m_qUrl, new_task + "$" + bucket + "$" + key + "$" + localId + "$" + filesRatio);
                printWithColor("sent new task to local2manager queue "+new_task + "$" + bucket + "$" + key + "$" + localId + "$" + filesRatio);
            }

            //check if a 'Manager' node is active on the EC2 cloud. If it is not, the application will start the manager node.
            startManager();

            while (!done) {
                // receive messages from the queue
                List<Message> messages = receiveMessages(m2l_qUrl);
                for (Message m : messages) {
                    String[] bodyArr = m.body().split("\\$");
                    //check for 'done task' message
                    if (bodyArr[0].equals(done_task) && bodyArr[1].equals(localId)) {
                        printWithColor("received done task!");
                        //delete message
                        deleteMessage(m2l_qUrl, m);
                        //get s3 location of the output file
                        outputS3Path = bodyArr[1];
                        done = true;
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // download summary file from S3 and write it to output file
            printWithColor("downloading summary from : bucket = "+bucket+" key = output "+outputS3Path);
            s3.getObject(GetObjectRequest.builder().bucket(bucket).key("output"+outputS3Path+".html").build(),
                    ResponseTransformer.toFile(Paths.get(localId+output)));

//*/
            //delete s3 bucket
            deleteBucket(bucket);
            printWithColor("deleted bucket "+bucket);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        printWithColor("finished elegantly!");
    }


    private static void createBucket(String bucket, Region region) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                //                .locationConstraint(region.id())
                                .build())
                .build());
    }


    private static String createQueueRequestAndGetUrl(String queue) {
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queue)
                    .build();
            sqs.createQueue(request);
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queue)
                    .build();
            return sqs.getQueueUrl(getQueueRequest).queueUrl();
        } catch (QueueNameExistsException e) {
            throw e;
        }
    }

    private static void sendMessage(String queueUrl, String message) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
    }

    private static List<Message> receiveMessages(String queueUrl) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        return sqs.receiveMessage(receiveRequest).messages();
    }

    private static void deleteMessage(String queueUrl, Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }

    private static void startManager() {

        if (managerIsActive())
            System.out.println("manager is already active...");
        else {

            Tag tag = Tag.builder()
                    .key("Manager")
                    .value("Manager")
                    .build();
            TagSpecification tags = TagSpecification.builder().tags(tag).resourceType(ResourceType.INSTANCE).build();

            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T2_MICRO)
                    .imageId(amiId)
                    .maxCount(1)
                    .minCount(1)
                    .keyName("dspass1")
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::320131450129:instance-profile/dspass1").build())
                    .securityGroupIds("sg-0eead8b108fc9f860")
                    .userData(Base64.getEncoder().encodeToString(getUserDataScript().getBytes()))
                    .instanceInitiatedShutdownBehavior("terminate") //added
                    .tagSpecifications(tags)//added
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);
            System.out.println("done");
        }
    }
    private static boolean managerIsActive() {
        boolean isActive=false;

        try {

           DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                            .filters(Filter.builder().name("tag:Manager").values("Manager").build(),Filter.builder().name("instance-state-name").values(new String[]{"running", "pending"}).build()).build();

            DescribeInstancesResponse response = ec2.describeInstances(request);
            printWithColor("is manager not active? "+response.reservations().isEmpty());
            if(!response.reservations().isEmpty())
                isActive=true;
            } catch (Ec2Exception e) {
            printWithColor(e.awsErrorDetails().errorMessage());
        }

        return isActive;
    }

    public static void deleteSQSQueue(String queueName) {

        try {

            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqs.deleteQueue(deleteQueueRequest);

        } catch (SqsException e) {
            printWithColor(e.awsErrorDetails().errorMessage());
        }
    }



    private static void deleteBucket(String bucket) {
        try {
            // To delete a bucket, all the objects in the bucket must be deleted first
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).build();
            ListObjectsV2Response listObjectsV2Response;

            do {
                listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    s3.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key(s3Object.key())
                            .build());
                }

                listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket)
                        .continuationToken(listObjectsV2Response.nextContinuationToken())
                        .build();

            } while(listObjectsV2Response.isTruncated());
            // snippet-end:[s3.java2.bucket_deletion.delete_bucket]

            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
            s3.deleteBucket(deleteBucketRequest);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static String getUserDataScript() {
        final String bucket="dsp211-ass1-jar";
        final String key="Manager.jar";

                String userData =
                //run the file with bash
                "#!/bin/bash\n"+
                        "exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1\n"+

                        //download Manager jar
                        "echo download "+key+ "\r\n" +
                        "wget https://" + bucket + ".s3.amazonaws.com/" + key +" -O " +key+ "\n" +
                        // run Manager
                        "echo running "+key+"\r\n" +
                        "java -jar "+key+"\n" +
                        "shutdown now";//added

        return userData;
    }

    // use only once
    private static void uploadJars(){
        try {
/*
            s3.createBucket(CreateBucketRequest
                    .builder().bucket("dsp211-ass1-jar")
                    .createBucketConfiguration(CreateBucketConfiguration.builder().build()).build());
                            //.locationConstraint(region.id()).build()).build());
            s3.putObject(PutObjectRequest.builder()
                            .bucket("dsp211-ass1-jar")
                            .key("Worker.jar").acl(ObjectCannedACL.PUBLIC_READ)
                            .build(),
                    Paths.get("Worker.jar"));
                    */
            s3.putObject(PutObjectRequest.builder()
                            .bucket("dsp211-ass1-jar")
                            .key("Manager.jar").acl(ObjectCannedACL.PUBLIC_READ)
                            .build(),
                    Paths.get("Manager.jar"));

        }catch (S3Exception e){
            e.printStackTrace();
        }
    }



    private static void printWithColor (String string){
        final String ANSI_CYAN = "\u001B[36m";
        final String ANSI_BLACK = "\u001B[30m";
        final String ANSI_RESET = "\u001B[0m";
        final String ANSI_CYAN_BACKGROUND = "\u001B[46m";
        final String ANSI_WHITE_BACKGROUND = "\u001B[47m";
        //System.out.println(ANSI_CYAN_BACKGROUND +ANSI_BLACK + string + ANSI_RESET);
        logger.info(string);

    }

    public static void initLogger(String loggerName) throws IOException{
        FileHandler fileHandler = new FileHandler(loggerName + ".txt");
        fileHandler.setFormatter(new SimpleFormatter());
        logger.setLevel(Level.ALL);
        logger.addHandler(fileHandler);
    }



}
