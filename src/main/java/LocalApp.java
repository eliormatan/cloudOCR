import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LocalApp {

    private static Ec2Client ec2;
    private static S3Client s3;
    private static SqsClient sqs;
    private static final String amiId = "ami-068dc7ca584573afe";

    public static void main(String[] args) {
        Logger logger = Logger.getLogger(LocalApp.class.getName());//todo
//        FileHandler fileHandler = new FileHandler(logger.getName() + ".txt");
//        fileHandler.setFormatter(new SimpleFormatter());
        logger.setLevel(Level.ALL);
//        logger.addHandler(fileHandler);
        logger.info("start time");
        final String arguments =
                "The application should be run as follows:\n" +
                        "java -jar yourjar.jar inputFileName outputFileName n terminate(optional)";
        if (args.length < 3)
            throw new IllegalArgumentException(arguments);
        final String input = args[0];
        final String output = args[1];
        int filesRatio = Integer.parseInt(args[2]);
        Region region = Region.US_EAST_1;
        final String bucket = "bucket" + System.currentTimeMillis();
        final String key = input;
        final String local2ManagerQ = "local2ManagerQ";
        final String manager2LocalQ = "manager2LocalQ";
        final String localId = "local" + System.currentTimeMillis();
        final String new_task = "new task";
        final String done_task = "done task";
        String outputS3Path = null;
        boolean done = false;
        boolean terminate = args.length > 3 && args[3].equals("terminate");


        try {
            ec2 = Ec2Client.builder().region(region).build();

            s3 = S3Client.builder().region(region).build();

//            uploadJars();


//            /*
            createBucket(bucket);
            sqs = SqsClient.builder().region(region).build();
            String l2m_qUrl = createQueueRequestAndGetUrl(local2ManagerQ);
            String m2l_qUrl = createQueueRequestAndGetUrl(manager2LocalQ + localId);

            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).acl(ObjectCannedACL.PUBLIC_READ).build(),
                    Paths.get(input));

            if (terminate) {
                sendMessage(l2m_qUrl, new_task + "$" + bucket + "$" + key + "$" + localId + "$" + filesRatio + "$" + "terminate");

            } else {
                sendMessage(l2m_qUrl, new_task + "$" + bucket + "$" + key + "$" + localId + "$" + filesRatio);
            }

//            startManager();

            while (!done) {
                List<Message> messages = receiveMessages(m2l_qUrl);
                for (Message m : messages) {
                    String[] bodyArr = m.body().split("\\$");
                    if (bodyArr[0].equals(done_task) && bodyArr[1].equals(localId)) {
                        deleteMessage(m2l_qUrl, m);
                        outputS3Path = bodyArr[1];
                        done = true;
                    }
                }

//                if (!managerIsActive() && !isOutputWaiting(bucket)) {
//                    deleteBucketAndM2LQueue(manager2LocalQ + localId, bucket);
//                    System.exit(1);
//                }
                try{
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            s3.getObject(GetObjectRequest.builder().bucket(bucket).key("output" + outputS3Path + ".html").build(),
                    ResponseTransformer.toFile(Paths.get(localId + output)));

            deleteBucketAndM2LQueue(manager2LocalQ + localId, bucket);

//            */
            logger.info("end time"); //todo

        } catch (
                Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    private static boolean isOutputWaiting(String bucket) {
        boolean res=false;
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).build();
        ListObjectsV2Response listObjectsV2Response=s3.listObjectsV2(listObjectsV2Request);
        if(listObjectsV2Response.contents().size()==2)
            res=true;
        return res;
    }

    private static void deleteBucketAndM2LQueue(String queue, String bucket) {
        deleteSQSQueue(queue);
        deleteBucket(bucket);
    }


    private static void createBucket(String bucket) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .build())
                .build());
    }


    private static String createQueueRequestAndGetUrl(String queue) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queue)
                .build();
        sqs.createQueue(request);
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    private static void sendMessage(String queueUrl, String message) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
//                .delaySeconds(5)
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
                    .keyName("ass1")
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::794818403225:instance-profile/ami-dsp211-ass1").build())
                    .securityGroupIds("sg-0630dc054e0184c80")
                    .userData(Base64.getEncoder().encodeToString(getUserDataScript().getBytes()))
                    .instanceInitiatedShutdownBehavior("terminate")
                    .tagSpecifications(tags)
                    .build();

            ec2.runInstances(runRequest);
        }
    }

    private static boolean managerIsActive() {
        boolean isActive = false;

        try {

            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .filters(Filter.builder().name("tag:Manager").values("Manager").build(), Filter.builder().name("instance-state-name").values(new String[]{"running", "pending"}).build()).build();

            DescribeInstancesResponse response = ec2.describeInstances(request);
            if (!response.reservations().isEmpty())
                isActive = true;
        } catch (Ec2Exception e) {
            System.out.println(e.awsErrorDetails().errorMessage());
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
            System.out.println(e.awsErrorDetails().errorMessage());
        }
    }


    private static void deleteBucket(String bucket) {
        try {
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

            } while (listObjectsV2Response.isTruncated());
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
            s3.deleteBucket(deleteBucketRequest);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static String getUserDataScript() {
        final String bucket = "dsp211-ass1-jars";
        final String key = "Manager.jar";

        return "#!/bin/bash\n" +
                "wget https://" + bucket + ".s3.amazonaws.com/" + key + " -O " + key + "\n" +
                "java -jar " + key + "\n" +
                "shutdown now";
    }

    private static void uploadJars() {
        try {
/*
           s3.createBucket(CreateBucketRequest
                    .builder().bucket("dsp211-ass1-jars")
                    .createBucketConfiguration(CreateBucketConfiguration.builder().build()).build());

  */

            s3.putObject(PutObjectRequest.builder()
                            .bucket("dsp211-ass1-jars")
                            .key("Manager.jar").acl(ObjectCannedACL.PUBLIC_READ)
                            .build(),
                    Paths.get("Manager.jar"));
            s3.putObject(PutObjectRequest.builder()
                            .bucket("dsp211-ass1-jars")
                            .key("Worker.jar").acl(ObjectCannedACL.PUBLIC_READ)
                            .build(),
                    Paths.get("Worker.jar"));

        } catch (S3Exception e) {
            e.printStackTrace();
        }
    }
}
