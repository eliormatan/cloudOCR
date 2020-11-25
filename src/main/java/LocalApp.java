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

import java.nio.file.Paths;
import java.util.Base64;
import java.util.Date;
import java.util.List;

public class LocalApp {

    private static Ec2Client ec2;
    private static S3Client s3;
    private static SqsClient sqs;
    private static final String amiId = "ami-076515f20540e6e0b";


    public static void main(String[] args) {
        final String input = args[1];
        final String output = args[2];
        final Region region = Region.US_WEST_2;
        final String bucket = "bucket_ocr";
        final String queue = "queue" + new Date().getTime();
        final String new_task = "new task";
        final String done_task = "done task";
        final String arguments =
                "The application should be run as follows:\n" +
                        "java -jar yourjar.jar inputFileName outputFileName n terminate(optional)";
        String outputS3Path = null;
        boolean done = false;
        boolean terminate = args.length > 4 && args[4].equals("terminate");

        if (args.length < 4)
            throw new IllegalArgumentException(arguments);


        try {
            //define s3
            s3 = S3Client.builder().region(region).build();
            createBucket(bucket, region);

            //define sqs
            sqs = SqsClient.builder().region(region).build();
            String queueUrl = createQueueRequestAndGetUrl(queue);

            //upload the input file to s3
            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(input).acl(ObjectCannedACL.PUBLIC_READ).build(),
                    Paths.get(input));

                /*s3.getUrl(bucket, input).toExternalForm();
                GetUrlRequest request = GetUrlRequest.builder().bucket(bucket).key(key).build();
                String inputS3Path = s3.getUrl(bucket, key).toExternalForm();
                String inputS3Pat2h = s3.utilities().getUrl(request).toExternalForm();

                 S3Utilities utilities = S3Utilities.builder().region(Region.US_WEST_2).build();
                 GetUrlRequest request2 = GetUrlRequest.builder().bucket("foo-bucket").key("key-without-spaces").build();
                 URL url = utilities.getUrl(request);

                S3Client s3client = S3Client.create();
                S3Utilities utilities2 = s3client.utilities();
                GetUrlRequest request3 = GetUrlRequest.builder().bucket("foo-bucket").key("key-without-spaces").build();
                URL url = utilities.getUrl(request);*/

            //send the location of the file in s3 to the queue (using $ as a delimiter)
            sendMessage(queueUrl, new_task + "$" + bucket + "$" + input);


            //check if a 'Manager' node is active on the EC2 cloud. If it is not, the application will start the manager node.
            startManager();

            while (!done) {
                // receive messages from the queue
                List<Message> messages = receiveMessages(queueUrl);
                for (Message m : messages) {
                    String[] bodyArr = m.body().split("$");
                    //check for 'done task' message
                    if (bodyArr[0].equals(done_task)) {
                        //delete message
                        deleteMessage(queueUrl, m);
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

            // download summary file from S3 and write it to output file (assuming it comes in html representation from the manager)
            s3.getObject(GetObjectRequest.builder().bucket(bucket).key(outputS3Path).build(),
                    ResponseTransformer.toFile(Paths.get(output)));

            if (terminate)
                sendMessage(queueUrl, "terminate");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createBucket(String bucket, Region region) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .locationConstraint(region.id())
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
            ec2 = Ec2Client.create();

            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T1_MICRO)
                    .imageId(amiId)
                    .maxCount(1)
                    .minCount(1)
                    .userData(Base64.getEncoder().encodeToString(getUserDataScript().getBytes()))
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);

            String instanceId = response.instances().get(0).instanceId();

            Tag tag = Tag.builder()
                    .key("Manager")
                    .value("Manager")
                    .build();

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();

            try {
                ec2.createTags(tagRequest);
                System.out.printf(
                        "Successfully started EC2 instance (Manager tag) %s based on AMI %s",
                        instanceId, amiId);

            } catch (Ec2Exception e) {
                e.printStackTrace();
            }
            System.out.println("done");
        }
    }
    private static boolean managerIsActive() {
        boolean isActive=false;
        //todo: check if manager is already active =  write function managerIsActive
          /*
        You can check if the manager is running by listing all your running instances, and checking if one of them is a manager.
        Use the "tags" feature of Amazon EC2 API to mark a specific instance as one running a Manager:
        -using tags
        -CreateTagsRequest API
        */
        //todo
        return isActive;
    }

    private static String getUserDataScript() {
        //todo: write USER DATA script string
        /*
            String userData = "";
            userData = userData + "#cloud-boothook\n";
            userData = userData + "#!/bin/bash\n";
            userData = userData + "sudo java -jar /home/ubuntu/Manager/Manager-1.0.jar " + bucketName + " " +
                    queueName + " " + arn + " " + keyName + " " + securityGroup + " " + workerAMI + " " + managerAMI;
            String base64UserData = null;
            try {
                base64UserData = new String(Base64.encodeBase64(userData.getBytes("UTF-8")), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            return base64UserData;
        }
         */
        return "";
    }




}
