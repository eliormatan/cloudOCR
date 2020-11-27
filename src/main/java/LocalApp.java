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
import java.util.List;

public class LocalApp {

    private static Ec2Client ec2;
    private static S3Client s3;
    private static SqsClient sqs;
    private static int filesRatio;
    private static final String amiId = "ami-076515f20540e6e0b"; //includes java


    public static void main(String[] args) {
        final String input = args[1];
        final String output = args[2];
        filesRatio = Integer.parseInt(args[3]);
        final Region region = Region.US_WEST_2;
        final String bucket = "bucket_ocr";
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
        boolean terminate = args.length > 4 && args[4].equals("terminate");

        if (args.length < 4)
            throw new IllegalArgumentException(arguments);


        try {
            //define s3
            s3 = S3Client.builder().region(region).build();
            createBucket(bucket, region);

            //define sqs
            sqs = SqsClient.builder().region(region).build();
            String l2m_qUrl = createQueueRequestAndGetUrl(local2ManagerQ);
            String m2l_qUrl = createQueueRequestAndGetUrl(manager2LocalQ);

            //upload the input file to s3
            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(input).acl(ObjectCannedACL.PUBLIC_READ).build(),
                    Paths.get(input));

            //send the location of the file in s3 to the queue (using $ as a delimiter)
            sendMessage(l2m_qUrl, new_task + "$" + bucket + "$" + input + "$" + localId + "$" + filesRatio);

            //check if a 'Manager' node is active on the EC2 cloud. If it is not, the application will start the manager node.
            startManager();

            while (!done) {
                // receive messages from the queue
                List<Message> messages = receiveMessages(m2l_qUrl);
                for (Message m : messages) {
                    String[] bodyArr = m.body().split("$");
                    //check for 'done task' message
                    if (bodyArr[0].equals(done_task)) {
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

            //todo: make sure the summary file in s3 (uploaded by the manager)is already in html format

            // download summary file from S3 and write it to output file
            s3.getObject(GetObjectRequest.builder().bucket(bucket).key(outputS3Path).build(),
                    ResponseTransformer.toFile(Paths.get(output)));

            if (terminate)
                sendMessage(l2m_qUrl, "terminate");

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
                    .instanceType(InstanceType.T2_MICRO)
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

        try {

            //option 1 - filter instances by manager tag
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(1).filters(Filter.builder().name("tag:Manager").build()).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            //option 2 - get all instance's tags , then search for manager
            /*DescribeTagsResponse response = ec2.describeTags();
            List<TagDescription> tags = response.tags();
            if(tags.contains("Manager"))
                isActive=true;
             */

            if(!response.reservations().isEmpty())
                isActive=true;
            } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

        return isActive;
    }

    private static String getUserDataScript() {
        //todo: write USER DATA script string
            String userData =
                    //run the file with bash
                    "#!/bin/bash\n"+
                    //install maven
                    "echo installing maven\n" +
                    "sudo apt-get install maven\n"+
                    "mvn -version" +
                    "echo download jar file\r\n" +

                    //todo: upload jars to internet and put the location do download here
                    //maybe use https://gofile.io/uploadFiles (it stays 10 days in the web
                    //,started counting from the last day it was downloaded)
                    // also can use github or aws s3
                    //todo: use wget to download jars (https://linuxize.com/post/wget-command-examples/)
                    // "wget ..."
                    //example: wget http://www.cs.bgu.ac.il/~dsp211/Main -O dsp.html
                    // will download the content at http://www.cs.bgu.ac.il/~dsp211/Main and save it to a file named dsp.html

                    //this ami should include java already
                    //run Manager
                    "echo running Manager\r\n" +
                    "java -jar Manager.jar";

            /* basic commands
            //https://dev.to/awwsmm/101-bash-commands-and-tips-for-beginners-to-experts-30je
             */

        return userData;
    }




}
