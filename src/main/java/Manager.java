import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Manager {
    private static S3Client s3;
    private static SqsClient sqs;//todo
//    private static SqsAsyncClient sqs;
    private static Ec2Client ec2;
    private static final String amiId = "ami-068dc7ca584573afe";
    private static final String local2ManagerQ = "local2ManagerQ";
    private static final String manager2LocalQ = "manager2LocalQ";
    private static final String manager2WorkerQ = "manager2WorkerQ";
    private static final String worker2ManagerQ = "worker2ManagerQ";
    private static final String new_image_task = "new image task";
    private static final String done_task = "done task";
    private static List<String> workerIds;

    private static final Logger logger = Logger.getLogger(Manager.class.getName());


    public static void main(String[] args) {
//        try {
//            initLogger("ManagerLogger");
        logger.setLevel(Level.ALL);

//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);  //todo
        workerIds = new LinkedList<>();
        Region region = Region.US_EAST_1;
        ec2 = Ec2Client.builder().region(region).build();
        sqs = SqsClient.builder().region(region).build();//todo
//        sqs= SqsAsyncClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();

        //to get the queue that sends new tasks from local app to manager we need to use the same name ,
        //queue access by name, i called it local2ManagerQ
        //local 2 manager queue url = local2ManagerQUrl
        String local2ManagerQUrl = getQueueRequestAndGetUrl(local2ManagerQ);
//        String local2ManagerQUrl = null;
//        local2ManagerQUrl = getQueueRequestAndGetUrlAsync(local2ManagerQ);

        logger.info("get local2manager" + local2ManagerQUrl);

        //create queues to send & receive messages from workers
        //createQueueRequestAndGetUrl function is taken from LocalApp
        String manager2WorkerQUrl = createQueueRequestAndGetUrl(manager2WorkerQ);
        String worker2ManagerQUrl = createQueueRequestAndGetUrl(worker2ManagerQ);
        logger.info("created manager2Worker and worker2Manager queues: " + manager2WorkerQUrl + " " + worker2ManagerQUrl);

        int pollNum = 10;
        int waitTime = 1;
        boolean terminate = false;
        while (!terminate) {
            logger.info("thread = " + Thread.currentThread().getId() + " in while loop receiving messages");
            //STEP 4: Manager downloads list of images
            //receive messages from localApp queue

            List<Message> messages =receiveMessages(local2ManagerQUrl,pollNum,waitTime);

            /*if (messages.isEmpty()) {
                if (pollNum != 1) {
                    pollNum = 1;
                    waitTime =20;
                }
            } else {
                pollNum = 10;
                waitTime = 1;
            }*/

            for (Message message : messages) {
                int linesPerWorker;
                String localId;
                String key;
                String bucket;
                String messageBody = message.body();
                logger.info("manager recieved message from local: " + messageBody);

                //split message and extract needed info
                //message should be "new task$bucket$key$n$localID$terminate"
                String[] splitMessage = messageBody.split("\\$");
                System.out.println(Arrays.toString(splitMessage));
                bucket = splitMessage[1];
                key = splitMessage[2];
                linesPerWorker = Integer.parseInt(splitMessage[4]);
                localId = splitMessage[3];
                if (splitMessage.length > 5 && splitMessage[5].equals("terminate")) {
                    terminate = true;
                    logger.info("TERMINATE == TRUE");
                }

                //manager 2 local queue url = manager2LocalQUrl
                String manager2LocalQUrl = getQueueRequestAndGetUrl(manager2LocalQ + localId);
                logger.info("get manager2local " + manager2LocalQUrl);

                logger.info("thread = " + Thread.currentThread().getId() + " taking care of new task " + localId);

                //build the input file's link from the received arguments to download from s3
                s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                        ResponseTransformer.toFile(Paths.get("input" + localId + ".txt")));
                logger.info("file downloaded from s3");

                //STEP 5: Manager creates an SQS message for each URL in the list of images
                //read lines (image links) from input file and create new image tasks for workers out of each line
                int numberOfLines = 0;
                boolean allSent = false;
                try {
                    BufferedReader reader = new BufferedReader(new FileReader("input" + localId + ".txt"));
                    String line = reader.readLine();
                    while (line != null) {
                        numberOfLines++;
                        queueNewImageTask(manager2WorkerQUrl, line, localId);
                        logger.info("sent new image task to manager2Worker queue " + new_image_task + "$" + localId + "$" + line);

                        //onto the next line
                        line = reader.readLine();
                    }
                    allSent = true;
                    reader.close();
                    //done reading all new image task lines, delete the file since we are done with it
                    File input = new File("input" + localId + ".txt");
                    input.delete();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                logger.info("done sending...sent " + numberOfLines + " masseges (new tasks to workers)");

                //delete the "new task" message from local2managerQUrl sqs queue
                if (allSent) {
                    deleteMessage(local2ManagerQUrl, message);
                }
                logger.info("new task in " + local2ManagerQUrl + "queue deleted");

                int requiredWorkers = ((int) Math.ceil((double) numberOfLines / linesPerWorker));
                logger.info("required workers = " + requiredWorkers + " =numberOfLines / linesPerWorker =" + numberOfLines + "/ " + linesPerWorker);
//                STEP 6: Manager bootstraps nodes to process messages
//                start workers and bootstrap them

                startOrUpdateWorkers(requiredWorkers);
                logger.info("workers boosted");

//
//                STEP 11: Manager reads all the Workers' messages from SQS and creates one summary file
//                STEP 12: Manager uploads summary file to S3
//                STEP 13: Manager posts an SQS message about summary file
//                };
                HandleWorkerOutput handleWorkerOutput = new HandleWorkerOutput(localId, bucket, numberOfLines, worker2ManagerQUrl, manager2LocalQUrl);
                executor.execute(handleWorkerOutput);


            }
            try {
                Thread.sleep(1000); //todo
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //time to TERMINATE
        //terminate thread pool
        executor.shutdown();
        //wait for all threads to finish their tasks
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //terminate workers
        TerminateInstancesRequest termRequest = TerminateInstancesRequest.builder().instanceIds(workerIds).build();
        ec2.terminateInstances(termRequest);
        logger.info("workers killed");

        //delete queues
        deleteSQSQueue(worker2ManagerQ);
        deleteSQSQueue(manager2WorkerQ);
        deleteSQSQueue(local2ManagerQ);
        logger.info("worker2Manager and manager2Worker and local2manager queues deleted");

        //terminate Manager using shutdown command

    }

    private static List<Message> receiveMessages(String queueUrl,int pollNum,int waitTime) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                //.maxNumberOfMessages(pollNum)
                //.waitTimeSeconds(waitTime)
                .build();

        return sqs.receiveMessage(receiveRequest).messages();
    }
    private static void startOrUpdateWorkers(int requiredWorkers) {
        //manager is being run on EC2, so localApp should upload both Manager.jar AND worker.jar
        //to predefined s3 buckets and known keys.
        //check if number of workers is as required
        logger.info("start startOrUpdateWorkers");
        int currWorkers = 0;
        try {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(Filter.builder().name("tag:Worker").values("Worker").build(), Filter.builder().name("instance-state-name").values(new String[]{"running", "pending"}).build()).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            //this is the number of workers, not sure if correct
            List<Reservation> reservations = response.reservations();
            for (Reservation r : reservations) {
                currWorkers += r.instances().size();
            }
            logger.info("number of active workers: " + currWorkers + " and required workers: " + requiredWorkers);
        } catch (Ec2Exception e) {
            logger.info(e.awsErrorDetails().errorMessage());
        }

        if (currWorkers < 19 && currWorkers < requiredWorkers) {
            int neededWorkers = requiredWorkers - currWorkers;
            Tag tag = Tag.builder()
                    .key("Worker")
                    .value("Worker")
                    .build();
            TagSpecification tags = TagSpecification.builder().tags(tag).resourceType(ResourceType.INSTANCE).build();

            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T2_MICRO)
                    .imageId(amiId)
                    .maxCount(neededWorkers)
                    .minCount(1)
                    .keyName("ass1")
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::794818403225:instance-profile/ami-dsp211-ass1").build())
                    .securityGroupIds("sg-0630dc054e0184c80")
                    .userData(Base64.getEncoder().encodeToString(getWorkerDataScript().getBytes()))
                    .tagSpecifications(tags)
                    .build();
            try {

                RunInstancesResponse runResponse = ec2.runInstances(runRequest);


                List<Instance> instances = runResponse.instances();
                logger.info("added workers= " + instances.size());

                for (Instance instance : instances) {
                    logger.info(instance.instanceId() + " idinstance");
                    workerIds.add(instance.instanceId());
                }
            } catch (Ec2Exception e) {
                logger.info(e.awsErrorDetails().errorMessage());
            }
        }
        logger.info("finish startOrUpdateWorkers");
    }

    private static String getWorkerDataScript() {
        final String bucket = "dsp211-ass1-jars";
        final String key = "Worker.jar";

        return "#!/bin/bash\n" +
                "wget https://" + bucket + ".s3.amazonaws.com/" + key + " -O " + key + "\n" +
                "java -jar " + key + "\n";

    }

    //send new image task to workers
    private static void queueNewImageTask(String queue, String line, String localId) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queue)
                .messageBody(new_image_task + "$" + localId + "$" + line)
//                .delaySeconds(5)
                .build();
        sqs.sendMessage(sendMessageRequest);
    }

    //receiveDoneOcrTasks
    //receive all finished OCR tasks from workers, create summary file, and upload to s3 for LocalApp
    public static void receiveDoneOcrTasks(int lines, String localId, String bucket, String worker2ManagerQUrl, String manager2LocalQUrl) {
        logger.info("start receiveDoneOcrTasks");
        int lineCount = lines;
        String output = "output" + localId + ".html";
        logger.info("start build summary file");
        int pollNum=10;
        int waitTime=1;
        while (lineCount != 0) {


            List<Message> messages =receiveMessages(worker2ManagerQUrl,pollNum,waitTime);
            /*
            if (messages.isEmpty()) {
                if (pollNum != 1) {
                    pollNum = 1;
                    waitTime =20;
                }
            } else {
                pollNum = 10;
                waitTime = 1;
            }
            */

            //read all done ocr tasks from worker2manager queue and append them all in output file
            for (Message m : messages) {
                String body = m.body();
//                logger.info("manager recieved message from worker: "+body);

                //message from worker should be done_ocr_task$localId$URL$OCR
                String[] splitMessage = body.split("\\$", 4);
                String taskId = splitMessage[1];
                String url = splitMessage[2];
                String ocr = splitMessage[3];
//                logger.info("message split: "+taskId+" "+url+" "+ocr);
//                logger.info("taskID: "+taskId+" localID: "+localId);

                if (taskId.equals(localId)) {
                    logger.info("thread = " + Thread.currentThread().getId() + " added line to output ID = " + taskId);
                    writeToOutput(output, url, ocr);
                    lineCount--;
                    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(worker2ManagerQUrl)
                            .receiptHandle(m.receiptHandle())
                            .build();
                    sqs.deleteMessage(deleteRequest);
                    logger.info("message from worker2ManagerQ deleted");

                }
            }
            try{  //todo
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("finish build summary file");
        //upload output summary file to s3 and send sqs message to localapp
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(output).acl(ObjectCannedACL.PUBLIC_READ)
                        .build(),
                RequestBody.fromFile(Paths.get(output)));
        logger.info("summary file uploaded to s3 by manager");

        SendMessageRequest messageRequest = SendMessageRequest.builder()
                .queueUrl(manager2LocalQUrl)
                .messageBody(done_task + "$" + localId)
//                .delaySeconds(5)
                .build();
        sqs.sendMessage(messageRequest);
        logger.info("send message to manager2LocalQ: " + done_task + "$" + localId);


        //after output is uploaded to s3, delete it since we no longer need it
        File f = new File(output);
        f.delete();
        logger.info("finish receiveDoneOcrTasks");
    }

    private static String createQueueRequestAndGetUrl(String queue) {
        GetQueueUrlRequest getQueueRequest = null;
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queue)
                    .build();
            sqs.createQueue(request);
            getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queue)
                    .build();
        } catch (QueueNameExistsException e) {
            logger.info(e.awsErrorDetails().toString());
        }
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    private static void deleteMessage(String queueUrl, Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }

    private static String getQueueRequestAndGetUrl(String queue) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

//    private static String getQueueRequestAndGetUrlAsync(String queue) {
////        String res=null;
////        try {
//            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
//                    .queueName(queue)
//                    .build();
//         return   sqs.getQueueUrl(getQueueRequest).queueUrl();
////        } catch (InterruptedException | ExecutionException e) {
////            e.printStackTrace();
////        }
////        return res;
//    }

    private static void writeToOutput(String output, String url, String ocr) {
        try {
            FileWriter writer = new FileWriter(output, true);
            writer.write("<p><img src=" + url + "></p>");
            writer.write(ocr + "<br>");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

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
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public static void initLogger(String loggerName) throws IOException {
        FileHandler fileHandler = new FileHandler(loggerName + ".txt");
        fileHandler.setFormatter(new SimpleFormatter());
        logger.setLevel(Level.ALL);
        logger.addHandler(fileHandler);
    }

    private static class HandleWorkerOutput implements Runnable {
        final private String localId;
        final private String bucket;
        final private int numberOfLines;
        final private String worker2ManagerQUrl;
        final private String manager2LocalQUrl;

        public HandleWorkerOutput(String localId, String bucket, int numberOfLines, String worker2ManagerQUrl, String manager2LocalQUrl) {
            this.localId = localId;
            this.bucket = bucket;
            this.numberOfLines = numberOfLines;
            this.worker2ManagerQUrl = worker2ManagerQUrl;
            this.manager2LocalQUrl = manager2LocalQUrl;
        }

        public void run() {
            receiveDoneOcrTasks(numberOfLines, localId, bucket, worker2ManagerQUrl, manager2LocalQUrl);
        }
    }

}