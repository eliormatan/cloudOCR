import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager {
    private static S3Client s3;
    private static SqsClient sqs;
    private static Ec2Client ec2;
    private static final String amiId = "ami-068dc7ca584573afe";
    private static final String local2ManagerQ = "local2ManagerQ";
    private static final String manager2LocalQ = "manager2LocalQ";
    private static final String manager2WorkerQ = "manager2WorkerQ";
    private static final String worker2ManagerQ = "worker2ManagerQ";
    private static final String new_image_task = "new image task";
    private static final String done_task = "done task";
    private static List<String> workerIds;
    private static List<String> bucketIds;
    public static AtomicInteger requiredWorkers;
    public static ConcurrentHashMap<String, Integer> numberOfLinesMap;
    public static List<String> manager2localqueues;



    public static void main (String[] args) {
        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        workerIds=new LinkedList<>();
        bucketIds=new LinkedList<>();
        numberOfLinesMap = new ConcurrentHashMap<>();
        manager2localqueues = new LinkedList<>();
        requiredWorkers = new AtomicInteger(0);
        Region region = Region.US_EAST_1;
        ec2 = Ec2Client.builder().region(region).build();
        sqs = SqsClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();


        String local2ManagerQUrl = getQueueRequestAndGetUrl(local2ManagerQ);

        String manager2WorkerQUrl = createQueueRequestAndGetUrl(manager2WorkerQ);
        String worker2ManagerQUrl = createQueueRequestAndGetUrl(worker2ManagerQ);

        boolean terminate = false;
        while(!terminate){
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(local2ManagerQUrl)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            for (Message message : messages) {
                int linesPerWorker;
                String localId;
                String key;
                String bucket;
                String messageBody = message.body();

                String[] splitMessage = messageBody.split("\\$");
                System.out.println(Arrays.toString(splitMessage));
                bucket = splitMessage[1];
                key = splitMessage[2];
                linesPerWorker = Integer.parseInt(splitMessage[4]);
                localId = splitMessage[3];
                if (splitMessage.length > 5 && splitMessage[5].equals("terminate")){
                    terminate = true;
                }
                bucketIds.add(bucket);
                manager2localqueues.add(manager2LocalQ + localId);

                handleLocalInput handleLocalInput = new handleLocalInput(localId, key, bucket, linesPerWorker, manager2WorkerQUrl, local2ManagerQUrl);
                executor.execute(handleLocalInput);

                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(local2ManagerQUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqs.deleteMessage(deleteRequest);

                try {
                    int req=requiredWorkers.get();
                    while (req==0){
                        req=requiredWorkers.get();
                    }
                    startOrUpdateWorkers(req);
                    requiredWorkers.set(0);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                handleWorkerOutput handleWorkerOutput = new handleWorkerOutput(localId, bucket,numberOfLinesMap.get(localId), worker2ManagerQUrl);
                executor.execute(handleWorkerOutput);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        TerminateInstancesRequest termRequest = TerminateInstancesRequest.builder().instanceIds(workerIds).build();
        ec2.terminateInstances(termRequest);

        while(!allBucketsAreDeleted()){
        }
        deleteSQSQueue(worker2ManagerQ);
        deleteSQSQueue(manager2WorkerQ);
        deleteSQSQueue(local2ManagerQ);
        for(String queue : manager2localqueues){
            deleteSQSQueue(queue);
        }
    }

    public static class handleWorkerOutput implements Runnable{
        String localId;
        String bucket;
        int numberOfLines;
        String worker2ManagerQUrl;
        public handleWorkerOutput(String localId, String bucket, int numberOfLines, String worker2ManagerQUrl){
            this.localId = localId;
            this.bucket = bucket;
            this.numberOfLines = numberOfLines;
            this.worker2ManagerQUrl = worker2ManagerQUrl;
        }
        public void run(){
            receiveDoneOcrTasks(numberOfLines, localId, bucket, worker2ManagerQUrl);
        }
    }

    public static class handleLocalInput implements Runnable{
        String localId;
        String key;
        String bucket;
        int linesPerWorker;
        String manager2WorkerQUrl;
        String local2ManagerQUrl;

        public handleLocalInput(String localId, String key, String bucket, int linesPerWorker, String manager2WorkerQUrl, String local2ManagerQUrl){
            this.localId = localId;
            this.key = key;
            this.bucket = bucket;
            this.linesPerWorker = linesPerWorker;
            this.manager2WorkerQUrl = manager2WorkerQUrl;
            this.local2ManagerQUrl = local2ManagerQUrl;
        }
        public void run(){
            String newTaskFileLink;

            try {
                File input = new File("input" + localId + ".txt");
                if (!input.exists()) {
                    input.createNewFile();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            newTaskFileLink = "https://" + bucket + ".s3.amazonaws.com/" + key;
            try (BufferedInputStream in = new BufferedInputStream(new URL(newTaskFileLink).openStream());
                 FileOutputStream fileOutputStream = new FileOutputStream("input" + localId + ".txt")) {
                byte[] dataBuffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                    fileOutputStream.write(dataBuffer, 0, bytesRead);
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
            int numberOfLines = 0;
            try {
                BufferedReader reader = new BufferedReader(new FileReader("input" + localId + ".txt"));
                String line = reader.readLine();
                while (line != null) {
                    numberOfLines++;
                    queueNewImageTask(manager2WorkerQUrl, line, localId);
                    line = reader.readLine();
                }
                reader.close();
                File input = new File("input" + localId + ".txt");
                input.delete();
            } catch (IOException e) {
                e.printStackTrace();
            }
            numberOfLinesMap.put(localId, numberOfLines);
            requiredWorkers.set((int) Math.ceil((double) numberOfLines / linesPerWorker));
        }
    }

    public static boolean allBucketsAreDeleted() {
        List<Bucket> buckets = s3.listBuckets().buckets();
        for ( Bucket b : buckets){
            if(bucketIds.contains(b.name())){
                return false;
            }
        }
        return true;
    }

    private static void startOrUpdateWorkers(int requiredWorkers) {
        int currWorkers=0;
        try {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(Filter.builder().name("tag:Worker").values("Worker").build(),Filter.builder().name("instance-state-name").values(new String[]{"running", "pending"}).build()).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            List<Reservation> reservations=response.reservations();
            for(Reservation r:reservations){
                currWorkers = r.instances().size();
            }
        } catch (Ec2Exception e) {
            System.out.println(e.awsErrorDetails().errorMessage());
        }

        if(currWorkers < 19 && currWorkers < requiredWorkers){
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
                    .keyName("dspass1")
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::320131450129:instance-profile/dspass1").build())
                    .securityGroupIds("sg-0eead8b108fc9f860")
                    .userData(Base64.getEncoder().encodeToString(getWorkerDataScript().getBytes()))
                    .tagSpecifications(tags)
                    .build();
            try {

                RunInstancesResponse runResponse = ec2.runInstances(runRequest);


            List<Instance> instances = runResponse.instances();

                for (Instance instance : instances) {
                    workerIds.add(instance.instanceId());
                }
            }catch (Ec2Exception e){
                System.out.println(e.awsErrorDetails().errorMessage());
            }
        }

    }

    private static String getWorkerDataScript(){
        final String bucket="dsp211-ass1-jar";
        final String key="Worker.jar";

        return "#!/bin/bash\n"+
                "wget https://" + bucket + ".s3.amazonaws.com/" + key +" -O " +key+ "\n" +
                "java -jar "+key+"\n";
    }

    private static void queueNewImageTask(String queue, String line, String localId){
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queue)
                .messageBody(new_image_task + "$" + localId + "$" + line)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(sendMessageRequest);
    }


    public static void receiveDoneOcrTasks(int lines, String localId, String bucket, String worker2ManagerQUrl) {
        String manager2LocalQUrl = getQueueRequestAndGetUrl(manager2LocalQ + localId);
        int lineCount = lines;
        String output = "output" + localId + ".html";
        while (lineCount != 0) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(worker2ManagerQUrl)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            for (Message m : messages) {
                String body = m.body();
                String[] splitMessage = body.split("\\$",4);
                String taskId = splitMessage[1];
                String url = splitMessage[2];
                String ocr = splitMessage[3];

                if (taskId.equals(localId)) {
                    writeToOutput(output, url, ocr);
                    lineCount--;
                    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(worker2ManagerQUrl)
                            .receiptHandle(m.receiptHandle())
                            .build();
                    sqs.deleteMessage(deleteRequest);
                }
            }
        }
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(output).acl(ObjectCannedACL.PUBLIC_READ)
                        .build(),
                RequestBody.fromFile(Paths.get(output)));

        SendMessageRequest messageRequest = SendMessageRequest.builder()
                .queueUrl(manager2LocalQUrl)
                .messageBody(done_task + "$" + localId)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(messageRequest);

        File f = new File(output);
        f.delete();
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

    private static String getQueueRequestAndGetUrl(String queue) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    private static void writeToOutput(String output, String url, String ocr){
        try {
            FileWriter writer = new FileWriter(output,true);
            writer.write("<p><img src=" + url + "></p>");
            writer.write(ocr+"<br>");
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
}