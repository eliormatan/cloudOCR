import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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
    private static final String done_ocr_task = "done ocr task";
    private static final String new_task = "new task";
    private static final String done_task = "done task";
    private static List<String> workerIds;
    private static List<String> bucketIds;
    public static AtomicInteger requiredWorkers = new AtomicInteger(0);
    public static ConcurrentHashMap<String, Integer> numberOfLinesMap;

    private static Logger logger = Logger.getLogger(Manager.class.getName());


    public static void main (String args[]) throws InterruptedException {
        try {
            initLogger("ManagerLogger");
        } catch (IOException e) {
            e.printStackTrace();
        }
        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
        workerIds=new LinkedList<>();
        bucketIds=new LinkedList<>();
        List<String> manager2localqueues = new LinkedList<>();
        numberOfLinesMap = new ConcurrentHashMap<>();
        Region region = Region.US_EAST_1;
        ec2 = Ec2Client.builder().region(region).build();
        sqs = SqsClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();

        //to get the queue that sends new tasks from local app to manager we need to use the same name ,
        //queue access by name, i called it local2ManagerQ
        //local 2 manager queue url = local2ManagerQUrl
        String local2ManagerQUrl = getQueueRequestAndGetUrl(local2ManagerQ);
        printWithColor("get local2manager"+local2ManagerQUrl);

        //create queues to send & receive messages from workers
        //createQueueRequestAndGetUrl function is taken from LocalApp
        String manager2WorkerQUrl = createQueueRequestAndGetUrl(manager2WorkerQ);
        String worker2ManagerQUrl = createQueueRequestAndGetUrl(worker2ManagerQ);
        printWithColor("created manager2Worker and worker2Manager queues: "+manager2WorkerQUrl+" "+worker2ManagerQUrl);

        boolean terminate = false;
        while(!terminate){
            printWithColor("thread = " + Thread.currentThread().getId() + " in while loop receiving messages");
            //STEP 4: Manager downloads list of images
            //receive messages from localApp queue
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
                printWithColor("manager recieved message from local: "+messageBody);

                //split message and extract needed info
                //message should be "new task$bucket$key$n$localID$terminate"
                String[] splitMessage = messageBody.split("\\$");
                System.out.println(Arrays.toString(splitMessage));
                bucket = splitMessage[1];
                key = splitMessage[2];
                linesPerWorker = Integer.parseInt(splitMessage[4]);
                localId = splitMessage[3];
                manager2localqueues.add(manager2LocalQ + localId);
                if (splitMessage.length > 5 && splitMessage[5].equals("terminate")){
                    terminate = true;
                    printWithColor("TERMINATE == TRUE");
                }
                bucketIds.add(bucket);

//              STEP 5: Manager creates an SQS message for each URL in the list of images
                handleLocalInput handleLocalInput = new handleLocalInput(localId, key, bucket, linesPerWorker, manager2WorkerQUrl, local2ManagerQUrl);
                executor.execute(handleLocalInput);

                //delete the "new task" message from local2managerQUrl sqs queue
                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(local2ManagerQUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqs.deleteMessage(deleteRequest);

//              STEP 6: Manager bootstraps nodes to process messages
//              start workers and bootstrap them
//              in order to start the required amount of workers we have to count the lines first, which means sending all messages to workers first...
//              so we have to wait anyway.
                try {
                    int req=requiredWorkers.get();
                    while (req==0){
                        Thread.sleep(1000);
                        req=requiredWorkers.get();
                    }
                    startOrUpdateWorkers(req);
                    requiredWorkers.set(0);
                } catch (Exception e) {
                    printWithColor(e.getMessage());
                }
                printWithColor("workers boosted");

//              STEP 11: Manager reads all the Workers' messages from SQS and creates one summary file
//              STEP 12: Manager uploads summary file to S3
//              STEP 13: Manager posts an SQS message about summary file
                handleWorkerOutput handleWorkerOutput = new handleWorkerOutput(localId, bucket,numberOfLinesMap.get(localId), worker2ManagerQUrl);
                executor.execute(handleWorkerOutput);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                printWithColor(e.getMessage());
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
        TerminateInstancesResponse termResponse = ec2.terminateInstances(termRequest);
        printWithColor("workers killed");


        //delete queues
        while(!allBucketsAreDeleted()){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        deleteSQSQueue(worker2ManagerQ);
        deleteSQSQueue(manager2WorkerQ);
        printWithColor("worker2Manager and manager2Worker queues deleted");
        deleteSQSQueue(local2ManagerQ);
        //delete all unique manager2local queues
        for(String queue : manager2localqueues){
            deleteSQSQueue(queue);
        }
        printWithColor("local2manager and manager2local queues deleted");

        //terminate Manager using shutdown command

    }

    //handle output from workers runnable class
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

    //handle input from localApp runnable class
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
            printWithColor("thread = "+ Thread.currentThread().getId() + " taking care of new task " + localId);
            String newTaskFileLink;

            //open/create input file and name it according to the localapp ID that sent it
            try {
                File input = new File("input" + localId + ".txt");
                if (!input.exists()) {
                    input.createNewFile();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            //build the input file's link from the received arguments to download from s3
            newTaskFileLink = "https://" + bucket + ".s3.amazonaws.com/" + key;
            //download the file - reference : https://www.baeldung.com/java-download-file
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

            printWithColor("file downloaded from s3");

            //STEP 5: Manager creates an SQS message for each URL in the list of images
            //read lines (image links) from input file and create new image tasks for workers out of each line
            int numberOfLines = 0;
            try {
                BufferedReader reader = new BufferedReader(new FileReader("input" + localId + ".txt"));
                String line = reader.readLine();
                while (line != null) {
                    numberOfLines++;
                    queueNewImageTask(manager2WorkerQUrl, line, localId);
                    printWithColor("sent new image task to manager2Worker queue " + new_image_task + "$" + localId + "$" + line);

                    //onto the next line
                    line = reader.readLine();
                }
                reader.close();
                //done reading all new image task lines, delete the file since we are done with it
                File input = new File("input" + localId + ".txt");
                input.delete();
                ;
            } catch (IOException e) {
                e.printStackTrace();
            }
            //save the number of lines for this specific localApp
            numberOfLinesMap.put(localId, numberOfLines);
            printWithColor("done sending...sent " + numberOfLines + " masseges (new tasks to workers)");

            //delete the "new task" message from local2managerQUrl sqs queue
            //todo: not needed here, let main thread delete the message
//            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
//                    .queueUrl(local2ManagerQUrl)
//                    .receiptHandle(message.receiptHandle())
//                    .build();
//            sqs.deleteMessage(deleteRequest);
//
//            printWithColor("new task in " + local2ManagerQUrl + "queue deleted");

            requiredWorkers.set((int) Math.ceil((double) numberOfLines / linesPerWorker));
            printWithColor("required workers = " + requiredWorkers + " =numberOfLines / linesPerWorker =" + numberOfLines + "/ " + linesPerWorker);
        }
    }

    public static boolean allBucketsAreDeleted() {
        printWithColor("bucket list size "+ s3.listBuckets().buckets().size());
        List<Bucket> buckets = s3.listBuckets().buckets();
        for ( Bucket b : buckets){
            if(bucketIds.contains(b.name())){
                return false;
            }
        }
        return true;
    }

    //todo: why it is create extra workers?
    private static void startOrUpdateWorkers(int requiredWorkers) {
        //manager is being run on EC2, so localApp should upload both Manager.jar AND worker.jar
        //to predefined s3 buckets and known keys.
        //check if number of workers is as required
        int currWorkers=0;
        try {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(Filter.builder().name("tag:Worker").values("Worker").build(),Filter.builder().name("instance-state-name").values(new String[]{"running", "pending"}).build()).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
            //this is the number of workers, not sure if correct
            List<Reservation> reservations=response.reservations();
            List<Instance> instances = null;
            for(Reservation r:reservations){
                currWorkers = r.instances().size();
            }
            printWithColor("number of active workers: "+currWorkers+" and required workers: "+requiredWorkers);
        } catch (Ec2Exception e) {
            printWithColor(e.awsErrorDetails().errorMessage());
        }

        if(currWorkers < 19 && currWorkers < requiredWorkers){
            int neededWorkers = requiredWorkers - currWorkers;
            printWithColor("needed workers= "+neededWorkers+" =required-active = "+requiredWorkers+" - "+currWorkers);
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
            printWithColor("added workers= "+Integer.toString(instances.size()));

            for (int i = 0; i < instances.size(); i++ ){
                printWithColor(instances.get(i).instanceId()+ " idinstance");
                workerIds.add(instances.get(i).instanceId());
            }
            }catch (Ec2Exception e){
                printWithColor(e.awsErrorDetails().errorMessage());
            }
        }

    }

    private static String getWorkerDataScript(){
        final String bucket="dsp211-ass1-jar";
        final String key="Worker.jar";

        String userData =
                //run the file with bash
                "#!/bin/bash\n"+
                        //download Worker jar
                        "wget https://" + bucket + ".s3.amazonaws.com/" + key +" -O " +key+ "\n" +
                        // run Worker
                        "java -jar "+key+"\n";

        return userData;

    }

    //send new image task to workers
    private static void queueNewImageTask(String queue, String line, String localId){
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queue)
                .messageBody(new_image_task + "$" + localId + "$" + line)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(sendMessageRequest);
    }

    //receiveDoneOcrTasks
    //receive all finished OCR tasks from workers, create summary file, and upload to s3 for LocalApp
    public static void receiveDoneOcrTasks(int lines, String localId, String bucket, String worker2ManagerQUrl) {
        //manager 2 local queue url = manager2LocalQUrl
        String manager2LocalQUrl = getQueueRequestAndGetUrl(manager2LocalQ + localId);
        printWithColor("get manager2local "+manager2LocalQUrl);
        int lineCount = lines;
        String output = "output" + localId + ".html";
        while (lineCount != 0) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(worker2ManagerQUrl)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            //read all done ocr tasks from worker2manager queue and append them all in output file
            for (Message m : messages) {
                String body = m.body();
//                printWithColor("manager recieved message from worker: "+body);

                //message from worker should be done_ocr_task$localId$URL$OCR
                String[] splitMessage = body.split("\\$",4);
                String taskId = splitMessage[1];
                String url = splitMessage[2];
                String ocr = splitMessage[3];
//                printWithColor("message split: "+taskId+" "+url+" "+ocr);
//                printWithColor("taskID: "+taskId+" localID: "+localId);

                if (taskId.equals(localId)) {
                    printWithColor("thread = "+Thread.currentThread().getId() + " added line to output ID = "+taskId);
                    writeToOutput(output, url, ocr);
                    lineCount--;
                    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(worker2ManagerQUrl)
                            .receiptHandle(m.receiptHandle())
                            .build();
                    sqs.deleteMessage(deleteRequest);
                    printWithColor("message from worker2ManagerQ deleted");

                }
            }
        }
        //upload output summary file to s3 and send sqs message to localapp
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(output).acl(ObjectCannedACL.PUBLIC_READ)
                        .build(),
                RequestBody.fromFile(Paths.get(output)));
        printWithColor("summary file uploaded to s3 by manager");

        SendMessageRequest messageRequest = SendMessageRequest.builder()
                .queueUrl(manager2LocalQUrl)
                .messageBody(done_task + "$" + localId)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(messageRequest);
        printWithColor("send message to manager2LocalQ: "+done_task + "$" + localId);


        //after output is uploaded to s3, delete it since we no longer need it
        File f = new File(output);
        f.delete();
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
    private static String getQueueRequestAndGetUrl(String queue) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        return queueUrl;
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

    public static void initLogger(String loggerName) throws IOException{
        FileHandler fileHandler = new FileHandler(loggerName + ".txt");
        fileHandler.setFormatter(new SimpleFormatter());
        logger.setLevel(Level.ALL);
        logger.addHandler(fileHandler);
    }

    private static void printWithColor (String string){
        final String ANSI_CYAN = "\u001B[36m";
        final String ANSI_BLACK = "\u001B[30m";
        final String ANSI_RESET = "\u001B[0m";
        final String ANSI_CYAN_BACKGROUND = "\u001B[46m";
        final String ANSI_WHITE_BACKGROUND = "\u001B[47m";
        System.out.println(ANSI_CYAN_BACKGROUND +ANSI_BLACK + string + ANSI_RESET);
        logger.info(string);

    }
}