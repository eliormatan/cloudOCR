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
import java.lang.reflect.Array;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Manager {
    private static S3Client s3;
    private static SqsClient sqs;
    private static Ec2Client ec2;
    private static final String amiId = "ami-076515f20540e6e0b";
    private static final String local2ManagerQ = "local2ManagerQ";
    private static final String manager2LocalQ = "manager2LocalQ";
    private static final String manager2WorkerQ = "manager2WorkerQ";
    private static final String worker2ManagerQ = "worker2ManagerQ";
    private static final String new_image_task = "new image task";
    private static final String done_ocr_task = "done ocr task";
    private static final String new_task = "new task";
    private static final String done_task = "done task";
    private static List<String> workerIds;

    private static Logger logger = Logger.getLogger(Manager.class.getName());


    public static void main (String args[]){
        try {
            initLogger("ManagerLogger");
        } catch (IOException e) {
            e.printStackTrace();
        }


        //todo: maybe add threads?

        Region region = Region.US_EAST_1;
        sqs = SqsClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();

        //to get the queue that sends new tasks from local app to manager we need to use the same name ,
        //queue access by name, i called it local2ManagerQ
        //local 2 manager queue url = local2ManagerQUrl
        String local2ManagerQUrl = getQueueRequestAndGetUrl(local2ManagerQ);

        //manager 2 local queue url = manager2LocalQUrl
        String manager2LocalQUrl = getQueueRequestAndGetUrl(manager2LocalQ);
        printWithColor("get loca2manager and manager2local queues "+local2ManagerQUrl+" "+manager2LocalQUrl);

        //create queues to send & receive messages from workers
        //createQueueRequestAndGetUrl function is taken from LocalApp
        String manager2WorkerQUrl = createQueueRequestAndGetUrl(manager2WorkerQ);
        String worker2ManagerQUrl = createQueueRequestAndGetUrl(worker2ManagerQ);
        printWithColor("created manager2Worker and worker2Manager queues: "+manager2WorkerQUrl+" "+worker2ManagerQUrl);

        String newTaskFileLink;
        String bucket;
        String key;
        int linesPerWorker;
        int requiredWorkers;
        String localId;
        boolean terminate = false;
        while(!terminate){
            //STEP 4: Manager downloads list of images
            //receive messages from localApp queue
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(local2ManagerQUrl)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            for (Message message : messages) {
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
                printWithColor("message split: "+bucket+" "+key+" "+linesPerWorker+" "+localId);
                if (splitMessage.length > 5 && splitMessage[5] == "terminate"){
                    terminate = true;
                }

                //open/create input file and name it according to the localapp ID that sent it
                try {
                    File input = new File("input" + localId + ".txt");
                    if (!input.exists()) {
                        input.createNewFile();
                    }
                } catch(IOException e){
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

                //done with the input file, so we delete it from s3
                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(key).build();
                s3.deleteObject(deleteObjectRequest);

                printWithColor("file deleted from s3");


                //STEP 5: Manager creates an SQS message for each URL in the list of images
                //read lines (image links) from input file and create new image tasks for workers out of each line
                int numberOfLines = 0;
                try{
                    BufferedReader reader = new BufferedReader(new FileReader("input"+localId+".txt"));
                    String line = reader.readLine();
                    while (line != null){
                        numberOfLines++;
                        queueNewImageTask(manager2WorkerQUrl, line, localId);
                        printWithColor("sent new image task to manager2Worker queue "+new_image_task + "$" + localId + "$" + line);

                        //onto the next line
                        line = reader.readLine();
                    }
                    reader.close();
                    //done reading all new image task lines, delete the file since we are done with it
                    File input = new File("input"+localId+".txt");
                    input.delete();;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                printWithColor("done sending...sent "+numberOfLines+" masseges (new tasks to workers)");

                //delete the "new task" message from local2managerQUrl sqs queue
                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(local2ManagerQUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqs.deleteMessage(deleteRequest);

                printWithColor("new task in "+local2ManagerQUrl+"queue deleted");

                requiredWorkers = (int) Math.ceil((double)numberOfLines / linesPerWorker);
                printWithColor("required workers = "+requiredWorkers+" =numberOfLines / linesPerWorker =" +numberOfLines+ "/ " + linesPerWorker);

//                STEP 6: Manager bootstraps nodes to process messages
//                start workers and bootstrap them
                //todo:: uncomment
                //startOrUpdateWorkers(requiredWorkers);
                //printWithColor("workers boosted");
//
//
//                STEP 11: Manager reads all the Workers' messages from SQS and creates one summary file
//                STEP 12: Manager uploads summary file to S3
//                STEP 13: Manager posts an SQS message about summary file
                receiveDoneOcrTasks(numberOfLines, localId, bucket,worker2ManagerQUrl);

            }
        }
        //terminate workers

        TerminateInstancesRequest termRequest = TerminateInstancesRequest.builder().instanceIds(workerIds).build();
        TerminateInstancesResponse termResponse = ec2.terminateInstances(termRequest);
        printWithColor("workers killed");


        //delete queues
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(worker2ManagerQUrl).build()); //worker2manager
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(manager2WorkerQUrl).build()); //manager2worker
        printWithColor("worker2Manager and manager2Worker queues deleted");

        //terminate Manager
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(Filter.builder().name("tag:Manager").build()).build();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        String managerId = response.reservations().get(0).instances().get(0).instanceId();
        printWithColor("managerId(supposed to be not null): "+managerId);
        TerminateInstancesRequest termManagerRequest = TerminateInstancesRequest.builder().instanceIds(managerId).build();
        TerminateInstancesResponse termManagerResponse = ec2.terminateInstances(termManagerRequest);
        printWithColor("manager killed himself!");


    }

    private static void startOrUpdateWorkers(int requiredWorkers) {
        //manager is being run on EC2, so localApp should upload both Manager.jar AND worker.jar
        //to predefined s3 buckets and known keys.
        //check if number of workers is as required
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(Filter.builder().name("tag:Worker").build()).build();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        //this is the number of workers, not sure if correct
        int currWorkers = (int) response.reservations().stream().count();
        printWithColor("number of active workers: "+currWorkers+" and required workers: "+requiredWorkers);

        if(currWorkers < 19 && currWorkers < requiredWorkers){
            int neededWorkers = requiredWorkers - currWorkers;
            printWithColor("needed workers= "+neededWorkers+" =required-active = "+requiredWorkers+" - "+currWorkers);
            ec2 = Ec2Client.create();
            Tag tag = Tag.builder()
                    .key("Worker")
                    .value("Worker")
                    .build();
            TagSpecification tags = TagSpecification.builder().tags(tag).build();

            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T2_MICRO)
                    .imageId(amiId)
                    .maxCount(neededWorkers)
                    .minCount(1)
                    .userData(Base64.getEncoder().encodeToString(getWorkerDataScript().getBytes()))
                    .tagSpecifications(tags)
                    .build();


            RunInstancesResponse runResponse = ec2.runInstances(runRequest);

            List<Instance> instances = runResponse.instances();
            printWithColor("added workes= "+Integer.toString(instances.size()-1));

            for (int i = 0; i < instances.size(); i++ ){
                workerIds.add(instances.get(i).instanceId());
            }
        }

    }

    private static String getWorkerDataScript(){
        String workerData=
                "#!/bin/bash\n"+
                "echo installing maven\n" +
                "sudo apt-get install maven\n"+
                "mvn -version" +
                "echo download jar file\r\n" +
                //todo: upload jar
                //we can upload jar files to s3 and download them using wget like so:
                // wget "https://<bucket>.s3.amazonaws.com/<key>
                "echo running Worker\r\n" +
                "java -jar Worker.jar";
        return workerData;
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
    //receive a finished OCR task from a worker
    public static void receiveDoneOcrTasks(int lines, String localId, String bucket, String qURL) {
        int lineCount = lines;
        String output = "output" + localId + ".html";
        while (lineCount != 0) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(qURL)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            //read all done ocr tasks from worker2manager queue and append them all in output file
            for (Message m : messages) {
                String body = m.body();
                printWithColor("manager recieved message from worker: "+body);

                //message from worker should be done_ocr_task$localId$URL$OCR
                String[] splitMessage = body.split("\\$",3);
                String taskId = splitMessage[1];
                String url = splitMessage[2];
                String ocr = splitMessage[3];
                //todo: check split why index out of bound splitMessage[3]?
                printWithColor("message split: "+taskId+" "+url+" "+ocr);
                printWithColor("taskID: "+taskId+"localID: "+localId);

                if (taskId.equals(localId)) {
                    writeToOutput(output, url, ocr);
                    printWithColor("add line to output");
                    lineCount--;
                    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(qURL)
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
        printWithColor("summary file uploded to s3 by manager");

        SendMessageRequest messageRequest = SendMessageRequest.builder()
                .queueUrl(qURL)
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
            writer.write("<img src=" + url + ">");
            writer.write(ocr);
        } catch (IOException e) {
            e.printStackTrace();
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