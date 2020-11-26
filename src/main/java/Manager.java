import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.*;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;

public class Manager {
    private static S3Client s3;
    private static SqsClient sqs;
    private static final String local2ManagerQ = "local2ManagerQ";
    private static final String manager2LocalQ = "manager2LocalQ";
    private static final String manager2WorkerQ = "manager2WorkerQ";
    private static final String worker2ManagerQ = "worker2ManagerQ";
    private static final String new_image_task = "new_image_task";
    private static final String done_ocr_task = "done_ocr_task";


    public static void main (String args[]) {
        //todo: maybe add threads?

        Region region = Region.US_WEST_2;
        sqs = SqsClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();

        //todo: to get the queue that sends new tasks from local app to manager we need to use the same name ,
        // queue access by name, i called it local2ManagerQ

        //local 2 manager queue url = local2ManagerQUrl
        String local2ManagerQUrl = getQueueRequestAndGetUrl(local2ManagerQ);
        //manager 2 local queue url = manager2LocalQUrl
        String manager2LocalQUrl = getQueueRequestAndGetUrl(manager2LocalQ);


        //create queues to send & receive messages from workers
        //createQueueRequestAndGetUrl function is taken from LocalApp
        String manager2WorkerQUrl = createQueueRequestAndGetUrl(manager2WorkerQ);
        String worker2ManagerQUrl = createQueueRequestAndGetUrl(worker2ManagerQ);

        boolean terminate = false;
        String newTaskFileLink;
        String bucket;
        String key;
        String localId;
        while(!terminate){
            //STEP 4: Manager downloads list of images
            //receive messages from localApp
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(local2ManagerQUrl)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            for (Message message : messages) {
                String messageBody = message.body();
                //todo: define how the new task message is sent from local app to the manager,
                // should receive bucket, key, and local app id to access new task file in s3,
                // in case of multiple local apps, each local app will have its own queues,
                // based on its own ID

                //todo: terminate or not? how to receive terminate message

                //todo: split new task messages and extract needed info such as bucket, key, localID

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
                //done with the input file, so we delete it from s3
                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(key).build();
                s3.deleteObject(deleteObjectRequest);

                //STEP 5: Manager creates an SQS message for each URL in the list of images
                //read lines (image links) from input file and create new image tasks for workers out of each line
                int numberOfLines = 0;
                try{
                    BufferedReader reader = new BufferedReader(new FileReader("input"+localId+".txt"));
                    String line = reader.readLine();
                    while (line != null){
                        numberOfLines++;
                        queueNewImageTask(manager2WorkerQUrl, line, localId);
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
                //delete the "new task" message from local2managerQUrl sqs queue
                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(local2ManagerQUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqs.deleteMessage(deleteRequest);

                //STEP 6: Manager bootstraps nodes to process messages
                //todo: start workers and bootstrap them

            }
        }
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

    //todo: add a function that receives done OCR task from workers
    //todo: add a function to merge outputs from workers
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
}