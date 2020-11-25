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

import java.io.IOException;
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

    public static void main (String args[]) {
        //todo: maybe add threads?

        Region region = Region.US_WEST_2;
        sqs = SqsClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();

        //todo: to get the queue that sends new tasks from local app to manager we need to use the same name
        //queue access by name, i called it local2ManagerQ

        //local 2 manager queue url = local2ManagerQUrl
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(local2ManagerQ)
                .build();
        String local2ManagerQUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

        //manager 2 local queue url = manager2LocalQUrl
        getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(manager2LocalQ)
                .build();
        String manager2LocalQUrl = sqs.getQueueUrl((getQueueRequest)).queueUrl();

        //create queues to send & receive messages from workers
        //todo: createQueueRequestAndGetUrl function is taken from LocalApp, is it shared or do we have to copy code?
        String manager2WorkerQUrl = createQueueRequestAndGetUrl(manager2WorkerQ);
        String worker2ManagerQUrl = createQueueRequestAndGetUrl(worker2ManagerQ);

        boolean terminate = false;
        String newTaskFileLink;
        String bucket;
        String key;
        String localId;
        while(!terminate){
            //receive messages from localApp
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(manager2LocalQUrl)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            for (Message message : messages) {
                String messageBody = message.body();
                //todo: define how the new task message is sent from local app to the manager
                //should receive bucket, key, and local app id to access new task file in s3
                //in case of multiple local apps, each local app will have its own queues
                //based on its own ID

                //todo: terminate or not?

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

                //todo: the rest
            }
        }
    }
    //todo: add a function to send new image task to workers
    //todo: add a function that receives done OCR task from workers
    //todo: add a function to merge outputs from workers
}