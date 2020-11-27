import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class Worker {

    private static SqsClient sqs;
    private static final String manager2WorkerQ = "manager2WorkerQ";
    private static final String worker2ManagerQ = "worker2ManagerQ";
    private static final String new_image_task = "new_image_task";
    private static final String done_ocr_task = "done_ocr_task";

    public static void main(String[] args) {

        Region region = Region.US_WEST_2;
        sqs = SqsClient.builder().region(region).build();

        //get m2w queue (created in manager)
        String manager2WorkerQUrl = getQueueRequestAndGetUrl(manager2WorkerQ);
        //get w2m queue (created in manager)
        String worker2ManagerQUrl = getQueueRequestAndGetUrl(worker2ManagerQ);

        while(true){
            // Worker gets an image message from an SQS queue
            // receive messages from the queue
            List<Message> messages = receiveMessages(manager2WorkerQUrl);
            for (Message m : messages) {
                String[] bodyArr = m.body().split("$");
                String task=bodyArr[0];
                String localId = bodyArr[1];
                String imageUrl = bodyArr[2];
                //check for 'done task' message
                if (task.equals(new_image_task)) {
                    //apply OCR and get result(image+text/error)
                    String result=ReadOCR.main(new String[]{imageUrl});

                    //send message to manager with the OCR result
                    sendMessage(worker2ManagerQUrl, done_ocr_task + "$" + localId + "$" + result);

                    //delete message
                    deleteMessage(manager2WorkerQUrl, m);
                }
            }
        }

        /*      IMPORTANT:
        If an exception occurs, then the worker should recover from it, send a message to the manager of the input message that caused the exception together with a short description of the exception, and continue working on the next message.
        If a worker stops working unexpectedly before finishing its work on a message, then some other worker should be able to handle that message.
        */



    }

    //todo: optimize - the function is duplicated from manager  //maybe create sqs package/class
    private static String getQueueRequestAndGetUrl(String queue) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        return queueUrl;
    }

    //todo: optimize - the function is duplicated from localApp
    private static List<Message> receiveMessages(String queueUrl) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .visibilityTimeout(45)
                .build();
        return sqs.receiveMessage(receiveRequest).messages();
    }

    //todo: optimize - the function is duplicated from localApp
    private static void deleteMessage(String queueUrl, Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }

    //todo: optimize - the function is duplicated from localApp
    private static void sendMessage(String queueUrl, String message) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
    }
}
