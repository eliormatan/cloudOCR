import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import net.sourceforge.tess4j.util.LoadLibs;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import java.util.List;

public class Worker {

    private static SqsClient sqs;
    private static final String manager2WorkerQ = "manager2WorkerQ";
    private static final String worker2ManagerQ = "worker2ManagerQ";
    private static final String new_image_task = "new_image_task";
    private static final String done_ocr_task = "done_ocr_task";

    public static void main(String[] args) {

        Region region = Region.US_EAST_1;
        sqs = SqsClient.builder().region(region).build();

        //get m2w queue (created in manager)
        String manager2WorkerQUrl = getQueueRequestAndGetUrl(manager2WorkerQ);
        //get w2m queue (created in manager)
        String worker2ManagerQUrl = getQueueRequestAndGetUrl(worker2ManagerQ);
        LocalApp.printWithColor("get manager2Worker and worker2Manager queues "+manager2WorkerQUrl+" "+worker2ManagerQUrl);


        while(true){
            // Worker gets an image message from an SQS queue
            // receive messages from the queue
            List<Message> messages = receiveMessages(manager2WorkerQUrl);
            for (Message m : messages) {
                LocalApp.printWithColor("worker recieved message from manager: "+m.body());
                String[] bodyArr = m.body().split("$");
                String task=bodyArr[0];
                String localId = bodyArr[1];
                String imageUrl = bodyArr[2];
                LocalApp.printWithColor("message split: "+task+" "+localId+" "+imageUrl);

                //check for 'done task' message
                if (task.equals(new_image_task)) {
                    //apply OCR and get result(image+text/error)
                    String result=applyOCR(imageUrl);
                    LocalApp.printWithColor("ocr result: "+result);

                    //send message to manager with the OCR result
                    sendMessage(worker2ManagerQUrl, done_ocr_task + "$" + localId + "$" + result);
                    LocalApp.printWithColor("send message to manager2LocalQ: "+done_ocr_task + "$" + localId + "$" + result);

                    //delete message
                    deleteMessage(manager2WorkerQUrl, m);
                    LocalApp.printWithColor("message from manager2WorkerQ deleted:" + m);

                }
            }
        }

        /*      IMPORTANT:
        If an exception occurs, then the worker should recover from it, send a message to the manager of the input message that caused the exception together with a short description of the exception, and continue working on the next message.
        If a worker stops working unexpectedly before finishing its work on a message, then some other worker should be able to handle that message.
        */



    }

    private static String getQueueRequestAndGetUrl(String queue) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        return queueUrl;
    }

    private static List<Message> receiveMessages(String queueUrl) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .visibilityTimeout(45)
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

    private static void sendMessage(String queueUrl, String message) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
    }

    private static String applyOCR(String image_url){
        String output=image_url+"$";

        //download image from URL

        URL url = null;
        try {
            url = new URL(image_url);
        } catch (MalformedURLException e) {
            output+=e.getMessage();
        }

        BufferedImage img = null;
        try {
            img = ImageIO.read(url);
        } catch (IOException e) {
            output+=e.getMessage();
        }

        //TESSERACT PART
        //create my OCR reader
        Tesseract reader = new Tesseract();
        File tessDataFolder = LoadLibs.extractTessResources("tessdata");
        reader.setDatapath(tessDataFolder.getPath());
        //perform OCR on image_to_read and write into HTML file result.html
        try {
            //perform OCR
            String OCR_result = reader.doOCR(img);
            output+=OCR_result;
        } catch (TesseractException e) {
            output+=e.getMessage();
        }
        return output;
    }
}
