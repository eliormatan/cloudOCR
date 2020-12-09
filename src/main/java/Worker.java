import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

public class Worker {

    private static SqsClient sqs;
    private static final String manager2WorkerQ = "manager2WorkerQ";
    private static final String worker2ManagerQ = "worker2ManagerQ";
    private static final String new_image_task = "new image task";
    private static final String done_ocr_task = "done ocr task";



    public static void main(String[] args) {

        Region region = Region.US_EAST_1;
        sqs = SqsClient.builder().region(region).build();

        String manager2WorkerQUrl = getQueueRequestAndGetUrl(manager2WorkerQ);
        String worker2ManagerQUrl = getQueueRequestAndGetUrl(worker2ManagerQ);
        while(true){
            List<Message> messages = receiveMessages(manager2WorkerQUrl);
            for (Message m : messages) {
                String[] bodyArr = m.body().split("\\$");
                String task=bodyArr[0];
                String localId = bodyArr[1];
                String imageUrl = bodyArr[2];
                if (task.equals(new_image_task)) {
                    String result=applyOCR(imageUrl);

                    sendMessage(worker2ManagerQUrl, done_ocr_task + "$" + localId + "$" + result);
                    deleteMessage(manager2WorkerQUrl, m);

                }
            }
        }
    }

    private static String getQueueRequestAndGetUrl(String queue) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
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

        Tesseract reader = new Tesseract();


        reader.setDatapath("/usr/share/tesseract");
        try {
            String OCR_result = reader.doOCR(img);
            output+=OCR_result;
        } catch (TesseractException e) {
            output+=e.getMessage();
        }
        return output;
    }


}
