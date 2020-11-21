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

public class LocalApp {

    private static S3Client s3;
    private static SqsClient sqs;

    public static void main(String[] args) {
        final String input=args[1];
        final Region region = Region.US_WEST_2;
        final String bucket = "bucket_ocr";
        final String key = "key_ocr";
        final String queue = "queue" + new Date().getTime();
        final String new_task="new task";
        final String done_task="done task";
        boolean done=false;

            try {
                //define s3
                s3 = S3Client.builder().region(region).build();
                createBucket(bucket, region);

                //define sqs
                sqs = SqsClient.builder().region(region).build();
                String queueUrl = createQueueRequestAndGetUrl(queue);

                //upload the input file to s3
                s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                        Paths.get(input)); //todo: check if second parameter is fine

                //get s3 location of the input file
                //todo: how to get the path in s3???
                /*ResponseInputStream<GetObjectResponse> obj= s3.getObject(GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build());*/


                //send the location of the file in s3 to the queue (using $ as a delimiter)
                sendMessage(queueUrl, new_task + "$" + inputS3Path);

        /* todo: Checking if a Manager Node is Active
        You can check if the manager is running by listing all your running instances, and checking if one of them is a manager.
        Use the "tags" feature of Amazon EC2 API to mark a specific instance as one running a Manager:
        -using tags
        -CreateTagsRequest API
        */

                while (!done) {
                    //todo: Checks an SQS queue for a message indicating the process is done and the response (the summary file) is available on S3.

                    // receive messages from the queue
                    List<Message> messages = receiveMessages(queueUrl);
                    for (Message m : messages) {
                        String[] bodyArr = m.body().split("$");
                        //check for 'done task' message
                        if (bodyArr[0].equals(done_task)) {
                            //delete message
                            deleteMessage(queueUrl, m);
                            //get s3 location of the output file
                            String outputS3Path = bodyArr[1];
                            done = true;
                        }
                    }


                }

                //todo: Downloads the summary file from S3, and create an html file representing the results.

                //todo: Sends a termination message to the Manager if it was supplied as one of its input arguments.
            } catch (IOException e) {
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

    private static void sendMessage(String queueUrl,String message) {
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

    private static void deleteMessage(String queueUrl,Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }





}
