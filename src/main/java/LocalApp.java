import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.nio.file.Paths;
import java.util.Date;

public class LocalApp {

    private static S3Client s3;
    private static SqsClient sqs;

    public static void main(String[] args) {
        final String input=args[1];
        final Region region = Region.US_WEST_2;
        final String bucket = "bucket" + System.currentTimeMillis();
        final String key = "key";
        final String queue = "queue" + new Date().getTime();
        final String message="new task";

        //define s3
        s3 = S3Client.builder().region(region).build();
        createBucket(bucket, region);

        //define sqs
        sqs=SqsClient.builder().region(region).build();
        String queueUrl=createQueueRequestAndGetUrl(queue);

        //upload the input file to s3
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                Paths.get(input));

        //send 'new task' to the queue (sqs)
        sendMessage(queueUrl,message);

        /* todo: Checking if a Manager Node is Active
        You can check if the manager is running by listing all your running instances, and checking if one of them is a manager.
        Use the "tags" feature of Amazon EC2 API to mark a specific instance as one running a Manager:
        -using tags
        -CreateTagsRequest API
        */

        //todo: Checks an SQS queue for a message indicating the process is done and the response (the summary file) is available on S3.

        //todo: Downloads the summary file from S3, and create an html file representing the results.

        //todo: Sends a termination message to the Manager if it was supplied as one of its input arguments.
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

}
