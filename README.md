# cloudOCR

## Abstract 
We built a distributed system, which shows by the three different components running on different machines, communicating and coordinating their actions through messages.
This application perform OCR on given images in the cloud using AWS EC2 SQS S3 etc.
The application will get as an input a text file containing a list of URLs of images. Then, instances will be launched in AWS (workers). Each worker will download image files, use some OCR library to identify text in those images (if any) and display the image with the text in a webpage.

## How to run our project: 
### Prerequisites:
An already existing bucket with Manager.jar and Worker.jar uploaded to s3, as well as aws credentials and the input file with list of URLs of images. 
### To run:
java -jar LocalApp.jar inputFileName outputFileName n optional:terminate

## Workflow: 
Once the Local App is started on your machine, it uploads your input file to a unique bucket in s3, and sends an sqs message to the Manager indicating that there is a new task waiting for him, with the bucket and input file name. Then, the local app checks whether a manager is running (and starts it if not) and waits for a response message from the manager. The Manager starts a thread pool, and listens to an sqs queue for messages from local apps (multiple local apps are supported) until he receives a message with terminate. Once the manager receives a message from local app, he sends the tasks to the workers. The manager creates a runnable task that handles returning done image ocr tasks from workers, and creates the summary file, and executes it with the executor. The manager handles input from local by sending individual messages for each URL in the input file to the workers queue, and collects the workers’ results. The workers run in an endless loop, handling tasks received from the manager.

## Project specifications:
AMI: ami-068dc7ca584573afe We created this AMI, and installed java 1.8, tesseract and AWS CLI. Instance type: T2.micro.

## Security: 
The manager and all of the workers get temporary credentials from their IAM role. Therefore, we transfer credentials in a secure way.

## Scalability: 
With the way our project is structured, having only one manager is a drawback ,and a possible bottleneck on scalability. Even though the manager’s tasks are simple, there is a heavy load on him depending on the number of local apps and input. However, if we run the manager on a powerful machine in the cloud, with enough power to handle a huge amount of local apps, we are still bottlenecked (as student users) by the total amount of ec2 instances we can run, which is only 20. If we had access to more resources to create ec2 instances (managers and workers), our program is scalable, because we reduce the load off of the manager by dividing the work to the workers, and letting them do the heavy lifting, as well as using threadpool in our manager implementation, which allows us to receive multiple tasks from clients and divide their work to the workers simultaneously, and receiving done tasks from workers at the same time. Also, we created unique queues for each local app to receive its summary file, to avoid race conditions.

## Persistence: 
In our implementation we take advantage of the SQS features :
1. Visibility timeout: received messages that are not completed in time (depending on the task at hand) return to the queue for someone else responsibility. For example: if a worker does not finish his job, the task message he received will return to the sqs queue, and another worker will receive it.
2. Long polling: we used long polling when receiving messages to improve responsiveness of communication (receive message requests)

## Multithreading:
We used multithreading in our manager, in order to send and receive tasks independently, which helps relieve some load on communication, and speeds up the manager’s work.

## Termination: 
When the manager receives a termination message, it no longer accepts any input from new clients, waits for the workers to finish their jobs, and returns results to the clients he is serving. After all his workers finish their jobs, he terminates them, and waits for clients to receive their summary files, then terminates itself by shutdown command. All bucket and queues created during the program’s runtime are deleted before the shutdown command.


