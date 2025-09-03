import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { utils } from "@varius.io/framework";

const sqs = new SQSClient({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    }
});

export async function putFirehoseEvent(data: any) {
    const queueUrl = process.env.SQS_FIREHOSE_QUEUE_URL;
    if (!queueUrl) {
        utils.logger.error("SQS_FIREHOSE_QUEUE_URL is not set.");
        return;
    }
    try {
        await sqs.send(new SendMessageCommand({
            QueueUrl: queueUrl,
            MessageBody: JSON.stringify(data),
        }));
    } catch (err) {
        utils.logger.error("Error in putFirehoseEvent: ", err);
    }
}