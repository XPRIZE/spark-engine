import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { utils } from "@varius.io/framework";

const sqs = new SQSClient({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
    // LOCAL STACK (see /local-setup.md): points this client at a local fake SQS (ElasticMQ)
    // instead of real AWS. Unset in real deployments - no behavior change there.
    ...(process.env.SQS_ENDPOINT_URL ? { endpoint: process.env.SQS_ENDPOINT_URL } : {}),
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