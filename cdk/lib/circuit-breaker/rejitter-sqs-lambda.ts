import { Context, SQSEvent, SQSRecord } from "aws-lambda";
import { Handler } from "aws-lambda/handler";
import { SQS } from "@aws-sdk/client-sqs";
import { Logger } from "@aws-lambda-powertools/logger";

const client = new SQS();
const logger = new Logger({ serviceName: "rejitter-lambda" });

export const handler: Handler<SQSEvent, void> = async (
  event: SQSEvent,
  _context: Context,
): Promise<void> => {
  const messages = event.Records;

  // Takes the events, clones them, and then re-adds them to the SQS Queue with a Jitter (DelaySeconds)
  return Promise.all(
    messages.map((sqsRecord: SQSRecord) => {
      const message = sqsRecord.body;
      const delay =
        Number(process.env["INITIAL_DELAY"]) +
        Math.floor(Math.random() * (Number(process.env["JITTER_DELAY"]) - 1));
      logger.info(`Delaying messages for ${delay}s`);
      return client.sendMessage({
        QueueUrl: process.env["QUEUE_URL"],
        MessageBody: message,
        DelaySeconds: delay,
      });
    }),
  ).then((_it) => {});
};
