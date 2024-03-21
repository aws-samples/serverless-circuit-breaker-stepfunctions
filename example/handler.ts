import { SQSHandler, SQSEvent, Context } from "aws-lambda";
import { Logger } from "@aws-lambda-powertools/logger";

const logger = new Logger({ serviceName: "example-failure-lambda" });

export const handler: SQSHandler = async (
  event: SQSEvent,
  _context: Context,
): Promise<void> => {
  const shouldError = process.env["SHOULD_ERROR"] === "true";
  logger.info(`Deciding on whether to fail event ${JSON.stringify(event)}`);

  if (shouldError) {
    logger.error(`Purposely failing Lambda due to Environment Variable.`);
    throw new Error("Purposely Error");
  }

  logger.info(`Completed successfully`);
  return;
};
