import { Construct } from "constructs";
import { Function, Runtime } from "aws-cdk-lib/aws-lambda";
import { Queue } from "aws-cdk-lib/aws-sqs";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import {
  Chain,
  Choice,
  Condition,
  DefinitionBody,
  Fail,
  JsonPath,
  Pass,
  StateMachine,
  TaskInput,
  Wait,
  WaitTime,
} from "aws-cdk-lib/aws-stepfunctions";
import {
  CallAwsService,
  LambdaInvoke,
} from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as cdk from "aws-cdk-lib";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import * as path from "node:path";
import { Duration } from "aws-cdk-lib";

export interface CircuitBreakerLambdaProps {
  triggeringFunction: Function;
  sqs: Queue;
  sqsEventSource: SqsEventSource;

  rejitter: boolean;
  rejitterInitialBackoffDelay?: number;
  rejitterDelay?: number;
  initialBackoffDelay?: number;
}

export class CircuitBreakerSQSLambda extends Construct {
  public readonly circuitBreakerStateMachine: StateMachine;

  /**
   * Creates the Step Function that orchestrates the Circuit Breaker.
   *
   * @param scope: The scope of the Construct
   * @param id: The ID of the construct
   * @param props: A series of parameters to configure the circuit breaker.
   */
  constructor(scope: Construct, id: string, props: CircuitBreakerLambdaProps) {
    super(scope, id);

    const { triggeringFunction, sqs, sqsEventSource } = props;
    const defaultTimeout = props.initialBackoffDelay ?? 10;

    const disableMapping = this.toggleEventSourceMapping({
      id: "Disable SQS Event Source Mapping (Break Circuit)",
      eventSource: sqsEventSource,
      lambda: triggeringFunction,
      enabled: false,
    });

    const enabledMapping = this.toggleEventSourceMapping({
      id: "Enable SQS Event Source Mapping (Fix Circuit)",
      eventSource: sqsEventSource,
      lambda: triggeringFunction,
      enabled: true,
    });

    const waitState = new Wait(this, `Wait ${defaultTimeout} Seconds`, {
      time: WaitTime.duration(cdk.Duration.seconds(defaultTimeout)),
    });

    const getTesterMessage = this.performSqsActionTask(sqs);

    const triggerLambda = new LambdaInvoke(
      this,
      "Test Whether service is fixed",
      {
        lambdaFunction: triggeringFunction,
        payload: TaskInput.fromObject({
          Records: [
            {
              body: JsonPath.stringAt("$.Messages[0].Body"),
            },
          ],
        }),
        resultPath: JsonPath.DISCARD,
      },
    );

    // FAILURE ROUTE, EXPONENTIAL BACKOFF / RETRY
    const incrementFailureAndTime = new Pass(
      this,
      "On example Fail, Increment Attempt Counter",
      {
        parameters: {
          Messages: JsonPath.stringAt("$.Messages"),
          Attempt: JsonPath.mathAdd(JsonPath.numberAt("$.Attempt"), 1),
          RetryTime: JsonPath.mathAdd(
            JsonPath.numberAt("$.RetryTime"),
            JsonPath.numberAt("$.RetryTime"),
          ),
        },
      },
    );

    const waitAndRetry = new Wait(this, "Exponential Backoff", {
      time: WaitTime.secondsPath("$.RetryTime"),
    }).next(triggerLambda);

    const exceededRetryChoice = new Choice(this, "Has Exceeded Retry Attempts?")
      .when(Condition.numberLessThan("$.Attempt", 10), waitAndRetry)
      .otherwise(new Fail(this, "Exceeded Retry Attempts"));

    triggerLambda.addCatch(incrementFailureAndTime.next(exceededRetryChoice), {
      resultPath: JsonPath.DISCARD,
    });

    // SUCCESS PATH, REOPEN THE CIRCUIT.
    const deleteMessageSqs = this.deleteSqsTaskAction(sqs);

    let chain = Chain.start(disableMapping)
      .next(waitState)
      .next(getTesterMessage)
      .next(triggerLambda)
      .next(deleteMessageSqs);

    if (props.rejitter) {
      const backoffDelay = props.rejitterInitialBackoffDelay ?? 60;
      const rejitterRange = props.rejitterDelay ?? 120;
      chain = chain.next(
        this.createRejitterLogic(sqs, backoffDelay, rejitterRange),
      );
    }

    chain.next(enabledMapping);

    this.circuitBreakerStateMachine = new StateMachine(
      this,
      "circuitBreakerStateMachine",
      {
        definitionBody: DefinitionBody.fromChainable(chain),
      },
    );
  }

  createRejitterLogic(
    sqs: Queue,
    rejitterInitialBackoffDelay: number,
    rejitterDelay: number,
  ): Chain {
    const messageRejitterLambda = new NodejsFunction(
      this,
      "rejitterMessageLambda",
      {
        entry: path.join(__dirname, "./rejitter-sqs-lambda.ts"),
        handler: "handler",
        runtime: Runtime.NODEJS_20_X,
        memorySize: 128,
        environment: {
          QUEUE_URL: sqs.queueUrl,
          INITIAL_DELAY: rejitterInitialBackoffDelay.toString(),
          JITTER_DELAY: rejitterDelay.toString(),
        },
      },
    );

    sqs.grantSendMessages(messageRejitterLambda);
    const rejitterSqsEventSource = new SqsEventSource(sqs, { enabled: false });
    messageRejitterLambda.addEventSource(rejitterSqsEventSource);

    const enableRejitter = this.toggleEventSourceMapping({
      id: "Rejitter Messages from SQS and ReQueue",
      eventSource: rejitterSqsEventSource,
      lambda: messageRejitterLambda,
      enabled: true,
    });

    const disableRejitter = this.toggleEventSourceMapping({
      id: "Stop Rejitter Messages from SQS and ReQueue",
      eventSource: rejitterSqsEventSource,
      lambda: messageRejitterLambda,
      enabled: false,
    });

    const waitAndDisableJitter = new Wait(this, "Wait 60 Seconds", {
      time: WaitTime.duration(
        cdk.Duration.seconds(rejitterInitialBackoffDelay),
      ),
    }).next(disableRejitter);

    return enableRejitter.next(waitAndDisableJitter);
  }

  /**
   * Calls the 'updateEventSourceMapping' API in order to turn the EventSource either
   * enabled, or disabled
   *
   * @param params: The parameters needed to call the API, including the Lambda that we are triggering.
   */
  toggleEventSourceMapping(params: {
    eventSource: SqsEventSource;
    lambda: Function;
    enabled: boolean;
    id: string;
  }) {
    return new CallAwsService(this, params.id, {
      service: "lambda",
      action: "updateEventSourceMapping",
      parameters: {
        Uuid: params.eventSource.eventSourceMappingId,
        Enabled: params.enabled,
      },
      iamResources: [
        params.eventSource.eventSourceMappingArn,
        params.lambda.functionArn,
      ],
      iamAction: "lambda:UpdateEventSourceMapping",
    });
  }

  performSqsActionTask(sqs: Queue) {
    return new CallAwsService(this, "Get SQS Message to Test Circuit", {
      service: "sqs",
      action: "receiveMessage",
      parameters: {
        MaxNumberOfMessages: 1,
        QueueUrl: sqs.queueUrl,
      },
      iamResources: [sqs.queueArn],
      iamAction: "sqs:receiveMessage",
      resultSelector: {
        "Messages.$": "$.Messages",
        Attempt: 1,
        RetryTime: 10,
      },
    }).addRetry({
      interval: Duration.seconds(10),
      maxAttempts: 5,
      backoffRate: 2,
    });
  }

  deleteSqsTaskAction(sqs: Queue) {
    return new CallAwsService(
      this,
      "Delete Successfully Processed SQS Message",
      {
        service: "sqs",
        action: "deleteMessage",
        parameters: {
          QueueUrl: sqs.queueUrl,
          ReceiptHandle: JsonPath.stringAt("$.Messages[0].ReceiptHandle"),
        },
        iamResources: [sqs.queueArn],
        iamAction: "sqs:deleteMessage",
      },
    );
  }
}
