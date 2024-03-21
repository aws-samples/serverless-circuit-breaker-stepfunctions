import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import * as path from "node:path";
import { Runtime, Function } from "aws-cdk-lib/aws-lambda";
import { Queue } from "aws-cdk-lib/aws-sqs";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import {
  Alarm,
  ComparisonOperator,
  MathExpression,
  Metric,
} from "aws-cdk-lib/aws-cloudwatch";
import { Duration } from "aws-cdk-lib";
import * as events from "aws-cdk-lib/aws-events";
import { StateMachine } from "aws-cdk-lib/aws-stepfunctions";
import { SfnStateMachine } from "aws-cdk-lib/aws-events-targets";
import { CircuitBreakerSQSLambda } from "./circuit-breaker/circuit-breaker";

export class ExampleStepFunctionCircuitBreaker extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const { sqsEventSource, errorFunction, sqs } = this.createExampleLambda();
    const errorRateAlarm = this.createAlarms(errorFunction);

    const stateMachine = new CircuitBreakerSQSLambda(this, "circuitBreaker", {
      triggeringFunction: errorFunction,
      sqs,
      sqsEventSource,
      initialBackoffDelay: 10,
      rejitter: true,
    });

    this.createEventBridgeAlarmTrigger(
      errorRateAlarm,
      stateMachine.circuitBreakerStateMachine,
    );
  }

  /**
   * Creates an alarm that triggers when a Lambda error rate has been 80% or Higher for
   * a significant period of time.
   *
   * @param alarmFunction: The function that is alarming
   */
  createAlarms(alarmFunction: Function): Alarm {
    const errorMetric = new Metric({
      namespace: "AWS/Lambda",
      metricName: "Errors",
      dimensionsMap: {
        FunctionName: alarmFunction.functionName,
      },
      period: Duration.seconds(60),
      statistic: "sum",
    });

    const invocationMetric = new Metric({
      namespace: "AWS/Lambda",
      metricName: "Invocations",
      dimensionsMap: {
        FunctionName: alarmFunction.functionName,
      },
      period: Duration.seconds(60),
      statistic: "sum",
    });

    const errorRateMetric = new MathExpression({
      expression: "100*(errorMetric/invocationMetric)",
      usingMetrics: {
        errorMetric,
        invocationMetric,
      },
    });

    return new Alarm(this, "errorRateAlarm", {
      metric: errorRateMetric,
      threshold: 80,
      evaluationPeriods: 1,
      alarmName: "ErrorRateAlarm",
      alarmDescription: "Error Rate of example",
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
    });
  }

  /**
   * Creates a Lambda, that simply fails if an environment variable is set to true.
   * This is purely for testing the functionality of the Step Function.
   */
  createExampleLambda(): {
    errorFunction: Function;
    sqs: Queue;
    sqsEventSource: SqsEventSource;
  } {
    const errorFunction = new NodejsFunction(this, "erroringFunction", {
      entry: path.join(__dirname, "../../example/handler.ts"),
      handler: "handler",
      runtime: Runtime.NODEJS_20_X,
      memorySize: 128,
      environment: {
        SHOULD_ERROR: "false",
      },
    });

    const dlq = new Queue(this, "circuitBreakerTestDLQ", {
      queueName: "circuitBreakerTestDLQ",
    });

    const sqs = new Queue(this, "circuitBreakerTestQueue", {
      queueName: "circuitBreakerTest",
      deadLetterQueue: {
        queue: dlq,
        maxReceiveCount: 15,
      },
    });

    const sqsEventSource = new SqsEventSource(sqs);
    errorFunction.addEventSource(sqsEventSource);

    return { sqsEventSource, errorFunction, sqs };
  }

  /**
   * Creates an EventBridge Trigger when the passed Alarm is triggered.
   * Will fire the CircuitBreaker Statemachine.
   *
   * @param alarm: The Alarm that will trigger the State Machine.
   * @param stateMachine: The Statemachine that will be triggered on the alarm firing.
   */
  createEventBridgeAlarmTrigger(alarm: Alarm, stateMachine: StateMachine) {
    const alarmStateChangeEventRule = new events.Rule(
      this,
      "alarmStateChangeEventRule",
      {
        description: `Triggers the alarm process step function on alarm state change`,
        eventPattern: {
          source: ["aws.cloudwatch"],
          detailType: ["CloudWatch Alarm State Change"],
          resources: [alarm.alarmArn],
          detail: {
            state: {
              value: ["ALARM"],
            },
          },
        },
      },
    );

    alarmStateChangeEventRule.addTarget(new SfnStateMachine(stateMachine));
  }
}
