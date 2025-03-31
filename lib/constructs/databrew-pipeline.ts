import * as path from "path";

import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as stepfunctions from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import { Construct } from "constructs";

export interface S3DatasetPathParameter {
  readonly name: string;
  readonly type?: string;
  readonly createColumn?: boolean;
}

export interface DatabrewPipelineProps {
  /**
   * Raw data bucket
   */
  readonly rawDataBucket: s3.IBucket;

  /**
   * Target bucket
   */
  readonly outDataBucket: s3.IBucket;

  /**
   * Key in target bucket for temporary files
   */
  readonly tmpKeyPattern: string;

  /**
   * Final destination in target bucket
   */
  readonly outKeyPattern: string;

  /**
   * Dynamic key of files in dataset
   */
  readonly rawKeyPattern: string;

  /**
   * Path to the parameter to track successful job's execution
   */
  readonly lastExecutionParameterName: string;

  /**
   * Dataset name
   */
  readonly dataset: string;

  /**
   * Definition of dynamic keys
   */
  readonly datasetPathParameters: S3DatasetPathParameter[];

  /**
   * Job name
   */
  readonly job: string;

  /**
   * Job role ARN
   */
  readonly jobRoleArn: string;

  /**
   * Timeout for the job
   */
  readonly timeout?: number;
}

export class DatabrewPipeline extends Construct {
  constructor(scope: Construct, id: string, props: DatabrewPipelineProps) {
    super(scope, id);

    const stack = cdk.Stack.of(this);

    //const { rawDataBucket, keyPattern, pathParameters } = props.dataset.props

    const postExecutionCleanupFn = new NodejsFunction(this, id, {
      runtime: lambda.Runtime.NODEJS_22_X,
      bundling: {
        externalModules: ["@aws-sdk"],
      },
      entry: path.resolve(__dirname, `../../lambdas/databrew-s3-cleanup/src/index.ts`),
    });
    postExecutionCleanupFn.addToRolePolicy(
      new cdk.aws_iam.PolicyStatement({
        actions: ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
        resources: [`${props.outDataBucket.bucketArn}`, `${props.outDataBucket.bucketArn}/*`],
      })
    );

    const postExecutionCleanupTask = new tasks.LambdaInvoke(this, "PostExecutionCleanupTask", {
      lambdaFunction: postExecutionCleanupFn,
      payload: stepfunctions.TaskInput.fromObject({
        destinationBucket: props.outDataBucket.bucketName,
        sourceKey: props.tmpKeyPattern,
        destinationKey: props.outKeyPattern,
      }),
      resultPath: "$.lambdaResult",
    });

    const getLastSuccessfulExecutionTime = new tasks.CallAwsService(this, "GetLastSuccessfulExecutionTime", {
      service: "ssm",
      action: "getParameter",
      parameters: {
        Name: props.lastExecutionParameterName,
        WithDecryption: true,
      },
      iamResources: [`arn:aws:ssm:${stack.region}:${stack.account}:parameter${props.lastExecutionParameterName}`],
      resultPath: "$.lastExecutionTimestamp",
    });

    const prepareTimestampsTask = new stepfunctions.Pass(this, "PrepareTimestampsTask", {
      parameters: {
        "last_execution_timestamp.$": "$.lastExecutionTimestamp.Parameter.Value",
        "current_timestamp.$": "$$.Execution.StartTime",
      },
      resultPath: "$.timestamps",
    });

    const datasetArn = cdk.Arn.format(
      {
        service: "databrew",
        resource: "dataset",
        resourceName: props.dataset,
        region: stack.region,
        account: stack.account,
      },
      stack
    );
    const updateDatasetTask = new tasks.CallAwsService(this, "UpdateDatasetTask", {
      service: "databrew",
      action: "updateDataset",
      parameters: {
        Input: {
          S3InputDefinition: {
            Bucket: props.rawDataBucket.bucketName,
            Key: props.rawKeyPattern,
          },
        },
        Name: props.dataset,
        PathOptions: {
          LastModifiedDateCondition: {
            Expression: "(AFTER :start_date) AND (BEFORE :end_date)",
            ValuesMap: {
              ":start_date": stepfunctions.JsonPath.stringAt("$.timestamps.last_execution_timestamp"),
              ":end_date": stepfunctions.JsonPath.stringAt("$.timestamps.current_timestamp"),
            },
          },
          Parameters: props.datasetPathParameters.reduce(
            (result, param) => ({
              ...result,
              [param.name]: {
                CreateColumn: param.createColumn ?? true,
                Name: param.name,
                Type: param.type ?? "String",
              },
            }),
            {}
          ),
        },
      },
      iamResources: [datasetArn],
      resultPath: "$.updateResult",
    });

    const setLastSuccessfulExecutionTime = new tasks.CallAwsService(this, "SetLastSuccessfulExecutionTime", {
      service: "ssm",
      action: "putParameter",
      parameters: {
        Name: props.lastExecutionParameterName,
        Value: stepfunctions.JsonPath.stringAt("$.timestamps.current_timestamp"),
        Type: "String",
        Overwrite: true,
      },
      iamResources: [`arn:aws:ssm:${stack.region}:${stack.account}:parameter${props.lastExecutionParameterName}`],
      resultPath: "$.setLastExecutionTimeResult",
    });

    const startDatabrewJob = new tasks.GlueDataBrewStartJobRun(this, "StartDatabrewJob", {
      name: props.job,
      resultPath: "$.jobRun",
    });
    const checkJobStatus = new tasks.CallAwsService(this, "CheckJobStatus", {
      service: "databrew",
      action: "describeJobRun",
      parameters: {
        Name: props.job,
        RunId: stepfunctions.JsonPath.stringAt("$.jobRun.RunId"),
      },
      iamResources: [props.jobRoleArn],
      resultPath: "$.jobStatus",
    });
    const waitForJobToComplete = new stepfunctions.Wait(this, "WaitForJobToComplete", {
      time: stepfunctions.WaitTime.duration(cdk.Duration.seconds(30)),
    });
    const jobStatusChoice = new stepfunctions.Choice(this, "JobStatusChoice")
      .when(stepfunctions.Condition.stringEquals("$.jobStatus.State", "STARTING"), waitForJobToComplete)
      .when(stepfunctions.Condition.stringEquals("$.jobStatus.State", "WAITING"), waitForJobToComplete)
      .when(stepfunctions.Condition.stringEquals("$.jobStatus.State", "RUNNING"), waitForJobToComplete)
      .when(stepfunctions.Condition.stringEquals("$.jobStatus.State", "STOPPING"), waitForJobToComplete)
      .when(stepfunctions.Condition.stringEquals("$.jobStatus.State", "SUCCEEDED"), postExecutionCleanupTask)
      .otherwise(
        new stepfunctions.Fail(this, "JobFailed", {
          cause: "DataBrew Job Failed",
        })
      );
    const verifyPostExecutionResult = new stepfunctions.Choice(this, "VerifyPostExecutionResult")
      .when(stepfunctions.Condition.numberEquals("$.lambdaResult.StatusCode", 200), setLastSuccessfulExecutionTime)
      .otherwise(
        new stepfunctions.Fail(this, "LambdaFailed", {
          cause: "Lambda execution failed or returned non-200 status",
        })
      );

    postExecutionCleanupTask.next(verifyPostExecutionResult);

    const definition = stepfunctions.Chain.start(getLastSuccessfulExecutionTime)
      .next(prepareTimestampsTask)
      .next(updateDatasetTask)
      .next(startDatabrewJob)
      .next(waitForJobToComplete)
      .next(checkJobStatus)
      .next(jobStatusChoice);

    new stepfunctions.StateMachine(this, "DatabrewPipelineStateMachine", {
      definitionBody: stepfunctions.DefinitionBody.fromChainable(definition),
      timeout: cdk.Duration.minutes(props.timeout ?? 15),
    });
  }
}
