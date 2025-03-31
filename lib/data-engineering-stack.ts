import * as cdk from "aws-cdk-lib";
import * as databrew from "aws-cdk-lib/aws-databrew";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";

import { cleanMergeWeatherStationsSteps } from "./constructs/clean-merge-recipe-steps";
import { DatabrewPipeline } from "./constructs/databrew-pipeline";
import { DatabrewRecipe } from "./constructs/databrew-recipe";

export interface DataEngineeringStackProps extends cdk.StackProps {
  readonly lastExecutionParameterName: string;
}

export class DataEngineeringStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: DataEngineeringStackProps) {
    super(scope, id, props);

    const rawDataBucketArn = "arn:aws:s3:::weather-stations-raw-data"; // Replace with your actual bucket ARN
    const rawDataBucket = s3.Bucket.fromBucketArn(this, `raw-data-bucket`, rawDataBucketArn);
    const cleanDataBucketArn = "arn:aws:s3:::weather-stations-clean-data"; // Replace with your actual bucket ARN
    const cleanDataBucket = s3.Bucket.fromBucketArn(this, `clean-data-bucket`, cleanDataBucketArn);

    const weatherStationsDataset = new databrew.CfnDataset(this, "Dataset", {
      name: "WeatherStationsDataset",
      format: "CSV",
      input: {
        s3InputDefinition: {
          bucket: rawDataBucket.bucketName,
          key: "data/{sensor_id}/{ingestion_date}/<.*>.csv",
        },
      },
      pathOptions: {
        lastModifiedDateCondition: {
          expression: "(AFTER :start_date) AND (BEFORE :end_date)",
          valuesMap: [
            {
              valueReference: ":start_date",
              value: "2025-01-30T01:00:00Z",
            },
            {
              valueReference: ":end_date",
              value: "2025-03-30T01:00:00Z",
            },
          ],
        },
        parameters: [
          {
            pathParameterName: "sensor_id",
            datasetParameter: {
              createColumn: true,
              name: "sensor_id",
              type: "String",
            },
          },
          {
            pathParameterName: "ingestion_date",
            datasetParameter: {
              createColumn: false,
              name: "ingestion_date",
              type: "String",
            },
          },
        ],
      },
    });

    const cleanMergeRecipe = new DatabrewRecipe(this, "CleanMergeRecipe", {
      name: "CleanMergeWeatherStationsData",
      description: "Clean, transform and merge weather-stations results (v2).",
      tags: {
        Project: "WeatherStations",
      },
      steps: cleanMergeWeatherStationsSteps,
    });

    const jobRole = new iam.Role(this, "JobRole", {
      assumedBy: new iam.ServicePrincipal("databrew.amazonaws.com"),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueDataBrewServiceRole")],
    });
    rawDataBucket.grantRead(jobRole);
    cleanDataBucket.grantReadWrite(jobRole);

    const cleanMergeOnDemandJob = new databrew.CfnJob(this, "CleanMergeOnDemandJob", {
      name: "CleanMergeWeatherStationsDataJob",
      type: "RECIPE",
      datasetName: weatherStationsDataset.name,
      roleArn: jobRole.roleArn,
      timeout: 300,
      maxCapacity: 2,
      recipe: {
        name: cleanMergeRecipe.recipe.name,
        version: "LATEST_PUBLISHED",
      },
      outputs: [
        {
          location: {
            bucket: cleanDataBucket.bucketName,
            key: "tmp",
          },
          partitionColumns: ["measurement_date"],
          maxOutputFiles: 1,
        },
      ],
    });
    const cleanMergeOnDemandJobArn = cdk.Arn.format(
      {
        service: "databrew",
        resource: "job",
        resourceName: cleanMergeOnDemandJob.name,
        region: this.region,
        account: this.account,
      },
      this
    );
    cleanMergeOnDemandJob.addDependency(cleanMergeRecipe.recipe);
    cleanMergeOnDemandJob.addDependency(weatherStationsDataset);
    cleanMergeOnDemandJob.node.addDependency(cleanMergeRecipe.publisher);

    new DatabrewPipeline(this, "CleanMergeWeatherStationsDataPipeline", {
      outDataBucket: cleanDataBucket,
      tmpKeyPattern: "tmp",
      outKeyPattern: "data",
      rawDataBucket,
      rawKeyPattern: "data/{sensor_id}/{ingestion_date}/<.*>.csv",
      lastExecutionParameterName: props.lastExecutionParameterName,
      dataset: weatherStationsDataset.name,
      datasetPathParameters: [
        {
          name: "sensor_id",
        },
        {
          name: "ingestion_date",
          createColumn: false,
        },
      ],
      job: cleanMergeOnDemandJob.name,
      jobRoleArn: cleanMergeOnDemandJobArn,
    });
  }
}
