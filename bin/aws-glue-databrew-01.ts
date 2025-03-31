#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { DataEngineeringStack } from "../lib/data-engineering-stack";

const app = new cdk.App();
new DataEngineeringStack(app, "DataEngineeringStack", {
  lastExecutionParameterName: "weather-stations-last-execution", // Replace with your actual parameter name
});
