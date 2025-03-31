import * as path from "path";

import * as cdk from "aws-cdk-lib";
import { CfnTag } from "aws-cdk-lib";
import { CfnRecipe } from "aws-cdk-lib/aws-databrew";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import * as cr from "aws-cdk-lib/custom-resources";
import { Construct } from "constructs";

export interface DatabrewRecipeProps {
  /**
   * Recipe name
   */
  readonly name: string;
  /**
   * Recipe description
   */
  readonly description?: string;

  /**
   * Transformations steps
   */
  readonly steps: CfnRecipe.RecipeStepProperty[];

  /**
   * Tags to describe the pipeline
   */
  readonly tags?: Record<string, string>;
}

export class DatabrewRecipe extends Construct {
  public readonly recipe: CfnRecipe;
  public readonly publisher: cdk.CustomResource;

  constructor(scope: Construct, id: string, props: DatabrewRecipeProps) {
    super(scope, id);

    // L1 construct to create working version or recipe
    this.recipe = new CfnRecipe(this, "Recipe", {
      ...props,
      tags: Object.entries(props.tags || {}).map(([key, value]) => ({ key, value } as CfnTag)),
    });

    // lambda encapsulating the logic to provision published versions
    const handler = new NodejsFunction(this, `${id}Publisher`, {
      runtime: lambda.Runtime.NODEJS_22_X,
      bundling: {
        externalModules: ["@aws-sdk"],
      },
      entry: path.resolve(__dirname, `../../lambdas/databrew-recipe/src/index.ts`),
    });
    // the lambda needs to publish/delete the version (so that we can support stack deletion)
    handler.addToRolePolicy(
      new cdk.aws_iam.PolicyStatement({
        actions: ["databrew:PublishRecipe", "databrew:BatchDeleteRecipeVersion", "databrew:ListRecipeVersions"],
        resources: [
          cdk.Arn.format(
            {
              service: "databrew",
              resource: "recipe",
              resourceName: props.name,
            },
            cdk.Stack.of(this)
          ),
        ],
      })
    );
    // standard support for custom resources in CDK
    const provider = new cr.Provider(this, "Provider", {
      onEventHandler: handler,
    });
    this.publisher = new cdk.CustomResource(this, "Resource", {
      serviceToken: provider.serviceToken,
      // we pass the name and the description to the lambda
      // so in order to publish new version we need to make sure
      // either the version or description will be updated
      properties: {
        RecipeName: props.name,
        Description: props.description,
      },
    });
    // wait with publishing for the working version to be created
    this.publisher.node.addDependency(this.recipe);
  }
}
