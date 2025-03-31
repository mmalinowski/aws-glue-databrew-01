import {
  DataBrewClient,
  PublishRecipeCommand,
  ListRecipeVersionsCommand,
  BatchDeleteRecipeVersionCommand,
} from '@aws-sdk/client-databrew'
import { CdkCustomResourceEvent, CdkCustomResourceResponse } from 'aws-lambda'

const databrew = new DataBrewClient({})

export async function handler(event: CdkCustomResourceEvent): Promise<CdkCustomResourceResponse> {
  // extract the parameter passed on from construct
  const recipeName = event.ResourceProperties.RecipeName
  const description = event.ResourceProperties.Description
  const physicalResourceId = `${recipeName}`

  try {
    switch (event.RequestType) {
      case 'Create':
      case 'Update':
        // in case of create and update,
        // call a publish command to make new version available
        await databrew.send(
          new PublishRecipeCommand({
            Name: recipeName,
            Description: description,
          })
        )
        // and return `CdkCustomResourceResponse`
        return {
          Status: 'SUCCESS',
          PhysicalResourceId: physicalResourceId,
          Data: {
            RecipeName: recipeName,
          },
          LogicalResourceId: event.LogicalResourceId,
          RequestId: event.RequestId,
          StackId: event.StackId,
        }

      case 'Delete': {
        // get all recipe versions
        const listResponse = await databrew.send(
          new ListRecipeVersionsCommand({
            Name: recipeName,
          })
        )

        const recipes = listResponse.Recipes || []
        if (recipes.length > 0) {
          const versionsToDelete = recipes
            // The LATEST_WORKING cannot be deleted if the recipe has other versions
            // so remove all others and the LATEST_WORKING will be deleted by the L1 construct itself
            .filter(recipe => recipe.RecipeVersion !== 'LATEST_WORKING')
            .filter(recipe => recipe.RecipeVersion !== undefined)
            .map(version => version.RecipeVersion?.toString())

          if (versionsToDelete.length > 0) {
            await databrew.send(
              // delete all versions in batch
              new BatchDeleteRecipeVersionCommand({
                Name: recipeName,
                RecipeVersions: versionsToDelete as string[],
              })
            )
          }
        }
        // return the standard response in case of success
        return {
          Status: 'SUCCESS',
          PhysicalResourceId: event.PhysicalResourceId,
          LogicalResourceId: event.LogicalResourceId,
          RequestId: event.RequestId,
          StackId: event.StackId,
        }
      }
    }
  } catch (error) {
    // return the standard response in case of failure
    return {
      Status: 'FAILED',
      Reason: error instanceof Error ? error.message : String(error),
      PhysicalResourceId: event.LogicalResourceId || physicalResourceId,
      LogicalResourceId: event.LogicalResourceId,
      RequestId: event.RequestId,
      StackId: event.StackId,
    }
  }
}
