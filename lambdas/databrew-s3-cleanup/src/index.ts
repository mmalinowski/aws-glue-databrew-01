import { S3Client, ListObjectsV2Command, CopyObjectCommand, DeleteObjectCommand, ListObjectsV2CommandOutput } from "@aws-sdk/client-s3";
import { Handler } from "aws-lambda";

const s3Client = new S3Client();

interface MoveRequest {
  destinationBucket: string;
  sourceKey: string;
  destinationKey: string;
}

interface Response {
  statusCode: number;
  body: string;
}

interface S3Object {
  Key?: string;
}

export const handler: Handler<MoveRequest, Response> = async (event: MoveRequest): Promise<Response> => {
  try {
    const listObjectsCommand = new ListObjectsV2Command({
      Bucket: event.destinationBucket,
      Prefix: event.sourceKey,
    });

    const listedObjects: ListObjectsV2CommandOutput = await s3Client.send(listObjectsCommand);

    if (!listedObjects.Contents || listedObjects.Contents.length === 0) {
      return {
        statusCode: 200,
        body: JSON.stringify({ message: "No files found to move" }),
      };
    }

    const movePromises: Promise<void>[] = (listedObjects.Contents as S3Object[]).map(async (object: S3Object): Promise<void> => {
      const sourceKey: string | undefined = object.Key;

      if (!sourceKey || !sourceKey.endsWith(".csv")) {
        return;
      }
      const pathParts: string[] = sourceKey.split("/");
      console.info(`Source key=${sourceKey}, parts=${pathParts}`);
      if (pathParts.length < 3) {
        return;
      }

      const destinationParts: string[] = pathParts.slice(2);
      const destinationKey: string = destinationParts.join("/");

      const copyCommand = new CopyObjectCommand({
        Bucket: event.destinationBucket,
        CopySource: `${event.destinationBucket}/${sourceKey}`,
        Key: `${event.destinationKey}/${destinationKey}`,
      });
      await s3Client.send(copyCommand);
      const deleteCommand = new DeleteObjectCommand({
        Bucket: event.destinationBucket,
        Key: sourceKey,
      });
      await s3Client.send(deleteCommand);

      console.info(`Moved ${sourceKey} to ${event.destinationKey}/${destinationKey}.`);
    });

    await Promise.all(movePromises.filter((promise): promise is Promise<void> => promise !== undefined));

    return {
      statusCode: 200,
      body: JSON.stringify({ message: "Files moved successfully." }),
    };
  } catch (error: unknown) {
    const errorMessage: string = error instanceof Error ? error.message : String(error);
    console.error("Error moving files: ", errorMessage);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: "Error moving files",
        error: errorMessage,
      }),
    };
  }
};
