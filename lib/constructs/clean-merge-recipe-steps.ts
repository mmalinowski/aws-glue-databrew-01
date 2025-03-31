import { CfnRecipe } from 'aws-cdk-lib/aws-databrew'

export const cleanMergeWeatherStationsSteps: CfnRecipe.RecipeStepProperty[] = [
  {
    action: {
      // we need measurement date out of the timestamp so that we can properly partition by it
      operation: 'DATE_FORMAT',
      parameters: {
        sourceColumn: 'timestamp',
        dateTimeFormat: 'yyyy-mm-dd',
        targetColumn: 'measurement_date',
      },
    },
  },
  {
    action: {
      // By using the WEEK_NUMBER operation, we add an extra column that indicates the week in which the measurements were taken
      operation: 'WEEK_NUMBER',
      parameters: {
        sourceColumn: 'timestamp',
        targetColumn: 'week_number',
      },
    },
  },
  {
    action: {
      // using sort operation, we make sure that all merged results are properly sorted
      operation: 'SORT',
      parameters: {
        expressions: JSON.stringify([
          { sourceColumn: 'sensor_id', ordering: 'ASCENDING', nullsOrdering: 'NULLS_TOP' },
          { sourceColumn: 'timestamp', ordering: 'ASCENDING', nullsOrdering: 'NULLS_TOP' },
        ]),
      },
    },
  },
  {
    action: {
      // we round the humidity and the new value is put in the extra column
      operation: 'ROUND',
      parameters: {
        sourceColumn: 'humidity',
        targetColumn: 'humidity_round',
      },
    },
  },
  {
    action: {
      // with case operator we add label for the pressure
      operation: 'CASE_OPERATION',
      parameters: {
        valueExpression:
          "CASE WHEN pressure IS NULL THEN 'N/A' WHEN pressure > 1020 THEN 'HIGH' WHEN pressure < 1000 THEN 'LOW' ELSE 'NORMAL' END",
        targetColumn: 'pressure_label',
      },
    },
  },
  {
    action: {
      // and the time for the clean up, removing old humidity value
      operation: 'DELETE',
      parameters: {
        sourceColumn: 'humidity',
      },
    },
  },
  {
    action: {
      // and renaming the column with rounded values
      operation: 'RENAME',
      parameters: {
        sourceColumn: 'humidity_round',
        targetColumn: 'humidity',
      },
    },
  },
]
