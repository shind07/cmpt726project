import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark import SparkConf, SparkContext
app_name = "NCAA Basketball"
spark = SparkSession.builder.appName(app_name).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

@functions.udf(returnType=types.IntegerType())
def calculate_PPS(type):
    if type == 'Three Point Jumper':
        return 3
    elif type in ['Layup','Two Point Jumper', 'Tip In' , 'Dunk']:
        return 2
    elif type ==  'Free Throw':
        return 1
    else:
        return 0

# Main
def main(input, output):
    # Read in data and cache because we will be splitting the data
    df = spark.read.parquet(input).cache()

    # PPS Analysis
    df_analysis_made = df.select('Action', 'Status', 'Shot_Clock').where((df['Status'] == 'made'))
    df_analysis_missed = df.select('Action', 'Status', 'Shot_Clock').where((df['Status'] == 'missed'))

    made = df_analysis_made.groupby('Action', 'Shot_Clock').agg(functions.count('*').alias('count_made'))
    missed = df_analysis_missed.groupby('Action', 'Shot_Clock').agg(functions.count('*').alias('count_missed'))
    all = missed.join(made, ['Action', 'Shot_Clock'])
    all = all.withColumn('attempts', all['count_made'] + all['count_missed']).withColumn('points_worth', calculate_PPS(all['Action']))
    all = all.withColumn('points', all['count_made']*all['points_worth'])

    PPS_byAction = all.groupby('Action') \
        .agg(functions.sum(all['count_made']).alias('total_made'), functions.sum(all['attempts']).alias('total_attempts'), functions.sum(all['points']).alias('total_points'))
    PPS_byAction = PPS_byAction.withColumn('PPS', PPS_byAction['total_points']/PPS_byAction['total_attempts'])

    # Can coalesce because there will only be about 5 rows - one for each action.
    PPS_byAction.coalesce(1).write.csv(output, mode='overwrite', header=True, compression='gzip')

if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)
