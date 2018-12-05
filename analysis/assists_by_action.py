import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark import SparkConf, SparkContext
app_name = "NCAA Basketball"
spark = SparkSession.builder.appName(app_name).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
from resources import play_by_play_schema_parsed


# Main
def main(input, output):
    # Read in CSV data and hold onto filename
    #df = spark.read.csv(input, header='true', schema=play_by_play_schema_parsed)
    df = spark.read.parquet(input)
    df.cache()

    # All shot attempts
    df_analysis_made = df.select('Action', 'Status').where((df['Status'] == 'made'))
    df_analysis_missed = df.select('Action', 'Status').where((df['Status'] == 'missed'))

    made = df_analysis_made.groupby('Action').agg(functions.count('*').alias('count_made'))
    missed = df_analysis_missed.groupby('Action').agg(functions.count('*').alias('count_missed'))
    all = missed.join(made, ['Action'])
    all = all.withColumn('attempts', all['count_made'] + all['count_missed'])

    # Assist Analysis
    df_score = df.filter((df['Action'] == 'Three Point Jumper') \
             | (df['Action'] == 'Layup') \
             | (df['Action'] == 'Two Point Jumper') \
             | (df['Action'] == 'Dunk')) \
        .withColumn('ScoringPlayer', df['Player']).withColumn('ScoringAction', df['Action']) \
        .select('Date', 'Seconds_Left', 'Division', 'Gender', 'Team', 'Action', 'ScoringPlayer', 'ScoringAction', 'File_Team')

    df_assist = df.filter(df['Action'] == 'Assist').withColumn('AssistingPlayer', df['Player']) \
        .select('Date', 'Seconds_Left', 'Division', 'Gender', 'Team', 'Action', 'AssistingPlayer', 'File_Team')

    df_joined = df_score.join(df_assist, ['Date', 'Seconds_Left', 'Division', 'Gender', 'Team', 'File_Team']) \
        .select('ScoringAction', 'ScoringPlayer', 'AssistingPlayer')

    df_joined = df_joined.groupby('ScoringAction').agg(functions.count('*').alias('TotalAssists')) \
        .withColumnRenamed('ScoringAction', 'Action')

    # Join tables
    summary = all.join(df_joined, ['Action'], 'outer')
    summary = summary.withColumn('PercentAssisted', summary['TotalAssists']/summary['count_made'])
    # output.show()

    # Save output
    summary.fillna(0).coalesce(1).write.csv(output, mode='overwrite', header=True, compression='gzip')

if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)
