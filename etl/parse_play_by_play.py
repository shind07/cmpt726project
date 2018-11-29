import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark import SparkConf, SparkContext
app_name = "NCAA Basketball"
spark = SparkSession.builder.appName(app_name).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

# Set directory that contains data
from config import data_directory
DATA_DIR = os.path.join(os.environ['HOME'], data_directory)

# Specify schema for our data
schema = types.StructType([
    types.StructField('Date', types.StringType(), False),
    types.StructField('Time', types.StringType(), False),
    types.StructField('Period', types.StringType(), False),
    types.StructField('TimeLeft', types.StringType(), False),
    types.StructField('Score', types.StringType(), False),
    types.StructField('Team', types.StringType(), False),
    types.StructField('Player', types.StringType(), False),
    types.StructField('Status', types.StringType(), False),
    types.StructField('Action', types.StringType(), False),
    types.StructField('ShotClock', types.IntegerType(), False),
    types.StructField('Lineup', types.StringType(), False),
    types.StructField('LineupTime', types.StringType(), False),
])


@functions.udf(returnType=types.IntegerType())
def period_mins_left(period):
    if period == '1st Period':
        return 30
    elif period == '2nd Period':
        return 20
    elif period == '3rd Period':
        return 10
    elif period == '4th Period':
        return 0
    elif period == '1st Half':
        return 20
    elif period == '2nd Half':
        return 0
    else:
        return 0

# Main
def main(output):
    # Read in CSV data and hold onto filename
    df = spark.read.csv(os.path.join(DATA_DIR, '*/*/*/*/*/Play by Play - All (Parsed).csv'), header='true', schema=schema) \
        .withColumn('filename', functions.input_file_name()) \
        .withColumn('split', functions.split('filename', '/')) \
        .withColumn('TimeLeft_split', functions.split('TimeLeft', ':')) \
        .withColumn('Score_split', functions.split('Score', '-'))

    # Parse the file name into columns
    df = df \
        .withColumn('Gender', df['split'].getItem(4)) \
        .withColumn('Year', df['split'].getItem(5)) \
        .withColumn('Division', df['split'].getItem(6)) \
        .withColumn('File_Team2', df['split'].getItem(7)) \
        .withColumn('Seconds_Left', df['TimeLeft_split'].getItem(0).cast(types.IntegerType())*60+df['TimeLeft_split'].getItem(1).cast(types.IntegerType())+period_mins_left(df['Period'])*60) \
        .withColumn('Home_Score', df['Score_split'].getItem(1)) \
        .withColumn('Away_Score', df['Score_split'].getItem(0)) \
        .withColumn('File_Team', functions.regexp_replace('File_Team2', '%20', ' ')) \
        .drop(df['Period']) \
        .drop(df['TimeLeft']) \
        .drop(df['filename']) \
        .drop(df['split']) \
        .drop(df['TimeLeft_split']) \
        .drop(df['Score_split']) \
        .drop(df['LineupTime'])
    df = df \
        .withColumn('Home_Margin', df['Home_Score'] - df['Away_Score']).cache()

    df = df.select('Date', 'Year', 'Gender', 'Division', 'Team', 'File_Team', 'Seconds_Left', 'Home_Score', 'Away_score', 'Home_Margin', 'Action')


    df.write.csv(output, mode='overwrite', header=True, compression='gzip')


if __name__ == '__main__':
    output = sys.argv[1]
    main(output)
