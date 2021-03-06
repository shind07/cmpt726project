import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark import SparkConf, SparkContext
app_name = "NCAA Basketball"
spark = SparkSession.builder.appName(app_name).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

DATA_DIR = '/Users/shind/Regular-stream'

box_schema = types.StructType([
    types.StructField('Date', types.StringType(), False),
    types.StructField('Time', types.StringType(), False),
    types.StructField('Period', types.StringType(), False),
    types.StructField('Team', types.StringType(), False),
    types.StructField('Player', types.StringType(), False),
    types.StructField('Pos', types.StringType(), False),
    types.StructField('MP', types.StringType(), False),
    types.StructField('FGM', types.IntegerType()),
    types.StructField('FGA', types.IntegerType()),
    types.StructField('3FG', types.IntegerType()),
    types.StructField('3FGA', types.IntegerType()),
    types.StructField('FT', types.IntegerType()),
    types.StructField('FTA', types.IntegerType()),
    types.StructField('PTS', types.IntegerType()),
    types.StructField('ORebs', types.IntegerType()),
    types.StructField('DRebs', types.IntegerType()),
    types.StructField('Tot Reb', types.IntegerType()),
    types.StructField('AST', types.IntegerType()),
    types.StructField('TO', types.IntegerType()),
    types.StructField('STL', types.IntegerType()),
    types.StructField('BLK', types.IntegerType()),
    types.StructField('Fouls', types.IntegerType()),
])

play_by_play_schema = types.StructType([
    types.StructField('Date', types.StringType(), False),
    types.StructField('Time', types.StringType(), False),
    types.StructField('Period', types.StringType(), False),
    types.StructField('TimeLeft', types.StringType(), False),
    types.StructField('Score', types.StringType(), False),
    types.StructField('Team', types.StringType(), False),
    types.StructField('Player', types.StringType(), False),
    types.StructField('Status', types.StringType(), False),
    types.StructField('Action', types.StringType(), False),
    types.StructField('Shot_Clock', types.IntegerType(), False),
    types.StructField('Lineup', types.StringType(), False),
    types.StructField('Lineup_Time', types.StringType(), False),
])

# Main
def main(source_directory):
    print("Listening to {}".format(source_directory))
    box_df = spark.readStream \
        .schema(box_schema) \
        .csv(os.path.join(DATA_DIR, '*/*/*/*/*/Box Score - All (Parsed).csv'), header='true')

    pbp_df = spark.readStream \
        .schema(play_by_play_schema) \
        .csv(os.path.join(DATA_DIR, '*/*/*/*/*/Play by Play - All (Parsed).csv'), header='true')

    box_stream = box_df.writeStream.format('console') \
        .outputMode('append').start()

    pbp_stream = pbp_df.writeStream.format('console') \
        .outputMode('append').start()

    box_stream.awaitTermination(3600)
    pbp_stream.awaitTermination(3600)


if __name__ == '__main__':
    sc = spark.sparkContext
    main(DATA_DIR)
