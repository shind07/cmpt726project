import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark import SparkConf, SparkContext
app_name = "NCAA Basketball"
spark = SparkSession.builder.appName(app_name).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from config import data_directory
DATA_DIR = os.path.join(os.environ['HOME'], data_directory)

# Main
def main():
    df = spark.read.csv(os.path.join(DATA_DIR, 'Men/*/*/*/*/Score.csv'), header='true') \
        .withColumn('filename', functions.input_file_name()) \
        .withColumn('split', functions.split('filename', '/'))

    df = df \
        .withColumnRenamed('_c0', 'Team1') \
        .withColumn('Gender', df['split'].getItem(4)) \
        .withColumn('Year', df['split'].getItem(5)) \
        .withColumn('Divison', df['split'].getItem(6)) \
        .withColumn('Team', df['split'].getItem(7)) \
        .withColumn('Date', df['split'].getItem(8)) \
        .withColumn('Team2', functions.regexp_replace('Team', '%20', ' ')) \
        .drop(df['split']) \
        .drop(df['filename']) \

    df2 = df
    join_conditions = [df['File_Team'] == teams2['opp_File_Team'], teams['Time'] == teams2['opp_Time'], \
        teams['Date'] == teams2['opp_Date'], teams['Team'] != teams2['opp_Team']]
    # #df.where(functions.isnull('Total')).show()
    # df.where(df['Date'] == '02.08.2016').show()
    df.select(df['_c0']).show()

if __name__ == '__main__':
    sc = spark.sparkContext
    main()
