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
def main(input, output):
    # Read in data and grab filename
    df = spark.read.csv(os.path.join(input, 'Men/*/*/*/*/Score.csv'), header='true') \
        .withColumn('filename', functions.input_file_name()) \
        .withColumn('split', functions.split('filename', '/'))

    # Parse filename, create additional columns
    df = df \
        .withColumnRenamed('_c0', 'Team1') \
        .withColumn('Gender', df['split'].getItem(4)) \
        .withColumn('Year', df['split'].getItem(5)) \
        .withColumn('Division', df['split'].getItem(6)) \
        .withColumn('Team2', df['split'].getItem(7)) \
        .withColumn('Date2', df['split'].getItem(8)) \
        .withColumn('File_Team', functions.regexp_replace('Team2', '%20', ' ')) \
        .withColumn('id', functions.monotonically_increasing_id()) \
        .withColumn('Date',functions.regexp_replace('Date2', '\.', '/'))  \
        .drop(df['split']) \
        .drop(df['filename']) \
        .drop('Team2') \
        .drop('Date2')

    # Join df to itself to get both teams in the same row
    df2 = df.toDF(*[c + "_2" for c in df.columns])
    join_conditions = [df['File_Team'] == df2['File_Team_2'], df['Date'] == df2['Date_2'], df['Division'] == df2['Division_2'], \
        df['Year'] == df2['Year_2'], df['Team1'] != df2['Team1_2'], df['Gender'] == df2['Gender_2']]
    home_and_away = df.join(df2, join_conditions)

    # Determine which team is home/away
    home_and_away = home_and_away \
        .withColumn('Home_Team', functions.when(home_and_away['id'] > home_and_away['id_2'], home_and_away['Team1']).otherwise(home_and_away['Team1_2']))
    home_and_away = home_and_away \
        .withColumn('Away_Team', functions.when(home_and_away['id'] < home_and_away['id_2'], home_and_away['Team1']).otherwise(home_and_away['Team1_2']))

    # Write data
    home_and_away = home_and_away.withColumnRenamed('Team1', 'Team') \
        .select(['Gender', 'Division', 'Year', 'Date', 'File_Team', 'Home_Team', 'Away_Team' ] )
    home_and_away.drop_duplicates().coalesce(1).write.csv(output, mode='overwrite', header=True, compression='gzip')

if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)
