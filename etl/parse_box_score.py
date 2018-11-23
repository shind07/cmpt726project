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

# Specify schema for out data
schema = types.StructType([
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

# Main
def main():
    # Read in CSV data and hold onto filename
    df = spark.read.csv(os.path.join(DATA_DIR, 'Men/*/*/*/*/Box Score - All (Parsed).csv'), header='true', schema=schema) \
        .withColumn('filename', functions.input_file_name()) \
        .withColumn('split', functions.split('filename', '/'))

    # Parse the file name into columns
    df = df \
        .withColumn('Gender', df['split'].getItem(4)) \
        .withColumn('Year', df['split'].getItem(5)) \
        .withColumn('Divison', df['split'].getItem(6)) \
        .withColumn('File_Team', df['split'].getItem(7)) \
        .withColumn('Date', df['split'].getItem(8)) \
        .withColumn('Team2', functions.regexp_replace('File_team', '%20', ' ')) \
        .drop(df['split'])
        #.drop(df['Team']) \
        #.dropDuplicates(['Team_Name', 'Date', 'Time', 'Player'])
        #.drop(df['filename']) \
        #.withColumn("id", functions.monotonically_increasing_id()) \
    teams = df.where(df['Player'] == 'Totals')
    # teams2 = teams
    # conds = [teams['File_Team'] == teams2['File_Team'], teams['Time'] == teams2['Time'], teams['Date'] == teams2['Date'], teams['Team'] != teams2['Team']]
    # #team.where((team['Date'] == '01.04.2017') & (team['File_Team'] == 'Texas')).show()
    # teams.join(teams2, conds).show()
    # #df.show()
    #df = df.dropDuplicates(['Team_Name', 'Date', 'Time', 'Player'])
    teams.na.fill(0).show()

if __name__ == '__main__':
    sc = spark.sparkContext
    main()
