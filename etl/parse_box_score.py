import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark import SparkConf, SparkContext
app_name = "NCAA Basketball"
spark = SparkSession.builder.appName(app_name).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from resources import box_score_schema

# Main
def main(input, output):
    # Read in CSV data and hold onto filename
    fname = '*/*/*/*/*/Box Score - All (Parsed).csv'
    df = spark.read.csv(os.path.join(input, fname), header=True, schema=box_score_schema) \
        .withColumn('filename', functions.input_file_name()) \
        .withColumn('split', functions.split('filename', '/'))

    # Grab only data for the team, not individual players
    df = df.where(df['Player'] == 'Totals')

    # Parse the file name into columns, fill 'null' entries with 0
    df = df \
        .withColumn('Gender', df['split'].getItem(4)) \
        .withColumn('Year', df['split'].getItem(5)) \
        .withColumn('Division', df['split'].getItem(6)) \
        .withColumn('File_Team2', df['split'].getItem(7)) \
        .withColumnRenamed('Tot Reb', 'Tot_Reb') \
        .na.fill(0)

    # Reformat the team name
    teams = df.withColumn('File_Team', functions.regexp_replace('File_Team2', '%20', ' ')) \

    # Make two copies of the data and join the games together so each row will have full data for the game.
    new_cols = ['opp_' + col for col in teams.columns]
    teams2 = teams.toDF(*new_cols)
    join_conditions = [teams['File_Team'] == teams2['opp_File_Team'], teams['Time'] == teams2['opp_Time'], \
        teams['Date'] == teams2['opp_Date'], teams['Team'] != teams2['opp_Team']]
    full_data = teams.join(teams2, join_conditions)

    # Keep columns we want, write data.
    final_columns = ['Gender', 'Year', 'Division','Date', 'Time', 'File_Team', 'Team', 'FGM', 'FGA', \
        '3FG', '3FGA', 'FT', 'FTA', 'PTS', 'ORebs', 'DRebs', 'Tot_Reb', 'AST', 'TO', 'STL', \
        'BLK', 'Fouls',  'opp_Team', 'opp_FGM', 'opp_FGA', 'opp_3FG', 'opp_3FGA', 'opp_FT', 'opp_FTA',\
        'opp_PTS', 'opp_ORebs', 'opp_DRebs', 'opp_Tot_Reb', 'opp_AST', 'opp_TO', 'opp_STL', 'opp_BLK', 'opp_Fouls', ]

    full_data.select(final_columns) \
        .where((full_data['PTS'] != 0) & (full_data['opp_PTS'] != 0)) \
        .write.parquet(output, mode='append', compression='gzip')

if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)
