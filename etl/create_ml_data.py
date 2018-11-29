import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark import SparkConf, SparkContext
app_name = "NCAA Basketball"
spark = SparkSession.builder.appName(app_name).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from config import pbp_directory

# Main
def main(output):
    # Read in CSV data and hold onto filename
    pbp = spark.read.csv('output/pbp', header='true')
    box = spark.read.csv('output/box', header='true')
    home_teams = spark.read.csv('output/home_teams', header='true')
    box_home_join_columns = ['File_Team','Date','Gender', 'Division', 'Year']
    df = box.join(home_teams, box_home_join_columns)

    #home_teams.where((home_teams['File_Team'] == 'Texas') & (home_teams['Date'] == '12/29/2015')).show()
    #box.where((box['File_Team'] == 'Texas') & (box['Date'] == '12/29/2015')).show()
    #df.where((df['File_Team'] == 'Texas') & (df['Date'] == '12/29/2015')).show()
    df = df \
        .withColumn('Win', functions.when(df['PTS'] > df['opp_PTS'], 1).otherwise(0))  \
        .withColumn('Is_Home_Team', functions.when(df['Team'] == df['Home_Team'], 1).otherwise(0))

    home_win_conds = [
        ((df['Win'] == 1) & (df['Is_Home_Team'] == 1)) | ((df['Win'] == 0) & (df['Is_Home_Team'] == 0))
    ]

    df = df .withColumn('Home_Team_Win', functions.when(*home_win_conds, 1).otherwise(0))
    #df = df.select(['File_Team','Date','Gender', 'Division', 'Year','Home_Team', 'Away_Team', 'Is_Home_Team', 'Home_Team_Win' ])
    #df.where((df['File_Team'] == 'Texas') & (df['Date'] == '12/29/2015')).show()

    final_columns = ['Year', 'Gender', 'Division', 'Date', 'File_Team', 'Team', 'opp_Team', 'Home_Team', 'Away_Team', 'PTS', 'opp_PTS', 'Home_Team_Win', 'Is_Home_Team']
    df = df.select(final_columns)#.show()

    # print(pbp.columns)
    # print(df.columns)
    pbp_join_conds = [
        df['Gender'] == pbp['Gender'],
        df['Year'] == pbp['Year'],
        df['Division'] == pbp['Division'],
        df['File_Team'] == pbp['File_Team'],
    ]

    pbp_join_cols = ['Gender', 'Year', 'Division', 'File_Team', 'Date']

    df = pbp.join(df.drop('Team'), pbp_join_cols)#.show()
    final_columns = ['Year', 'Gender', 'Division', 'Date',  'Home_Team', 'Away_Team', 'Seconds_Left','Home_Score', 'Away_Score', 'Home_Margin', 'Home_Team_Win']
    df.select(final_columns).write.csv(output, mode='overwrite', header=True, compression='gzip')

if __name__ == '__main__':
    output = sys.argv[1]
    main(output)
