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
    home_teams = home_teams.toDF(*[col + '_home' for col in home_teams.columns])
    box_home_conditions = [
        box['File_Team'] == home_teams['Team_home'],
        box['Date'] == home_teams['Date_home'],
        box['Gender'] == home_teams['Gender_home'],
        box['Division'] == home_teams['Division_home'],
        box['Year'] == home_teams['Year_home'],
    ]
    df = box.join(home_teams, box_home_conditions)

    df = df \
        .withColumn('Win', functions.when(df['PTS'] > df['opp_PTS'], 1).otherwise(0))  \
        .withColumn('Is_Home_Team', functions.when(df['Team'] == df['Team_home'], 1).otherwise(0))
    home_win_conds = [
        ((df['Win'] == 1) & (df['Is_Home_Team'] == 1)) | ((df['Win'] == 0) & (df['Is_Home_Team'] == 0))
    ]

    df = df \
        .withColumn('Home_Team_Win', functions.when(*home_win_conds, 1).otherwise(0)) \
        .withColumnRenamed('Home_Team_home', 'Home_Team') \
        .withColumnRenamed('Away_Team_home', 'Away_Team') \

    #df_forML = df.select('Date', 'Year', 'Gender', 'Division', 'Team', 'FileTeam', 'Seconds_Left', 'Away_score', 'Home_Score', 'Action')
    #df.show()
    final_columns = ['Year', 'Gender', 'Division', 'Date', 'File_Team', 'Team', 'opp_Team', 'Home_Team', 'Away_Team', 'PTS', 'opp_PTS', 'Home_Team_Win']
    df.select(final_columns).write.csv(output, mode='overwrite', header=True, compression='gzip')

if __name__ == '__main__':
    output = sys.argv[1]
    main(output)
