import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark import SparkConf, SparkContext
app_name = "NCAA Basketball"
spark = SparkSession.builder.appName(app_name).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from resources import play_by_play_schema_parsed, box_score_schema_parsed

# Main
def main(pbp_path, box_path, box_stats_path, home_path, output):
    # Read in CSV data and hold onto filename
        # pbp = spark.read.csv('output/pbp', header='true')#, schema=play_by_play_schema_parsed)
        # box = spark.read.csv('output/box', header='true', schema=box_score_schema_parsed)
        # home_teams = spark.read.csv('output/home_teams', header='true')
        pbp = spark.read.parquet(pbp_path)#, schema=play_by_play_schema_parsed)
        box = spark.read.parquet(box_path)
        home_teams = spark.read.parquet(home_path)

        # Join box scores with home teams
        box_home_join_columns = ['File_Team','Date','Gender', 'Division', 'Year']
        df = box.join(home_teams, box_home_join_columns)

        #df.where((df['Home_Team'] =='LeTourneau') & (df['Away_Team'] == 'Ozarks (AR)')).show()

        # Determine which team won the game - the label for the ML data
        df = df \
            .withColumn('Win', functions.when(df['PTS'].cast(types.IntegerType()) > df['opp_PTS'].cast(types.IntegerType()), 1).otherwise(0))  \
            .withColumn('Is_Home_Team', functions.when(df['Team'] == df['Home_Team'], 1).otherwise(0))

        home_win_conds = [((df['Win'] == 1) & (df['Is_Home_Team'] == 1)) | ((df['Win'] == 0) & (df['Is_Home_Team'] == 0))]
        df = df.withColumn('Home_Team_Win', functions.when(*home_win_conds, 1).otherwise(0))
        #df.where((df['Home_Team'] =='LeTourneau') & (df['Away_Team'] == 'Ozarks (AR)')).write.csv('output/debug', mode='overwrite', header=True)

        # Join DF with home teams and winning teams to PBP data
        pbp_join_cols = ['Gender', 'Year', 'Division', 'File_Team', 'Date']
        df = pbp.join(df.drop('Team'), pbp_join_cols)

        # Write out data
        final_columns = ['Year', 'Gender', 'Division', 'Date', 'Home_Team', 'Away_Team', 'Seconds_Left', 'Action', 'Status', 'Home_Score', 'Away_Score', 'Home_Margin', 'Home_Team_Win']
        df = df.select(final_columns)#.drop_duplicates()
        #df.write.csv(output, mode='overwrite', header=True, compression='gzip')

        # Get more advanced stats for each team
        box_stats_columns = ['Year', 'Gender', 'Division', 'Team', 'ORtg', 'DRtg', 'NetRtg', 'OREB%', 'DREB%', 'FT%', 'FTr', '3PAr', 'STL%', 'BLK%']
        box_stats = spark.read.csv(box_stats_path, header=True).select(box_stats_columns)
        box_stats_home = box_stats.toDF(*[col + '_Home' for col in box_stats.columns])
        box_stats_away = box_stats.toDF(*[col + '_Away' for col in box_stats.columns])

        home_conds = [
            df['Home_Team'] == box_stats_home['Team_Home'],
            df['Gender'] == box_stats_home['Gender_Home'],
            df['Division'] == box_stats_home['Division_Home'],
            df['Year'] == box_stats_home['Year_Home']
        ]

        away_conds = [
            df['Away_Team'] == box_stats_away['Team_Away'],
            df['Gender'] == box_stats_away['Gender_Away'],
            df['Division'] == box_stats_away['Division_Away'],
            df['Year'] == box_stats_away['Year_Away']
        ]

        df = df.join(box_stats_home, home_conds).drop(*['Year_Home','Gender_Home','Division_Home','Team_Home'])
        df = df.join(box_stats_away, away_conds).drop(*['Year_Away','Gender_Away','Division_Away','Team_Away'])#.show()

        df = df.drop_duplicates().where(df['Year'] == '2017') \
            .orderBy(['Year', 'Gender', 'Division', 'Date', 'Home_Team', 'Away_Team', 'Seconds_Left'], ascending=[0,0,0,0,0,0,0])
        df.write.csv(output, mode='overwrite', header=True, compression='gzip')

if __name__ == '__main__':
    main(*sys.argv[1:])
