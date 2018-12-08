import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark import SparkConf, SparkContext
from resources import renameGroupedColumns, box_score_columns
app_name = "NCAA Basketball"
spark = SparkSession.builder.appName(app_name).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

# Main
def main(input, output):
    df = spark.read.parquet(input)

    # Sum all data by year for each team
    df = df \
        .withColumn('GP', functions.lit(1).cast(types.IntegerType())) \
        .withColumn('Win', functions.when(df['PTS'] > df['opp_PTS'], 1).otherwise(0)) \
        .withColumn('Loss', functions.when(df['PTS'] < df['opp_PTS'], 1).otherwise(0)) \
        .groupby(['Gender', 'Year', 'Division', 'Team']).sum()

    # Rename grouped columns to make for easier calculations
    df = df.toDF(*renameGroupedColumns(df.columns))

    # calculate new columns
    df = df \
        .withColumn('FT%', df['FT'] / df['FTA']) \
        .withColumn('OREB%', (df['ORebs'] / (df['ORebs'] + df['opp_DRebs']))) \
        .withColumn('DREB%', functions.round((df['DRebs'] / (df['DRebs'] + df['opp_ORebs'])), 2)) \
        .withColumn('POSS', df['FGA'] + df['TO'] + 0.475*df['FTA'] - df['ORebs']) \
        .withColumn('opp_POSS', df['opp_FGA'] + df['opp_TO'] + 0.475*df['opp_FTA'] - df['opp_ORebs']) \
        .withColumn('AST%', df['AST'] / df['FGM'])

    df = df \
        .withColumn('PPP', df['PTS'] / df['POSS']) \
        .withColumn('opp_PPP', df['opp_PTS'] / df['opp_POSS']) \
        .withColumn('eFG%', (df['FGM'] + 0.5*df['3FG']) / df['FGA']) \
        .withColumn('opp_eFG%', (df['opp_FGM'] + 0.5*df['opp_3FG']) / df['opp_FGA']) \
        .withColumn('TOV%', df['TO'] / df['POSS']) \
        .withColumn('FTr', df['FTA'] / df['FGA']) \
        .withColumn('3PAr', df['3FGA'] / df['FGA']) \
        .withColumn('STL%', df['STL'] / df['opp_POSS']) \
        .withColumn('BLK%', df['BLK'] / df['opp_FGA']) \
        .withColumn('Pace', df['POSS'] / df['GP']) \
        .withColumn('opp_Pace', df['opp_POSS'] / df['GP'])

    df = df \
        .withColumn('ORtg', df['PPP']*100) \
        .withColumn('DRtg', df['opp_PPP']*100)

    # Cache dataframe because we are writing to 2 outputs
    df = df.withColumn('NetRtg', df['ORtg'] - df['DRtg']).cache()

    # Write out final data - IT IS OK TO COALESCE BECAUSE WE HAVE GROUPED BY TEAM - DATA WILL BE NO MORE THAN A FEW THOUSAND ROWS OF CSV OUTPUT
    # First write out the data for all teams, then group data and write out summary data.
    df.orderBy('Year', 'Gender', 'Division').coalesce(1).write.csv(output+'-teams', header=True, mode='overwrite')
    df = df.groupby('Gender', 'Year', 'Division').avg()
    final_columns = ['Gender', 'Year', 'Division', 'PPP', 'FTr', '3PAr', 'eFG%', 'Pace', 'OReb%', 'DReb%']
    df.toDF(*renameGroupedColumns(df.columns)).select(final_columns).orderBy('Year', 'Gender', 'Division') \
        .coalesce(1).write.csv(output+'_all', mode='overwrite', header=True)

if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)
