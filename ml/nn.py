import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import DecisionTreeRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

cols = ['Year', 'Gender', 'Division', 'Date', 'Home_Team', 'Away_Team', 'Seconds_Left', \
    'Action', 'Status', 'Home_Score', 'Away_Score', 'Home_Margin', 'Home_Team_Win', 'ORtg_Home', \
    'DRtg_Home', 'NetRtg_Home', 'OREB%_Home', 'DREB%_Home', 'FT%_Home', 'FTr_Home', '3PAr_Home', \
    'STL%_Home', 'BLK%_Home', 'ORtg_Away', 'DRtg_Away', 'NetRtg_Away', 'OREB%_Away', 'DREB%_Away', \
    'FT%_Away', 'FTr_Away', '3PAr_Away', 'STL%_Away', 'BLK%_Away']

string_cols = ['Year', 'Gender', 'Division', 'Date', 'Home_Team', 'Away_Team','Action', 'Status',]
int_cols = ['Home_Score', 'Away_Score', 'Home_Margin', 'Home_Team_Win', 'Seconds_Left']

nn_schema = types.StructType(
    [types.StructField(field_name, types.StringType(), False) if field_name in string_cols \
     else types.StructField(field_name, types.IntegerType(), False) if field_name in int_cols \
     else types.StructField(field_name, types.FloatType(), False) \
    for field_name in cols]
)
def main(inputs, output):
    # Load data, split into training and validation sets
    data = spark.read.csv(inputs, header=True, schema=nn_schema)
    data = data.select('Home_Score', 'Away_Score', 'Home_Margin', 'Seconds_Left', 'Home_Team_Win')


    train, validation = data.randomSplit([0.75, 0.25])

    assembler = VectorAssembler(
        inputCols=['Home_Score', 'Away_Score', 'Home_Margin', 'Seconds_Left'],
        outputCol='features')

    classifier = RandomForestClassifier(labelCol='Home_Team_Win', maxBins=200, maxDepth=5)

    pipeline = Pipeline(stages=[assembler, classifier])
    model = pipeline.fit(train)
    model.write().overwrite().save(output)
    predictions = model.transform(validation)
    predictions.write.csv(output + '-predictions', mode='overwrite', header='True' )
    #predictions.show()
    #predictions.select('Home_Score', 'Away_Score', 'Home_Margin', 'Seconds_Left', 'Home_Team_Win', 'prediction', 'probability').show(10)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
