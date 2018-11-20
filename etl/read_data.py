import sys, re, math, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark import SparkConf, SparkContext
app_name = "NASA Logs Correlation"
spark = SparkSession.builder.appName(app_name).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')


DATA_DIR = os.path.join(os.environ['HOME'], 'Regular-test')


# Main
def main():
    df = spark.read.csv(os.path.join(DATA_DIR, '*/*/*/*/*/Score.csv'))
    #df = sc.textFile(os.path.join(DATA_DIR, '*/*/*/*/*/Play by Play - All (Parsed).csv'))
    df.show()


if __name__ == '__main__':
    sc = spark.sparkContext
    main(*sys.argv[1:])
