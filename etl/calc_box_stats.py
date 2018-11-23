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


# Main
def main(input):
    # Read in CSV data and hold onto filename
    print(input)
    df = spark.read.csv(input, header='true')
    df.show()
if __name__ == '__main__':
    input = sys.argv[1]
    main(input)
