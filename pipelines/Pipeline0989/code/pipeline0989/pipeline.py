from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from pipeline0989.config.ConfigStore import *
from pipeline0989.udfs.UDFs import *
from prophecy.utils import *
from pipeline0989.graph import *

def pipeline(spark: SparkSession) -> None:
    df_spotify = spotify(spark)
    sql_database(spark, df_spotify)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Pipeline0989")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/Pipeline0989", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/Pipeline0989")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
