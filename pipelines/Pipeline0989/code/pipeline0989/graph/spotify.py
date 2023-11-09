from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipeline0989.config.ConfigStore import *
from pipeline0989.udfs.UDFs import *

def spotify(spark: SparkSession) -> DataFrame:
    return spark.read.table("`spark_catalog`.`default`.`prophecytablespotify`")
