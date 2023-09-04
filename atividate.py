import sys, getopt

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# spark-submit --jars=postgresql-42.6.0.jar /home/marcos/code/python/spark_tuto/atividate.py -i /home/marcos/Documents/Spark+PySpark/download/Atividades/Vendedores.parquet
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Exemplo").getOrCreate()

    opts, args = getopt.getopt(sys.argv[1:], "i:")
    infile = ""

    for opt, arg in opts:
        if opt == "-i":
            infile = arg

    vendedores = spark.read.format("parquet").load(infile)
    vendedores.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5432/spark"
    ).option("dbtable", "Vendedores").option("user", "postgres").option(
        "password", "123456"
    ).option(
        "driver", "org.postgresql.Driver"
    ).save()
    
    spark.stop()