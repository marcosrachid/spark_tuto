from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Streaming Postgres").getOrCreate()

    jsonschema = "nome STRING, postagem STRING, data INT"

    df = spark.readStream.json(
        "/home/marcos/Documents/Spark+PySpark/download/testestream/", schema=jsonschema
    )

    diretorio = "/home/marcos/temp"

    def atualizapostgres(dataf, batchId):
        dataf.write.format("jdbc").option(
            "url", "jdbc:postgresql://localhost:5432/posts"
        ).option("dbtable", "posts").option("user", "postgres").option(
            "password", "123456"
        ).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "append"
        ).save()

    stcal = (
        df.writeStream.foreachBatch(atualizapostgres)
        .outputMode("append")
        .trigger(processingTime="5 second")
        .option("checkpointlocation", diretorio)
        .start()
    )

    stcal.awaitTermination()
