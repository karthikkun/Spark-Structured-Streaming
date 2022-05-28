package com.group

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

import java.util.logging.Logger

object StreamingWordCount extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hello Spark SQL")
      .config("spark.streaming.stopGracefullyOnShutdown", "true") //default false - not mandatory - to stop gracefully
      .config("spark.sql.shuffle.partitions", 3)
      .master("local[3]")
      .getOrCreate()

    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    //linesDF.printSchema()
    val wordsDF = linesDF.select(expr("explode(split(value, ' ')) as word"))
    val countDF = wordsDF.groupBy("word").count()

    val wordCountQuery = countDF.writeStream
      .format("console")
      .option("checkpointLocation", "chk-point-dir")
      .outputMode("complete")
      .start()

    logger.info("Listening to localhost:9999")
    wordCountQuery.awaitTermination()

  }
}
