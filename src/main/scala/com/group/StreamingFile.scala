package com.group

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger

import java.util.logging.Logger

object StreamingFile extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hello Spark SQL 2")
      .config("spark.streaming.stopGracefullyOnShutdown", "true") //default false - not mandatory - to stop gracefully
      .config("spark.sql.shuffle.partitions", 3)
      .config("spark.sql.streaming.schemaInference", "true")
      .master("local[3]")
      .getOrCreate()

    val rawDF = spark.readStream
      .format("json")
      .option("path", "input")  //hdfs s3 fully qualified path
      .option("maxFilesPerTrigger", 1)
      .load()

    //rawDF.printSchema()

    val explodedDF = rawDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
      "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
      "DeliveryAddress.State", "DeliveryAddress.PinCode",
      "explode(InvoiceLineItems) as LineItem"
    )

    val flattenedDF = explodedDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")

    val invoiceWriterQuery = flattenedDF.writeStream
      .format("json")
      .option("path", "output")
      .option("checkpointLocation", "chk-point-dir")
      .outputMode("append")
      .queryName("Flattened Invoice Writer")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    invoiceWriterQuery.awaitTermination()




  }

}
