package com.group.kafka

import com.group.kafka.Schema.getSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.Trigger

import java.util.logging.Logger

object StreamingKafka extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark SQL Kafka")
      .config("spark.streaming.stopGracefullyOnShutdown", "true") //default false - not mandatory - to stop gracefully
      .config("spark.sql.shuffle.partitions", 3)
      .master("local[3]")
      .getOrCreate()

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "0.0.0.0:9092") //comma seperated host:port
      .option("subscribe", "invoices")
      .option("startingOffsets", "earliest") //earliest takes effect only when checkpoint is null
      .load()

    //kafkaDF.printSchema()

    val valueDF = kafkaDF
      .select(from_json(col("value").cast("string"), getSchema).as("value")).select(col("value.*"))

    //valueDF.printSchema()

    val explodedDF = valueDF
      .selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
        "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
        "DeliveryAddress.State", "DeliveryAddress.PinCode",
        "explode(InvoiceLineItems) as LineItem"
      )

    //explodedDF.printSchema()

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
