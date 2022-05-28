package com.group.util

import org.apache.spark.sql.DataFrame
import collection.mutable.ArrayBuffer

object FlattenDF {

  val totalMainArrayBuffer = collection.mutable.ArrayBuffer[String]()

  def flatten_df_Struct(dfTemp: DataFrame, dfTotalOuter: DataFrame): DataFrame = {
    //dfTemp.printSchema
    val totalStructCols = dfTemp.dtypes.map(x => x.toString.substring(1, x.toString.size - 1)).filter(_.split(",", 2)(1).contains("Struct")) // in case i the column names come with the word Struct embedded in it
    val mainArrayBuffer = ArrayBuffer[String]()
    for (totalStructCol <- totalStructCols) {
      val tempArrayBuffer = ArrayBuffer[String]()
      tempArrayBuffer += s"${totalStructCol.split(",")(0)}.*"
      //tempArrayBuffer.toSeq.toDF.show(false)
      val columnsInside = dfTemp.selectExpr(tempArrayBuffer: _*).columns
      for (column <- columnsInside)
        mainArrayBuffer += s"${totalStructCol.split(",")(0)}.${column} as ${totalStructCol.split(",")(0)}_${column}"
      //mainArrayBuffer.toSeq.toDF.show(false)
    }
    //dfTemp.selectExpr(mainArrayBuffer:_*).printSchema
    val nonStructCols = dfTemp.selectExpr(mainArrayBuffer: _*).dtypes.map(x => x.toString.substring(1, x.toString.size - 1)).filter(!_.split(",", 2)(1).contains("Struct")) // in case i the column names come with the word Struct embedded in it
    for (nonStructCol <- nonStructCols)
      totalMainArrayBuffer += s"${nonStructCol.split(",")(0).replace("_", ".")} as ${nonStructCol.split(",")(0)}" // replacing _ by . in origial select clause if it's an already nested column
    dfTemp.selectExpr(mainArrayBuffer: _*).dtypes.map(x => x.toString.substring(1, x.toString.size - 1)).filter(_.split(",", 2)(1).contains("Struct")).size
    match {
      case value if value == 0 => dfTotalOuter.selectExpr(totalMainArrayBuffer: _*)
      case _ => flatten_df_Struct(dfTemp.selectExpr(mainArrayBuffer: _*), dfTotalOuter)
    }
  }

  def flatten_df(dfTemp: DataFrame): DataFrame = {
    var totalArrayBuffer = ArrayBuffer[String]()
    val totalNonStructCols = dfTemp.dtypes.map(x => x.toString.substring(1, x.toString.size - 1)).filter(!_.split(",", 2)(1).contains("Struct")) // in case i the column names come with the word Struct embedded in it
    for (totalNonStructCol <- totalNonStructCols)
      totalArrayBuffer += s"${totalNonStructCol.split(",")(0)}"
    totalMainArrayBuffer.clear
    flatten_df_Struct(dfTemp, dfTemp) // flattened schema is now in totalMainArrayBuffer
    totalArrayBuffer = totalArrayBuffer ++ totalMainArrayBuffer
    dfTemp.selectExpr(totalArrayBuffer: _*)
  }

}
