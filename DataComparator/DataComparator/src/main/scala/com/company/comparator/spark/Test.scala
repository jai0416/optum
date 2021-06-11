package com.company.comparator.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object Test {
  val spark: SparkSession = SparkSession.builder()
    .appName("jay")
    .master("local[2]").getOrCreate()

  def main(args: Array[String]): Unit = {
    print("heelo")
    var latestFile = "reddy.xlsx"
    var sourcePath = "C:\\Jay\\New folder (7)\\DataComparator\\src\\main\\resources\\"
    import org.apache.spark.sql.functions._
  var df=  getDFExcelSheetUsingSpark(latestFile, sourcePath).orderBy(desc("Name")).
    show(100, false)

  }

  def getDFExcelSheetUsingSpark(latestFile: String, sourcePath: String): DataFrame = {
    val df = spark.read.format("com.crealytics.spark.excel")
      .option("sheetName", "Sheet1")
      .option("header", "true")
      .option("location", sourcePath)
      .option("treatEmptyValuesAsNulls", true)
      .load(sourcePath + latestFile)
    df

  }
}