package com.company.comparator.spark

import java.io.FileOutputStream
import java.sql.Timestamp
import java.util.{Calendar, Date, Properties}

import com.company.comparator.factory.SparkSessionFactory
import com.company.comparator.utils.{HdfsUtils, Utils, StringConstants => SC}
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
  * Class to compare hive and csv data
  */
class HiveCSVComparator(spark: SparkSession, inputProperties: Properties) {

  val log: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def compareHiveCSVData(): Unit = {
    val latestFile = HdfsUtils.getLastModifiedFile(inputProperties.getProperty(SC.SOURCE_FILE_PATH), spark)
    val properties = getPropFromExcelSheetUsingSpark(latestFile, inputProperties.getProperty(SC.SOURCE_FILE_PATH))

    val hive_db=properties.getProperty(SC.HIVE_DATABASE)
    val table=properties.getProperty(SC.HIVE_TABLE)
    val hiveDF = spark.read.format("hive").table(hive_db+"."+table)

    val csvDF = getDFExcelSheetUsingSpark(latestFile, inputProperties.getProperty(SC.SOURCE_FILE_PATH))
  //  val csvDF = spark.read.format("csv").option("header","true").option("delimiter", properties.getProperty(SC.CSV_DELIMITER)).load(properties.getProperty(SC.CSV_FILE_PATH))
    csvDF.createOrReplaceTempView(SC.CSV_TEMP_TABLE)
   // createHiveInternalTable(properties,latestFile.substring(0,latestFile.indexOf(".")))
   // createHiveExternalTable()
  //  val csvHiveDF = spark.read.format("hive").table(properties.getProperty(SC.CSV_DATABASE)+"."+latestFile.substring(0,latestFile.indexOf(".")))
    compareDF(getHiveDFWithCommentsAsCol(hiveDF, csvDF), csvDF, properties,latestFile)
  }

  def createHiveExternalTable(properties: Properties,latestFile: String): Unit = {
    val db = properties.getProperty(SC.CSV_DATABASE)
    val tableName = latestFile.substring(0,latestFile.indexOf("."))
    val delimiter = properties.getProperty(SC.CSV_DELIMITER)
    spark.sql(s"""drop table if exists $db.$tableName""")

    spark.sql(s"""create table $db.$tableName
                |    row format delimited
                |    fields terminated by '$delimiter'
                |    stored as textfile
                |    as select * from ${SC.CSV_TEMP_TABLE}
                |    where 1 = 2""".stripMargin)

    spark.sql(
      s"""alter table $db.$tableName
         |set location 'hdfs://${Utils.getFullPath(properties)}'
       """.stripMargin)

    val catalog = spark.sessionState.catalog
    val tableIdentifier = TableIdentifier(tableName, Some(db))
    val tableMetadata = catalog.getTableMetadata(tableIdentifier)
    val updatedMetadata = tableMetadata.copy(tableIdentifier, CatalogTableType.EXTERNAL)
    catalog.alterTable(updatedMetadata)
    //TODO the path created for internal table will be left without deleting
  }

  def createHiveInternalTable(properties: Properties,latestFile: String): Unit = {
    val db = properties.getProperty(SC.CSV_DATABASE)
    val tableName = latestFile
    spark.sql(s"""drop table if exists $db.$tableName""")
    spark.sql("create table "+properties.getProperty(SC.CSV_DATABASE)+"."+latestFile
      +" as select * from "+SC.CSV_TEMP_TABLE)
  }

  def compareDF(hiveDF: DataFrame, csvDF: DataFrame, properties: Properties,latestFile: String): Unit = {

    hiveDF.printSchema()
    csvDF.printSchema()

    val csvColumns = csvDF.columns.map(_.toLowerCase)
    val hiveColumns = hiveDF.columns.map(_.toLowerCase)

    log.info("No of Columns in Hive Table: {}", hiveColumns.length)
    log.info("No of Columns in CSV File: {}", csvColumns.length)
    val commonColumns = csvColumns.intersect(hiveColumns)
    val csvSpecificColumns = csvColumns.diff(hiveColumns)
    //TODO Check if we need to convert columns to upper case / lower case if needed.
    log.info("Common columns between csv and hive are: [{}]", commonColumns)
    log.info("Columns present in csv but not in hive are: [{}]", csvSpecificColumns)
    log.info("Columns present in hive but not in csv are: [{}]", hiveColumns.diff(csvColumns))


    /**
    1.Check if primary keys are present in hiveDF and csv DF, if not present then can't compare
      2.If primary keys present in both DFs then we can compare
      3.First make a list of primary keys and non primary keys list using above columns list for each DFs in a variable
      4.Now make a left join with hiveDF to csvDF using primary keys as a left join condition and then use any of the non primary key for null check
      5.if non primary key is null then that mean primary key is not present in csv.
      6. repeat 4 & 5 in a reverse way for csv df as first table now.
      7.using a inner join now between two DFs will give common data between two DFs
      8.get the counts for above dfs total counts and also counts for dfs prepared in 4, 5, 6 & 7
      9.Now we will have:
        a.Total counts for hiveDF and csvDF
        b.Total count of hiveDF minus csvDF
        c.total count of csvDF minus hiveDF
        d.total count of csvDF intersect hiveDF
      */

    val primaryKeysList: Array[String] = properties.getProperty(SC.PRIMARY_KEYS).toLowerCase.split(",").map(_.trim)


    if(primaryKeysList.forall(hiveColumns.contains) && primaryKeysList.forall(csvColumns.contains)) {

      val cachedHiveDF = hiveDF

      val cachedCSVDF = csvDF

      //Taking matching dataset from hiveDF and csvDF and then select the matching primary keys
      val commonDF = cachedHiveDF.join(cachedCSVDF, primaryKeysList.toSeq).select(primaryKeysList.head, primaryKeysList.tail:_*).dropDuplicates()
   //   val commonDF = cachedHiveDF.join(cachedCSVDF,
   //   cachedHiveDF.col(properties.getProperty(SC.PRIMARY_KEYS)) === cachedCSVDF.col(properties.getProperty(SC.PRIMARY_KEYS))).select(primaryKeysList.head, primaryKeysList.tail:_*)
      //Taking primary keys that are only present in hiveDF
    //  val specificHiveDF = cachedHiveDF.join(cachedCSVDF, primaryKeysList.toSeq, SC.LEFT_ANTI).select(primaryKeysList.head, primaryKeysList.tail:_*).dropDuplicates()
      //Taking primary keys that are only present in csvDF

      val specificCsvDF = cachedCSVDF.join(cachedHiveDF, primaryKeysList.toSeq, SC.LEFT_ANTI).select(primaryKeysList.head, primaryKeysList.tail:_*).dropDuplicates()

      //Total counts
     val hiveRecordsCount = cachedHiveDF.count()
      log.info("Hive total records: [{}]", hiveRecordsCount)
      val csvRecordsCount = cachedCSVDF.count()
      log.info("CSV total records: [{}]", csvRecordsCount)
      val commonRecordsCount = commonDF.count()
      log.info("Common records between hive and csv: [{}]",commonRecordsCount)
      //val hiveSpecificRecordsCount = specificHiveDF.count()
      //log.info("Records that are present only in hive table: [{}]", hiveSpecificRecordsCount)
      val csvSpecificRecordsCount = specificCsvDF.count()
      log.info("Records that are present only in csv table: [{}]", csvSpecificRecordsCount)
      //TODO find the percentage 4th point in requirement.

      writeToExcel(primaryKeysList, csvColumns, hiveColumns, commonColumns, csvSpecificColumns, commonDF.collect(), specificCsvDF.collect(), properties,latestFile)

    } else {
      log.error("Hive table or csv doesn't contains primary keys provided.")

    }
  }

  def writeToExcel(primaryKeys: Array[String],
                   csvColumns: Array[String],
                   hiveColumns: Array[String],
                   commonColumns: Array[String],
                   csvSpecificColumns: Array[String],
                   commonData: Array[Row],
                   csvSpecificData:Array[Row],
                   properties: Properties,
                   latestFile: String): Unit = {
    log.info("Primary key Columns: {}", primaryKeys)
    val workbook = new XSSFWorkbook()

    //Creating sheet for no of columns in each table info
    val noofColumns = workbook.createSheet("Column Counts")
    Utils.writeCountsToExcel(noofColumns, csvColumns.length, hiveColumns.length)

    log.info("Creating sheet for CSV columns comparision")
    val columns = workbook.createSheet("CSV Columns")
    Utils.writeToExcel(columns, commonColumns, csvSpecificColumns)

    log.info("Creating sheet for Primary key data present")
    val commonSheet = workbook.createSheet("Primary Keys Data Present")
    Utils.writeDataSetToExcelMultiColumn(commonSheet, primaryKeys, commonData)

    log.info("Creating sheet for Primary key data not present")
    val specificSheet = workbook.createSheet("Primary Keys Not Present")
    Utils.writeDataSetToExcelMultiColumn(specificSheet, primaryKeys, csvSpecificData)

    //Creating sheet for CSV only primary key data comparision
   /* val commonSheet = workbook.createSheet("Primary Keys Data")
    Utils.writeDataSetToExcel(commonSheet, primaryKeys, commonData, csvSpecificData)*/

    //Creating sheet for percentage info
    val percentageSheet = workbook.createSheet("Percentage")
    Utils.writePercentageToExcel(percentageSheet, commonData.length, csvSpecificData.length)
    val date = new Date()
    val timestamp = new Timestamp(date.getTime)
    val time = timestamp.toString.replace(" ", "T").replace(":",".")
    try {
     // val outputStream = new FileOutputStream(latestFile+time+".xls")
      val outputStream = new FileOutputStream(latestFile.substring(0,latestFile.indexOf("."))+"_CompareData_"+time+".xls")

      try workbook.write(outputStream)
      finally if (outputStream != null) outputStream.close()
    }

    //TODO invoke send mail method in utils with right params
    //TODO uncomment below line

    //Utils.sendMail(properties.getProperty("mail.id"),inputProperties.getProperty("mail.from"),inputProperties.getProperty("mail.subject"), "Test",inputProperties.getProperty("mail.host"), latestFile+time+".xls",properties,latestFile)
    /*log.info("mail.id------------------->"+properties.getProperty(SC.MAIL_ID))
    log.info("SC.APP_NAME--------------->"+properties.getProperty(SC.APP_NAME))
    log.info("SC.HIVE_DATABASE--------------->"+properties.getProperty(SC.HIVE_DATABASE))
    log.info("mail.from----------------->"+inputProperties.getProperty("mail.from"))
    log.info("mail.subject----------------->"+inputProperties.getProperty("mail.subject"))
    log.info("mail.host----------------->"+inputProperties.getProperty("mail.host"))*/

    //Utils.sendMail(properties.getProperty(SC.MAIL_ID),inputProperties.getProperty("mail.from"),inputProperties.getProperty("mail.subject"), "Test",inputProperties.getProperty("mail.host"), latestFile+time+".xls",properties,latestFile)
    //Utils.deleteOutputFile(latestFile+time+".xls")
    log.info("Sending mail"+properties.getProperty(SC.MAIL_ID))
    Utils.sendMail(properties.getProperty(SC.MAIL_ID),inputProperties.getProperty("mail.from"),inputProperties.getProperty("mail.subject"), "Test",inputProperties.getProperty("mail.host"), latestFile.substring(0,latestFile.indexOf("."))+"_CompareData_"+time+".xls",properties,latestFile)
    log.info(s"Deleting output file ${latestFile.substring(0,latestFile.indexOf("."))}_CompareData_$time.xls")
    Utils.deleteOutputFile(latestFile.substring(0,latestFile.indexOf("."))+"_CompareData_"+time+".xls")
    log.info("Copying file to archive directory")
    HdfsUtils.archiveFile(inputProperties.getProperty(SC.SOURCE_FILE_PATH), inputProperties.getProperty(SC.ARCHIVE_DIRECTORY), spark)

  }

  //TODO check if its working as expected
  def getHiveDFWithCommentsAsCol(hiveDF: DataFrame, csvDF: DataFrame): DataFrame = {
    val commentsMap = hiveDF.schema.map(field => (field.getComment(), field.name)).toMap
    var newDF = hiveDF
    for(i <- csvDF.columns) {
      var tempDF = newDF
      if(commentsMap.contains(Some(i))) {
        tempDF = newDF.withColumnRenamed(commentsMap.get(Some(i)).get, i)
      }
      newDF = tempDF
    }
    newDF
  }

  def getPropFromExcelSheetUsingSpark(latestFile: String, sourcePath: String): Properties = {

    log.info("Latest file------------------------------------------>"+latestFile)
    log.info("sourcePath------------------------------------------->"+sourcePath)
    val props = spark.read.format("com.crealytics.spark.excel")
      .option("dataAddress", "'Sheet2'!A1")
      .option("header", "false")
      .option("inferSchema", true)
      .option("location", sourcePath)
      .option("treatEmptyValuesAsNulls", true)
      .load(sourcePath+latestFile).take(110).map(r => r.toString().split("=")).map(a => (a(0), a(1)));
      log.info("properties map------------------------------>"+props)

    val properties = new Properties
    props.foreach { case (key, value) => properties.setProperty(key.replace("[",""), value.toString.replace("]","")) }
   // properties.putAll(props.mapValues(_.toString).asJava)

    log.info("properties---------------------------->"+properties)
    properties

  }

  def getDFExcelSheetUsingSpark(latestFile: String, sourcePath: String): DataFrame = {
    val df = spark.read.format("com.crealytics.spark.excel")
      .option("sheetName", "Sheet1")
      .option("header", "true")
      .option("location", sourcePath)
      .option("treatEmptyValuesAsNulls", true)
      .load(sourcePath+latestFile)
    df
  }

}
