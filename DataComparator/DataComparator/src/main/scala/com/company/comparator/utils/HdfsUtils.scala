package com.company.comparator.utils

import java.util.Properties

import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object HdfsUtils {
  val log: Logger = LoggerFactory.getLogger(this.getClass.getName)
  def archiveFile(sourcePath: String, targetPath: String, spark: SparkSession): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    fs.exists(new Path(sourcePath))
    FileUtil.copy(fs, new Path(sourcePath+getLastModifiedFile(sourcePath,spark)), fs, new Path(targetPath), true, hadoopConf)
  }

  def readPropertiesFile(propertiesPath: String, spark: SparkSession): Properties = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    fs.exists(new Path(propertiesPath))
    val inputStream=fs.open(new Path(propertiesPath))
    val properties=new Properties()
    properties.load(inputStream)
    properties
  }



  def getLastModifiedFile(sourcePath: String, spark: SparkSession): String = {

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    fs.exists(new Path(sourcePath))

    val status = fs.listStatus(new Path(sourcePath))
    if(status.length<=0){
      throw new RuntimeException("Failed to process the file as input path is empty")
    }
    var max = 0L
    var latestFile = ""
    for(i <- status) {
      log.info("Path----------------------------------------->"+i.getPath)
      log.info("Path Name----------------------------------------->"+i.getPath.getName)
      if(i.isFile() && i.getModificationTime >= max) {

        if (i.getPath.getName.toLowerCase.startsWith("dq_check_")){
          latestFile = i.getPath.getName
          max = i.getModificationTime
        }
      }
    }
    if(latestFile==null || latestFile.isEmpty){
      throw new RuntimeException("Failed to process the file as input path don't have any valid files")
    }
    latestFile
  }
}
