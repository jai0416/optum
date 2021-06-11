package com.company.comparator

import java.io.{BufferedReader, File, FileInputStream}
import java.net.URL
import java.util.Properties

import com.company.comparator.factory.SparkSessionFactory
import com.company.comparator.spark.HiveCSVComparator
import org.apache.hadoop.hdfs.client.HdfsUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source._

object Main {
  val log: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    var is: FileInputStream=null
    //log.info("arrguments passed {},{}",args.length,args(0))
   // val propertiesFilePath = String.valueOf(args(0).trim())
   /* var url: URL = null
    if(args.length > 0) {
      //url = getClass.getResource(args(0))
      val file = new File(args(0))
      val calss = classOf[HiveCSVComparator]
       is = new FileInputStream(file)

    } else {
      url = getClass.getResource("/input.properties")

      /*url = getClass.getResource(propertiesFilePath)*/
    }
    var reader: BufferedReader = null
    var properties: Properties = null
    if (url != null) {
      try {
        val source = fromURL(url)
        log.info("Loading properties file : [{}]",url.toString)
        properties = new Properties()
        reader = source.bufferedReader()
        properties.load(reader)
        println(properties.toString)
        log.info("Properties passed: {}",properties.toString)
      } catch {
        case e: Exception => log.error("Failed to read properties file {}",e)
      } finally {
        if (reader != null) {
          reader.close()
        }
      }
    }
    else {
      log.info("loading properties from external path")
      properties =new Properties()
      properties.load(is)

    }*/
    //Trigger hive to csv data comparision

    process(args(0))
  }

  def process(propertiespath: String): Unit = {
    val spark: SparkSession = SparkSessionFactory.getSparkSession()
   val properties= utils.HdfsUtils.readPropertiesFile(propertiespath,spark)
    val comparator = new HiveCSVComparator(spark, properties)
    comparator.compareHiveCSVData()
  }
}
