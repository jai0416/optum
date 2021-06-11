package com.company.comparator.factory

import java.util.Properties

import com.company.comparator.utils.{StringConstants => SC}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  var sparkSession: Option[SparkSession] = null
  def getSparkSession(): SparkSession = {
    if(sparkSession==null || !sparkSession.isDefined) {
      init()
    }
    sparkSession.get
  }

  def init(): Unit = {
    sparkSession = Some(SparkSession.builder().appName("Test").enableHiveSupport().getOrCreate())

   // sparkSession = Some(SparkSession.builder().appName(properties.getProperty(SC.APP_NAME)).enableHiveSupport().getOrCreate())
  }

}
