package com.company.comparator

import java.io.BufferedReader
import java.util.Properties
import  org.apache.spark.util.SignalUtils
import org.junit._
import Assert._
import com.company.comparator.Main.{getClass, log}
import com.company.comparator.spark.HiveCSVComparator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source.fromURL

class HiveCSVComparatorTest {

  val log: Logger = LoggerFactory.getLogger(this.getClass.getName)
  var properties: Properties = null
  var spark: SparkSession = null

  @Before
  def setup(): Unit = {

    val url = getClass.getResource("/input_test.properties")
    var reader: BufferedReader = null
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
    spark = SparkSession.builder()
      .appName("HiveCSVDataComparatorTest")
      .config("spark.sql.caseSensitive", false)
      .master("local")
      .getOrCreate()

    log.info("spark config sss {}",spark.conf)
  }

  @Test
  def testCompareData(): Unit = {

    log.info("Test Properties, [{}]", properties)

    val hiveData = Seq(("a","A",1,3,6,2),("b","B",2,5,4,9),("c","C",3,1,3,1),("d","D",4,3,4,5),("e","E",5,4,1,2),("f","F",6,5,7,9))

    val csvData = Seq(("a","A",1,3,6,2,5),("b","B",2,5,4,3,9),("c","C",3,2,1,3,1),("d","D",4,3,3,4,5),("g","G",7,4,7,2,1),("h","H",8,5,2,2,8),("i","I",9,5,1,1,6))

    val hiveDF = spark.createDataFrame(spark.sparkContext.parallelize(hiveData)).toDF("P1","P2","P3","C4","C5","C6")

    val csvDF = spark.createDataFrame(spark.sparkContext.parallelize(csvData)).toDF("P1","P2","P3","C4","C7","C8","C9")

    val hiveCSVComparator = new HiveCSVComparator(spark, properties)

    hiveCSVComparator.compareDF(hiveDF, csvDF, properties,"Test")

  }

  @Ignore
  @Test
  def testCompareData2(): Unit = {
    //primary.keys=sls_qtn_id
    log.info("Test Properties, [{}]", properties)

    val hiveData = Seq(("234429188","1","NGQ - Partner","NQ01500959"),
      ("235884127","1","NGQ - Partner","NQ01881112"),
      ("234417919","1","NGQ - Partner","NQ01497110"),
      ("235885992","1","Eclipse","97840025"),
      ("235824526","2","Eclipse","97828923"),
      ("235885995","1","Eclipse","1100048397"),
      ("234426164","1","NGQ - Partner","NQ01499942"),
      ("234426742","1","NGQ - Partner","NQ01500099"),
      ("235877634","1","NGQ - Partner","NQ01878433"),
      ("235882193","2","Eclipse","97839562"),
      ("234245666","2","NGQ - Partner","NQ01452228"),
      ("234401715","1","NGQ - Partner","NQ01494659"))

    val hiveDF = spark.createDataFrame(spark.sparkContext.parallelize(hiveData)).toDF("sls_qtn_id","sls_qtn_vrsn_sqn_nr","orig_asset","asset_quote_n")

    val csvData = Seq(("234429188","1","NGQ - Partner","1"),
      ("235884127","1","NGQ - Partner","1"),
      ("234417919","1","NGQ - Partner","1"),
      ("235885992","1","Eclipse","1"),
      ("235824526","2","Eclipse","1"),
      ("234435261","1","NGQ - Partner","1"),
      ("235884607","2","Eclipse","1"))

    val csvDF = spark.createDataFrame(spark.sparkContext.parallelize(csvData)).toDF("sls_qtn_id","sls_qtn_vrsn_sqn_nr","orig_asset","asset_quote_vrsn")
    val hiveCSVComparator = new HiveCSVComparator(spark, properties)

    hiveCSVComparator.compareDF(hiveDF, csvDF, properties,"Test")

  }


  @Test
  def testCompareData3(): Unit = {
    //primary.keys=sls_qtn_id
    log.info("Test Properties, [{}]", properties)

    //primary.keys=qv.SLS_QTN_ID

    val hiveData = Seq(("235840575","NGQ","4851212.11","ngqc_system@hpe.com"),
      ("235879190","NGQ","50027641","ngqc_system@hpe.com"),
      ("235879303","NGQ","50027651","ngqc_system@hpe.com"),
      ("235872704","NGQ","50060001","ngqc_system@hpe.com"),
      ("235873401","NGQ","50060041","ngqc_system@hpe.com"),
      ("235872768","NGQ","50060071","ngqc_system@hpe.com"),
      ("235872311","NGQ","50060921","ngqc_system@hpe.com"),
      ("235872772","NGQ","50060971","ngqc_system@hpe.com"),
      ("235876896","NGQ","50185121","ngqc_system@hpe.com"),
      ("235877175","NGQ","50189161","ngqc_system@hpe.com"),
      ("235876954","NGQ","50193211","ngqc_system@hpe.com"),
      ("235876977","NGQ","50194661","ngqc_system@hpe.com"),
      ("235877489","NGQ","50194961","ngqc_system@hpe.com"),
      ("235877473","NGQ","50196991","ngqc_system@hpe.com"),
      ("235877506","NGQ","50197291","ngqc_system@hpe.com"))

    val hiveDF = spark.createDataFrame(spark.sparkContext.parallelize(hiveData)).toDF("SLS_QTN_ID","ORIG_ASSET","ASSET_QUOTE_NR","CREATION_PERSON_ID")

    val csvData = Seq(("235872768","1","NGQ","50060071","1","ngqc_system@hpe.com"),
      ("235872311","1","NGQ","50060921","1","ngqc_system@hpe.com"),
      ("235872772","1","NGQ","50060971","1","ngqc_system@hpe.com"),
      ("235876896","1","NGQ","50185121","1","ngqc_system@hpe.com"),
      ("235877175","1","NGQ","50189161","1","ngqc_system@hpe.com"),
      ("235876954","1","NGQ","50193211","1","ngqc_system@hpe.com"),
      ("235876977","1","NGQ","50194661","1","ngqc_system@hpe.com"),
      ("235877489","1","NGQ","50194961","1","ngqc_system@hpe.com"),
      ("235877473","1","NGQ","50196991","1","ngqc_system@hpe.com"),
      ("235877506","1","NGQ","50197291","1","ngqc_system@hpe.com"),
      ("235877590","1","NGQ","50197301","1","ngqc_system@hpe.com"),
      ("235877738","1","NGQ","5019731.11","1","ngqc_system@hpe.com"),
      ("235877528","1","NGQ","5019731.21","1","ngqc_system@hpe.com"),
      ("235877610","1","NGQ","50197491","1","ngqc_system@hpe.com"),
      ("235877513","1","NGQ","50197501","1","ngqc_system@hpe.com"))

    val csvDF = spark.createDataFrame(spark.sparkContext.parallelize(csvData)).toDF("SLS_QTN_ID","SLS_QTN_VRSN_SQN_NR","ORIG_ASSET","ASSET_QUOTE_NR","ASSET_QUOTE_VRSN","CREATION_PERSON_ID")


    val hiveCSVComparator = new HiveCSVComparator(spark, properties)

    hiveCSVComparator.compareDF(hiveDF, csvDF, properties,"Test")

  }

  @Test
  def testCompareData4(): Unit = {
    //primary.keys=sls_qtn_id
    log.info("Test Properties, [{}]", properties)
    //primary.keys=qv.SLS_QTN_ID

    val hiveData = Seq(("230938790","1","EscalatePricing","TRUE"),
      ("230938790","1","BundleCheckFlag","TRUE"),
      ("230938790","1","DataCheckFlag","TRUE"),
      ("230938790","1","Prevalidate","TRUE"),
      ("230938790","1","opgCreated","TRUE"),
      ("230938790","1","AddToExistingDealFlg","FALSE"),
      ("230938790","1","AllowOpenMarket","TRUE"),
      ("230938790","1","Bundled","TRUE"),
      ("230938790","1","BundledTax","FALSE"),
      ("230938790","1","CarePckSupresion","TRUE"),
      ("230938790","1","ClicCheckFlag","FALSE"),
      ("230938790","1","ComplShip","TRUE"),
      ("230938790","1","ConfigCheckFlag","FALSE"),
      ("230938790","1","ConsBill","FALSE"),
      ("230938790","1","CustConfigChk","FALSE"),
      ("230938790","1","CustmrAccept","FALSE"),
      ("230938790","1","CustomerFlag","TRUE"),
      ("230938790","1","ExemptFreight","FALSE"),
      ("230938790","1","GlobalPricing","FALSE"),
      ("230938790","1","GovtLLC","FALSE"),
      ("230938790","1","IncludeCmtInOutput","FALSE"),
      ("230938790","1","InvoiceHold","FALSE"),
      ("230938790","1","IsMultiQuoteDealFlag","FALSE"),
      ("230938790","1","LPOnlyDisabled","TRUE"),
      ("230938790","1","Leasing","FALSE"),
      ("230938790","1","MCCSuppession","TRUE"),
      ("230938790","1","NormPo","0"),
      ("230938790","1","OfflineApproval","FALSE"),
      ("230938790","1","PriceCheckFlag","TRUE"),
      ("230938790","1","ProductCheckFlag","FALSE"),
      ("230938790","1","RepricingRequired","FALSE"),
      ("230938790","1","SOOverriden","TRUE"),
      ("230938790","1","ServiceOnlyOrder","FALSE"),
      ("230938790","1","ShipHandExemption","FALSE"),
      ("230938790","1","ShipHandOverride","FALSE"),
      ("230938790","1","ShippingCallRequired","FALSE"),
      ("230938790","1","SpecHandExemption","FALSE"),
      ("230938790","1","SpecialPricingFlag","FALSE"),
      ("230938790","1","TAACompliance","FALSE"),
      ("230938790","1","TacPricing","FALSE"),
      ("230938790","1","VerifiedCustomer","TRUE"),
      ("230938790","1","bidDeskEscalation","FALSE"),
      ("230938790","1","customerEligible","TRUE"),
      ("230938790","1","doEligibilityChecks","TRUE"),
      ("230938790","1","doTSReporting","TRUE"),
      ("230938790","1","eduCustomer","FALSE"),
      ("230938790","1","eeaPresent","FALSE"),
      ("230938790","1","excelForOption","FALSE"),
      ("230938790","1","fallBackLogicEnabled","FALSE"),
      ("230938790","1","higherVrsnProdDisc","FALSE"))

    val hiveDF = spark.createDataFrame(spark.sparkContext.parallelize(hiveData)).toDF("SLS_QTN_ID","SLS_QTN_VRSN_SQN_NR","FLG_TYP","FLG_VL")

    val csvData = Seq(("230938790","1","EscalatePricing","TRUE"),
      ("230938790","1","BundleCheckFlag","TRUE"),
      ("230938790","1","DataCheckFlag","TRUE"),
      ("230938790","1","Prevalidate","TRUE"),
      ("230938790","1","opgCreated","TRUE"),
      ("230938790","1","AddToExistingDealFlg","FALSE"),
      ("230938790","1","AllowOpenMarket","TRUE"),
      ("230938790","1","Bundled","TRUE"),
      ("230938790","1","BundledTax","FALSE"),
      ("230938790","1","CarePckSupresion","TRUE"),
      ("230938790","1","ClicCheckFlag","FALSE"),
      ("230938790","1","ComplShip","TRUE"),
      ("230938790","1","ConfigCheckFlag","FALSE"),
      ("230938790","1","ConsBill","FALSE"),
      ("230938790","1","CustConfigChk","FALSE"),
      ("230938790","1","CustmrAccept","FALSE"),
      ("230938790","1","CustomerFlag","TRUE"),
      ("230938790","1","ExemptFreight","FALSE"),
      ("230938790","1","GlobalPricing","FALSE"),
      ("230938790","1","GovtLLC","FALSE"),
      ("230938790","1","IncludeCmtInOutput","FALSE"),
      ("230938790","1","InvoiceHold","FALSE"),
      ("230938790","1","IsMultiQuoteDealFlag","FALSE"),
      ("230938790","1","LPOnlyDisabled","TRUE"),
      ("230938790","1","Leasing","FALSE"),
      ("230938790","1","MCCSuppession","TRUE"),
      ("230938790","1","NormPo","0"),
      ("230938790","1","OfflineApproval","FALSE"),
      ("230938790","1","PriceCheckFlag","TRUE"),
      ("230938790","1","ProductCheckFlag","FALSE"),
      ("230938790","1","RepricingRequired","FALSE"),
      ("230938790","1","SOOverriden","TRUE"),
      ("230938790","1","ServiceOnlyOrder","FALSE"),
      ("230938790","1","ShipHandExemption","FALSE"),
      ("230938790","1","ShipHandOverride","FALSE"),
      ("230938790","1","ShippingCallRequired","FALSE"),
      ("230938790","1","SpecHandExemption","FALSE"),
      ("230938790","1","SpecialPricingFlag","FALSE"),
      ("230938790","1","TAACompliance","FALSE"),
      ("230938790","1","TacPricing","FALSE"),
      ("230938790","1","VerifiedCustomer","TRUE"),
      ("230938790","1","bidDeskEscalation","FALSE"),
      ("230938790","1","customerEligible","TRUE"),
      ("230938790","1","doEligibilityChecks","TRUE"),
      ("230938790","1","doTSReporting","TRUE"),
      ("230938790","1","eduCustomer","FALSE"),
      ("230938790","1","eeaPresent","FALSE"),
      ("230938790","1","excelForOption","FALSE"),
      ("230938790","1","fallBackLogicEnabled","FALSE"),
      ("230938790","1","higherVrsnProdDisc","FALSE"))

    val csvDF = spark.createDataFrame(spark.sparkContext.parallelize(csvData)).toDF("SLS_QTN_ID","SLS_QTN_VRSN_SQN_NR","FLG_TYP","FLG_VL")

    val hiveCSVComparator = new HiveCSVComparator(spark, properties)

    hiveCSVComparator.compareDF(hiveDF, csvDF, properties,"Test")

  }




}


