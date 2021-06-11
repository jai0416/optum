package com.company.comparator.utils

import java.io.{File, FileOutputStream}
import java.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.commons.lang.StringUtils
import org.apache.poi.hssf.usermodel.{HSSFSheet, HSSFWorkbook}
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}
import javax.activation.{DataHandler, DataSource, FileDataSource}
import javax.mail.internet.{MimeBodyPart, MimeMultipart}
import org.apache.poi.xssf.usermodel.XSSFSheet

object Utils {

  val log: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def getNameFromPath(properties: Properties): String = {
    //TODO decide What if they just give a path only
    val filePath = properties.getProperty(StringConstants.CSV_FILE_PATH)
    val fileName = FilenameUtils.getName(filePath)
    if(StringUtils.isEmpty(fileName)) {
      val fileName = properties.getProperty(StringConstants.HIVE_TABLE)+"_CSV"
      log.info("Provided path without any filename, will create csv table name as : [{}]", fileName)
      fileName
    } else {
      fileName.substring(0,fileName.indexOf("."))
    }
  }

  def getFullPath(properties: Properties): String = {
    val inputFilePath = properties.getProperty(StringConstants.CSV_FILE_PATH)
    val filePath = FilenameUtils.getFullPath(inputFilePath)
    if(StringUtils.isEmpty(filePath)) {
      log.error("Invalid input for csv path: [{}]", filePath)
      throw new RuntimeException("Invalid input for csv path: "+filePath)
    } else {
      filePath
    }
  }

  def sendMail(mailTo:String, mailFrom:String, mailSubject:String, mailContent:String, mailHost:String, fileName: String, inputproperties: Properties,latestFile: String)= {
    import java.util.Properties

    import javax.mail.{Message, Session, Transport}
    import javax.mail.internet.{InternetAddress, MimeMessage};

    var properties: Properties = System.getProperties();
    properties.setProperty("mail.smtp.host", mailHost)
    var session: Session = Session.getDefaultInstance(properties)
    try {
      var message: MimeMessage = new MimeMessage(session)
      message.setFrom(new InternetAddress(mailFrom))
      var mailRecipient = mailTo.split(",")
      for (mailto <- mailRecipient) {
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(mailto))
      }
      message.setSubject(mailSubject)

      var messageBodyPart = new MimeBodyPart()
      messageBodyPart.setText(s"""Data Comparision results for input file $latestFile and hive table ${inputproperties.getProperty(StringConstants.HIVE_DATABASE)}.${inputproperties.getProperty(StringConstants.HIVE_TABLE)}""")
      val messagemultipart = new MimeMultipart()
      messagemultipart.addBodyPart(messageBodyPart)
      messageBodyPart = new MimeBodyPart()
      val source = new FileDataSource(fileName)
      messageBodyPart.setDataHandler(new DataHandler(source))
      messageBodyPart.setFileName(fileName)
      messagemultipart.addBodyPart(messageBodyPart)

      message.setContent(messagemultipart)
      Transport.send(message)
    } catch {
      case ex: Exception => {
        log.error("Exception while sending mail.")
        throw new RuntimeException("Error while sending mail with results")
      }
    }
  }

  def writeToExcel(sheet: XSSFSheet, col1: Array[String], col2: Array[String]): Unit = {

    var rowCount = 0

    val length = math.max(col1.length, col2.length)
    var columnCount = 0
    val row = sheet.createRow(rowCount)
    val header1 = row.createCell(columnCount)
    columnCount += 1
    header1.setCellValue("Present Columns")
    val header2 = row.createCell(columnCount)
    columnCount = 0
    header2.setCellValue("Not present columns")
    rowCount += 1
    for(i <- 0 until length) {
      val row = sheet.createRow(rowCount)
      val column1 = row.createCell(columnCount)
      columnCount += 1
      if(rowCount <= col1.length) {
        column1.setCellValue(col1(rowCount-1))
      } else {
        column1.setCellValue("")
      }
      val column2 = row.createCell(columnCount)
      columnCount = 0
      if(rowCount <= col2.length) {
        column2.setCellValue(col2(rowCount-1))
      } else {
        column2.setCellValue("")
      }
      rowCount += 1
    }
  }

  def writeDataSetToExcelMultiColumn(sheet: XSSFSheet, csvColumns: Array[String], data: Array[Row]): Unit = {
    var rowCount = 0
    var columnCount = 0
    val row = sheet.createRow(rowCount)

    for(i <- 0 until csvColumns.length) {
      val header = row.createCell(i)
      header.setCellValue(csvColumns(i))
    }
    rowCount += 1
    for(r <- data) {
      val row = sheet.createRow(rowCount)
      for(i <- 0 until csvColumns.length) {

        val header = row.createCell(columnCount)
        if(r.get(i) != null) {
          if(r.get(i).isInstanceOf[String]) {
            header.setCellValue(r.get(i).asInstanceOf[String])
          } else if (r.get(i).isInstanceOf[Int]) {
            header.setCellValue(r.get(i).toString.toDouble)
          }
        } else {
          header.setCellValue("null")
        }
        columnCount += 1
      }
      rowCount += 1
      columnCount = 0
    }
  }

  def writeDataSetToExcel(sheet: HSSFSheet, csvColumns: Array[String], data: Array[Row], specificData: Array[Row]): Unit = {

    var rowCount = 0

    val length = math.max(data.length, specificData.length)
    var columnCount = 0
    val row = sheet.createRow(rowCount)
    val header1 = row.createCell(columnCount)
    columnCount += 1
    header1.setCellValue("Primary Key Data present")
    val header2 = row.createCell(columnCount)
    columnCount = 0
    header2.setCellValue("Primary Key Data not present")
    rowCount += 1
    for(i <- 0 until length) {
      val row = sheet.createRow(rowCount)
      val column1 = row.createCell(columnCount)
      columnCount += 1
      if(rowCount <= data.length) {
        if(data(i).get(0) != null) {
          if(data(i).get(0).isInstanceOf[String]) {
            column1.setCellValue(data(i).get(0).asInstanceOf[String])
          } else if (data(i).get(0).isInstanceOf[Int]) {
            column1.setCellValue(data(i).get(0).toString.toDouble)
          }
        } else {
          column1.setCellValue("null")
        }
      } else {
        column1.setCellValue("")
      }
      val column2 = row.createCell(columnCount)
      columnCount = 0
      if(rowCount <= specificData.length) {
        if(specificData(i).get(0) != null) {
          if(specificData(i).get(0).isInstanceOf[String]) {
            column2.setCellValue(specificData(i).get(0).asInstanceOf[String])
          } else if (specificData(i).get(0).isInstanceOf[Int]) {
            column2.setCellValue(specificData(i).get(0).toString.toDouble)
          }
        } else {
          column2.setCellValue("null")
        }
      } else {
        column2.setCellValue("")
      }
      rowCount += 1
    }
  }

  def writeCountsToExcel(sheet: XSSFSheet, csvColumnsCount: Int, hiveColumnsCount: Int) {
    var rowCount = 0
    var columnCount = 0
    val row = sheet.createRow(rowCount)
    val header1 = row.createCell(columnCount)
    columnCount += 1
    header1.setCellValue("Type of Table")
    val header2 = row.createCell(columnCount)
    columnCount = 0
    header2.setCellValue("No of Columns")
    rowCount += 1

    val row2 = sheet.createRow(rowCount)
    val header3 = row2.createCell(columnCount)
    columnCount += 1
    header3.setCellValue("Hive Table")
    val header4 = row2.createCell(columnCount)
    columnCount = 0
    header4.setCellValue(hiveColumnsCount)
    rowCount += 1

    val row3 = sheet.createRow(rowCount)
    val header5 = row3.createCell(columnCount)
    columnCount += 1
    header5.setCellValue("CSV Table")
    val header6 = row3.createCell(columnCount)
    columnCount = 0
    header6.setCellValue(csvColumnsCount)
    rowCount += 1

  }

  def writePercentageToExcel(sheet: XSSFSheet, commonDataCount: Double, csvSpecificData: Double) {
    var rowCount = 0
    var columnCount = 0
    val row = sheet.createRow(rowCount)
    val header1 = row.createCell(columnCount)
    columnCount += 1
    header1.setCellValue("Type")
    val header2 = row.createCell(columnCount)
    columnCount = 0
    header2.setCellValue("Percentage")
    rowCount += 1

    val row2 = sheet.createRow(rowCount)
    val header3 = row2.createCell(columnCount)
    columnCount += 1
    header3.setCellValue("Matching Data")
    val header4 = row2.createCell(columnCount)
    columnCount = 0
    var matching = ((commonDataCount/(commonDataCount+csvSpecificData)) * 100)
    matching = BigDecimal(matching).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    log.info("Matching: {}", matching)
    header4.setCellValue(matching+"%")
    rowCount += 1

    val row3 = sheet.createRow(rowCount)
    val header5 = row3.createCell(columnCount)
    columnCount += 1
    header5.setCellValue("Non Matching Data")
    val header6 = row3.createCell(columnCount)
    columnCount = 0
    var nonMatching = ((csvSpecificData/(commonDataCount+csvSpecificData)) * 100)
    nonMatching = BigDecimal(nonMatching).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    log.info("notMatching: {}", nonMatching)
    header6.setCellValue(nonMatching+"%")
    rowCount += 1
  }

  def deleteOutputFile(filePath: String): Boolean = {
    val fileToDelete = FileUtils.getFile(filePath)
    val result: Boolean = FileUtils.deleteQuietly(fileToDelete)
    result
  }

}
