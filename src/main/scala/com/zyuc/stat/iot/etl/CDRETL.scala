package com.zyuc.stat.iot.etl

import com.zyuc.stat.iot.etl.AuthLogETL.{doJob, logError, logInfo}
import com.zyuc.stat.iot.etl.MMELogETL.logInfo
import com.zyuc.stat.iot.etl.util.{AuthLogConverterUtils, CDRConverterUtils, CommonETLUtils}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.{makeCoalesce, renameHDFSDir}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-8-7.
  */
object CDRETL extends Logging{

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val loadTime = "201708071205"
    val inputPath = "hdfs://cdh-nn1:8020/hadoop/IOT/data/cdr/pdsn/srcdata/"
    val outputPath = "hdfs://cdh-nn1:8020/hadoop/IOT/data/cdr/output/pdsn/"

    //val inputPath = "hdfs://cdh-nn1:8020/hadoop/IOT/data/cdr/pgw/srcdata/"
    //val outputPath = "hdfs://cdh-nn1:8020/hadoop/IOT/data/cdr/pgw/output/"

    val appName = "pdsn_" + loadTime
   val fileWildcard = "*AAA*"
    //val fileWildcard = "*dat"

    val coalesceSize = 128

    val logType = "pdsn"
    //val logType = "pgw"

    val logTableName = "iot_cdr_data_pdsn"
    //val logTableName = "iot_cdr_data_pgw"

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    doJob(sqlContext, fileSystem, appName, loadTime, inputPath, outputPath, fileWildcard, coalesceSize, logType, logTableName)

  }

  def doJob(parentContext: SQLContext, fileSystem:FileSystem, appName:String, loadTime:String, inputPath:String, outputPath:String, fileWildcard:String, coalesceSize:Int, logType:String, logTableName:String):String = {
    val sqlContext = parentContext.newSession()
    val sc = parentContext.sparkContext

    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    if (logType != CDRConverterUtils.LOG_TYPE_HACCG && logType != CDRConverterUtils.LOG_TYPE_PDSN && logType != CDRConverterUtils.LOG_TYPE_PGW) {
      val logTypeErr = "[" + appName + "] 日志类型authlogType错误, 期望值： " + CDRConverterUtils.LOG_TYPE_HACCG + "," + CDRConverterUtils.LOG_TYPE_PDSN + "," + CDRConverterUtils.LOG_TYPE_PGW
      logError(logTypeErr)
      return logTypeErr
    }


    val srcLocation = inputPath + "/" + loadTime

    val fileExists = if (fileSystem.globStatus(new Path(srcLocation + "/*")).length > 0) true else false
    if (!fileExists) {
      logInfo(s"$srcLocation not exists.")
      return s"$srcLocation not exists."
    }

    val srcDoingLocation = inputPath + "/" + loadTime + "_doing"
    val isRename = renameHDFSDir(fileSystem, srcLocation, srcDoingLocation)
    var result = "Success"
    if (!isRename) {
      result = "Failed"
      logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)
      return "appName:" + appName + ": " + s"$srcLocation rename to $srcDoingLocation :" + result + ". "
    }
    logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)

    val cdrLocation = srcDoingLocation + "/" + fileWildcard
    val authFileExists = if (fileSystem.globStatus(new Path(cdrLocation)).length > 0) true else false

    if (!authFileExists) {
      logInfo("No Files during time: " + loadTime)
      // System.exit(1)
      return "appName:" + appName + ":No Files ."
    }

    val jsonRDD = sc.hadoopFile(cdrLocation,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],1).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    val srcCDRDF = sqlContext.read.json(jsonRDD)

    val cdrDF = CDRConverterUtils.parse(srcCDRDF, logType)
    if (cdrDF == null) {
      logInfo(s"$cdrLocation , data file Exists, but no data in file")
      return s"$cdrLocation  , data file Exists, but no data in file"
    }

    // authDF.show(200)

    // 计算cloalesce的数量
    val coalesceNum = makeCoalesce(fileSystem, srcDoingLocation, coalesceSize)
    logInfo(s"$inputPath , $coalesceSize, $coalesceNum")

    //  val logTableName="iot_auth_data_3gaaa"

    // 结果数据分区字段
    val partitions = "d,h,m5"
    // 将数据存入到HDFS， 并刷新分区表
    val executeResult = CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, cdrDF, coalesceNum, partitions, loadTime, outputPath, logTableName, appName)

    val srcDoneLocation = inputPath + "/" + loadTime + "_done"
    val isDoneRename = renameHDFSDir(fileSystem, srcDoingLocation, srcDoneLocation)
    var doneResult = "Success"
    if (!isDoneRename) {
      doneResult = "Failed"
      logInfo(s"$srcDoingLocation rename to $srcDoneLocation :" + doneResult)
      return "appName:" + appName + ": " + s"$srcDoingLocation rename to $srcDoneLocation :" + doneResult + ". "
    }
    logInfo(s"$srcDoingLocation rename to $srcDoneLocation :" + doneResult)

    executeResult
  }
}
