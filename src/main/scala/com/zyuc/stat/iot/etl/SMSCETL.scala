package com.zyuc.stat.iot.etl

import com.zyuc.stat.iot.etl.CDRETL.logInfo
import com.zyuc.stat.iot.etl.util.CDRConverterUtils.logError
import com.zyuc.stat.iot.etl.util.{CDRConverterUtils, CommonETLUtils}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.{makeCoalesce, renameHDFSDir}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.tools.scalap.scalax.util.StringUtil

/**
  * Created by mengjd
  */
object SMSCETL extends Logging{

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("smsc")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    //val loadTime = "201709271456"
    val loadTime = sc.getConf.get("spark.app.loadTime", "201709271456") //
    val inputPath = "hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/smsc/srcdata/smsc/"
    val outputPath = "hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/smsc/output/smsc/"

    val appName = "smsc_" + loadTime
    val fileWildcard = "*smsc*"
    val coalesceSize = 128
    val logType = "smsc"
    val logTableName = "iot_cdr_data_smsc"
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    doJob(sqlContext, fileSystem, appName, loadTime, inputPath, outputPath, fileWildcard, coalesceSize, logType, logTableName)
  }

  def doJob(parentContext: SQLContext, fileSystem:FileSystem, appName:String, loadTime:String, inputPath:String, outputPath:String, fileWildcard:String, coalesceSize:Int, logType:String, logTableName:String):String = {
    //创建session
    val sqlContext = parentContext.newSession()
    //切换数据库
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    //第一步：校验参数
    var msg =validate(fileSystem, appName, loadTime, inputPath, outputPath, fileWildcard, coalesceSize, logType, logTableName)
    if(!msg.isEmpty){
      return msg;
    }
    //第二步：更改文件目录名称
    val srcDoingLocation = inputPath + "/" + loadTime + "_doing"
    val srcLocation = inputPath + "/" + loadTime
    val isRename = renameHDFSDir(fileSystem, srcLocation, srcDoingLocation)
    var result = "Success"
    if (!isRename) {
      result = "Failed"
      logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)
      return "appName:" + appName + ": " + s"$srcLocation rename to $srcDoingLocation :" + result + ". "
    }
    logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)

    //数据来源
    val smssLocation = srcDoingLocation + "/" + fileWildcard
    val smssFileExists = if (fileSystem.globStatus(new Path(smssLocation)).length > 0) true else false

    if (!smssFileExists) {
      logInfo("No Files during time: " + loadTime)
      // System.exit(1)
      return "appName:" + appName + ":No Files ."
    }

    //第三步：清洗数据，转换成dataframe
    val smscLocation = srcDoingLocation + "/" + fileWildcard
    val srcSmscDF = sqlContext.read.format("json").load(smscLocation)
    val smscDF = parseETL(srcSmscDF, logType)
    if (smscDF == null) {
      logInfo(s"$smscLocation , data file Exists, but no data in file")
      return s"$smscLocation  , data file Exists, but no data in file"
    }
    //第四步：分片数据文件并入HDFS
    // 计算cloalesce的数量
    val coalesceNum = makeCoalesce(fileSystem, srcDoingLocation, coalesceSize)
    logInfo(s"$inputPath , $coalesceSize, $coalesceNum")

    // 结果数据分区字段
    val partitions = "d,h,m5"
    // 将数据存入到HDFS， 并刷新分区表
    val executeResult = CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, smscDF, coalesceNum, partitions, loadTime, outputPath, logTableName, appName)
    //第五步：更改源数据文件名称，标志读取已读
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

  /**
    * 校验参数
    * @param fileSystem
    * @param appName
    * @param loadTime
    * @param inputPath
    * @param outputPath
    * @param fileWildcard
    * @param coalesceSize
    * @param logType
    * @param logTableName
    * @return
    */
  def validate(fileSystem:FileSystem, appName:String, loadTime:String, inputPath:String, outputPath:String, fileWildcard:String, coalesceSize:Int, logType:String, logTableName:String): String ={
    var msg : String = "";
    if(appName.isEmpty){
      val logTypeErr = "[" + appName + "] 应用名称为空 "+inputPath+"\n"
      logError(logTypeErr)
      msg += logTypeErr
    }
    if(loadTime.isEmpty){
      val logTypeErr = "[" + appName + "] 日志加载时间loadTime为空\n"
      logError(logTypeErr)
      msg += logTypeErr
    }
    if(inputPath.isEmpty){
      val logTypeErr = "[" + appName + "] 日志来源inputPath为空\n"
      logError(logTypeErr)
      msg += logTypeErr
    }
    if(outputPath.isEmpty){
      val logTypeErr = "[" + appName + "] 日志清洗后导出outputPath为空\n"
      logError(logTypeErr)
      msg += logTypeErr
    }
    if(fileWildcard.isEmpty){
      val logTypeErr = "[" + appName + "] 日志fileWildcard为空\n"
      logError(logTypeErr)
      msg += logTypeErr
    }
    if(logType.isEmpty){
      val logTypeErr = "[" + appName + "] 日志类型logType错误, 期望值： smsc\n"
      logError(logTypeErr)
      msg += logTypeErr
    }
    if(logTableName.isEmpty){
      val logTypeErr = "[" + appName + "] 日志logTableName表名错误\n"
      logError(logTypeErr)
      msg += logTypeErr
    }

    msg
  }

  /**
    * 清洗数据
    * @param df
    * @param logType
    * @return
    */
  def parseETL(df: DataFrame, logType:String) = {
    var smscDF:DataFrame = null
    try{
      smscDF = df.selectExpr("concat('86',Called_Number,'') as Called_Number","Called_Type","concat('86',Calling_Number,'') as Calling_Number",
        "Calling_Type","CommitTime","FinishTime","Number_of_Transmission",
        "Result_of_First_Send","Result_of_Last_Send","Time_of_First_Issued",
        "substr(regexp_replace(CommitTime,'-',''),3,6) as d", "substr(CommitTime,12,2) as h",
        "lpad(floor(substr(CommitTime,15,2)/5)*5,2,'0') as m5"
      )
    }catch {
      case e:Exception=>{
        e.printStackTrace()
        logError(s"[ $logType ] 失败 处理异常 " + e.getMessage)
      }
    }
    smscDF;
  }

}
