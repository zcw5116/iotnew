package com.zyuc.stat.iot.etl

import com.zyuc.stat.iot.etl.secondary.AuthlogSecondETL.logInfo
import com.zyuc.stat.iot.etl.util.{CommonETLUtils, OperaLogConverterUtils}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.makeCoalesce
import com.zyuc.stat.utils.{CharacterEncodeConversion, FileUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

/**
  * Created by zhoucw on 17-7-25.
  */
object OperalogETL extends Logging{
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)


    val appName = sc.getConf.get("spark.app.name") // oper_2017073111
    // 操作时间
    val operaTime = sc.getConf.get("spark.app.operaTime")  // 20170723
    val pcrfPath = sc.getConf.get("spark.app.pcrfPath") // /hadoop/IOT/ANALY_PLATFORM/OperaLog/PCRF/
    val hssPath = sc.getConf.get("spark.app.hssPath")   // /hadoop/IOT/ANALY_PLATFORM/OperaLog/HSS/
    val hlrPath = sc.getConf.get("spark.app.hlrPath")  //  /hadoop/IOT/ANALY_PLATFORM/OperaLog/HLR/

    // userTable
    val userTable = sc.getConf.get("spark.app.user.tableName")     // "iot_customer_userinfo"
    val userTablePartitionDayid = sc.getConf.get("spark.app.user.userTablePartitionDayid")  //  "20170801"

    val operTable = sc.getConf.get("spark.app.operTable")  // iot_opera_log

    val pcrfWildcard = sc.getConf.get("spark.app.pcrfWildcard")  //  *dat*" + operaTime + "*"
    val hssWildcard = sc.getConf.get("spark.app.hssWildcard")   // operaTime + 1
    val hlrWildcard = sc.getConf.get("spark.app.hlrWildcard")  // operaTime => 2017-06-15
    val outputPath = sc.getConf.get("spark.app.outputPath")   // /hadoop/IOT/ANALY_PLATFORM/OperaLog/OutPut/
    val coalesceNum = sc.getConf.get("spark.app.coalesceNum", "1")// 1

    val pcrfLocation = pcrfPath + "/" + pcrfWildcard //
    val hssLocation = hssPath + "/" + hssWildcard // operaTime
    val hlrLocation = hlrPath + "/" + hlrWildcard  // operaTime

    val userDF = sqlContext.table(userTable).filter("d=" + userTablePartitionDayid).
      selectExpr("mdn", "imsicdma", "custprovince", "case when length(vpdncompanycode)=0 then 'N999999999' else vpdncompanycode end  as vpdncompanycode").
      cache()

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val partitions = "d" // 按日分区

    val pcrfFileExists = if (fileSystem.globStatus(new Path(pcrfLocation)).length > 0) true else false
    val hssFileExists = if (fileSystem.globStatus(new Path(hssLocation)).length > 0) true else false
    val hlrFileExists = if (fileSystem.globStatus(new Path(hlrLocation)).length > 0) true else false

    if (!pcrfFileExists && !hssFileExists && !hlrFileExists) {
      logInfo("No Files during time: " + operaTime)
      System.exit(1)
    }

    var operLogDF:DataFrame = null
    if(pcrfFileExists){
      val pcrfTmpDF = sqlContext.read.json(CharacterEncodeConversion.transfer(sc, pcrfLocation, "GBK"))
      val pcrfDF = OperaLogConverterUtils.parse(pcrfTmpDF, userDF, OperaLogConverterUtils.Oper_PCRF_PLATFORM)
      if(operLogDF == null){
        operLogDF = pcrfDF
      }else{
        operLogDF = operLogDF.unionAll(pcrfDF)
      }
    }

    if(hssFileExists){
      val hssTmpDF = sqlContext.read.json(CharacterEncodeConversion.transfer(sc, hssLocation, "GBK"))
      val hssDF = OperaLogConverterUtils.parse(hssTmpDF, userDF, OperaLogConverterUtils.Oper_HSS_PLATFORM)
      if(operLogDF == null){
        operLogDF = hssDF
      }else{
        operLogDF = operLogDF.unionAll(hssDF)
      }
    }

    if(hlrFileExists){
      val hlrTmpDF = sqlContext.read.json(CharacterEncodeConversion.transfer(sc, hlrLocation, "GBK"))
      val hlrDF = OperaLogConverterUtils.parse(hlrTmpDF, userDF, OperaLogConverterUtils.Oper_HLR_PLATFORM)
      if(operLogDF == null){
        operLogDF = hlrDF
      }else{
        operLogDF = operLogDF.unionAll(hlrDF)
      }
    }


    // 结果数据分区字段
   // val partitions = "d"
    // 将数据存入到HDFS， 并刷新分区表
    CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, operLogDF, coalesceNum.toInt , partitions, operaTime, outputPath, operTable, appName)

    sc.stop()
  }

}
