package com.zyuc.stat.iot.etl.secondary

import java.util.Date

import com.zyuc.stat.iot.etl.util.{CDRConverterUtils, CommonETLUtils}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.makeCoalesce
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created by zhoucw on 17-8-7.
  */
object CDRSecondETL extends Logging {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)


    val appName = sc.getConf.get("spark.app.name") // name_{}_2017073111
    val inputPath = sc.getConf.get("spark.app.inputPath") // "hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/data/cdr/output/pdsn/data/"
    val outputPath = sc.getConf.get("spark.app.outputPath") //"hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/data/cdr/secondaryoutput/pdsn/"
    var logType:String = sc.getConf.get("spark.app.item.type") // pdsn,pgw,haccg
    val userTable = sc.getConf.get("spark.app.user.table") //"iot_customer_userinfo"
    val userTablePatitionDayid = sc.getConf.get("spark.app.user.userTablePatitionDayid")
    val cdrFirstETLTable = sc.getConf.get("spark.app.table.source") // "iot_cdr_data_pdsn"
    val cdrSecondaryETLTable = sc.getConf.get("spark.app.table.stored") // "iot_cdr_data_pdsn_h"
    val hourid = sc.getConf.get("spark.app.timeid")//yyyymmddhhmiss
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt //128


    //val appName = "name_2017081001"
    //val appName = "name_2017080922"
/*    val appName = "name_2017080922"
    val inputPath = "hdfs://cdh-nn1:8020/tmp/cdr/output/pgw/data/"
    //val inputPath = "hdfs://cdh-nn1:8020/tmp/cdr/output/pgw/data/"
    val outputPath = "hdfs://cdh-nn1:8020/hadoop/IOT/data/cdr/secondaryoutput/"
    // val logType = "pdsn" // pdsn pgw haccg
    val logType = "pgw"
    val userTable = "iot_customer_userinfo" //"iot_customer_userinfo"
    val userTablePatitionDayid = "20170801"
    // val cdrFirstETLTable = "iot_cdr_data_pdsn" // "iot_cdr_data_pdsn"
    val cdrSecondaryETLTable = "iot_cdr_data_pdsn_day" // "iot_cdr_data_pdsn_day"
    val coalesceSize = 128 //128*/

    if (logType != CDRConverterUtils.LOG_TYPE_PGW && logType != CDRConverterUtils.LOG_TYPE_PDSN && logType != CDRConverterUtils.LOG_TYPE_HACCG) {
      logError("[" + appName + "] 日志类型logType错误, 期望值： " + CDRConverterUtils.LOG_TYPE_PGW + "," + CDRConverterUtils.LOG_TYPE_PDSN + "," + CDRConverterUtils.LOG_TYPE_HACCG)
      return
    }


    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val partitionD = hourid.substring(2, 8)
    val partitionH = hourid.substring(8,10)
    // cdr第一次清洗保存到位置
    val inputLocation = inputPath + "/d=" + partitionD + "/h=" + partitionH

    try {


      var begin = new Date().getTime

      // disable type inference
      sqlContext.setConf("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      // cdr第一次清洗保存到位置
      val cdrDF = sqlContext.read.format("orc").load(inputLocation)
        //.filter("d=" + partitionD).filter("h=" + partitionH)


      val userDF = sqlContext.table(userTable).filter("d=" + userTablePatitionDayid).
        selectExpr("mdn", "imsicdma", "belo_prov as custprovince", "case when length(companycode)=0 then 'P999999999' else companycode end  as vpdncompanycode").
        cache()


      def getAuthLogDF(cdrLogType: String): DataFrame = {
        // 关联出字段, userDF: vpdncompanycode, custprovince
        var resultDF: DataFrame = null
        if (cdrLogType == CDRConverterUtils.LOG_TYPE_PDSN) {
         resultDF = cdrDF.join(userDF, userDF.col("mdn") === cdrDF.col("mdn"), "left").
          select(cdrDF.col("mdn"), cdrDF.col("account_session_id"), cdrDF.col("acct_status_type"),
            cdrDF.col("originating").alias("upflow"), cdrDF.col("termination").alias("downflow"), cdrDF.col("event_time"), cdrDF.col("active_time"),
            cdrDF.col("acct_input_packets"), cdrDF.col("acct_output_packets"),cdrDF.col("acct_session_time"),
            userDF.col("vpdncompanycode"), userDF.col("custprovince"),
            cdrDF.col("cellid"),cdrDF.col("bsid")).
           withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))

        } else if (cdrLogType == CDRConverterUtils.LOG_TYPE_PGW) {

           resultDF = cdrDF.join(userDF, userDF.col("mdn") === cdrDF.col("mdn"), "left").
            select(cdrDF.col("mdn"), cdrDF.col("recordtype"), cdrDF.col("starttime"),
              cdrDF.col("stoptime"), cdrDF.col("l_timeoffirstusage"), cdrDF.col("l_timeoflastusage"),
              cdrDF.col("l_datavolumefbcuplink").alias("upflow"), cdrDF.col("l_datavolumefbcdownlink").alias("downflow"),
              userDF.col("vpdncompanycode"), userDF.col("custprovince")).
             withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))


        } else if (cdrLogType == CDRConverterUtils.LOG_TYPE_HACCG) {
/*
          resultDF = cdrDF.join(userDF, userDF.col("mdn") === cdrDF.col("mdn"), "left").
            select(cdrDF.col("mdn"), cdrDF.col("account_session_id"), cdrDF.col("acct_status_type"),
              cdrDF.col("originating"), cdrDF.col("termination"), cdrDF.col("event_time"), cdrDF.col("active_time"),
              cdrDF.col("acct_input_packets"), cdrDF.col("acct_output_packets"),cdrDF.col("acct_session_time"),
              cdrDF.col("cellid"),cdrDF.col("bsid"),cdrDF.col("d"),cdrDF.col("h"))*/

        }
        resultDF
      }


      val resultDF = getAuthLogDF(logType)


      // 计算cloalesce的数量
      val coalesceNum = makeCoalesce(fileSystem, inputLocation, coalesceSize)
      logInfo(s"$inputPath , $coalesceSize, $coalesceNum")

      // 结果数据分区字段
      val partitions = "d,h"
      // 将数据存入到HDFS， 并刷新分区表
     CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, resultDF, coalesceNum, partitions, hourid.substring(0,10), outputPath , cdrSecondaryETLTable, appName)


    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        logError("[" + appName + "] 失败 处理异常" + e.getMessage)
    }
    finally {
      sc.stop()
    }
  }

}
