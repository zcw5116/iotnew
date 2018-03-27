package com.zyuc.stat.iot.etl.secondary


import com.zyuc.stat.iot.etl.util.CommonETLUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.makeCoalesce
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by dell on 2017/8/21.
  */
object CDRSecondETLDay extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name") // name_20170731
    val inputPath = sc.getConf.get("spark.app.inputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/cdr/secondaryoutput/pdsn/data  小时文件存放路径
    val outputPath = sc.getConf.get("spark.app.outputPath") //"hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/cdr/ETL/pdsn/"            汇总后文件存放路径
    val cdrSecondaryETLTable = sc.getConf.get("spark.app.table.stored") // "iot_cdr_data_pdsn_d"
    var logType:String = sc.getConf.get("spark.app.item.type") // pdsn pgw haccg
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt //128
    val timeid = sc.getConf.get("spark.app.timeid")//yyyymmddhhmiss



    val dayid = timeid.substring(0,8) //"20170731"
    val partitionD = dayid.substring(2,8)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    // 清洗后存放位置
    val inputLocation = inputPath + "/d=" + partitionD
    try{

      sqlContext.setConf("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      val cdrDF = sqlContext.read.format("orc").load(inputLocation)
      var resultDF: DataFrame = null
      if(logType == "pdsn"){
         resultDF = cdrDF.select(cdrDF.col("mdn"),cdrDF.col("account_session_id"),cdrDF.col("acct_status_type"),cdrDF.col("upflow")
          ,cdrDF.col("downflow"),cdrDF.col("event_time"),cdrDF.col("active_time"),
           cdrDF.col("acct_input_packets"),cdrDF.col("acct_output_packets"),cdrDF.col("acct_session_time")
          ,cdrDF.col("vpdncompanycode"),cdrDF.col("custprovince"),cdrDF.col("cellid"),cdrDF.col("bsid")).withColumn("d", lit(partitionD))

      }else if(logType == "pgw"){
         resultDF = cdrDF.select(cdrDF.col("mdn"),cdrDF.col("recordtype"),cdrDF.col("starttime"),cdrDF.col("stoptime")
          ,cdrDF.col("l_timeoffirstusage"),cdrDF.col("l_timeoflastusage"),cdrDF.col("upflow"),cdrDF.col("downflow"),cdrDF.col("vpdncompanycode")
          ,cdrDF.col("custprovince")).withColumn("d", lit(partitionD))
      }else if(logType == "haccg"){


      }

      // 计算cloalesce的数量
      val coalesceNum = makeCoalesce(fileSystem, inputLocation, coalesceSize)
      logInfo(s"$inputPath , $coalesceSize, $coalesceNum")

      // 结果数据分区字段
      val partitions = "d"
      // 将数据存入到HDFS， 并刷新分区表
      CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, resultDF, coalesceNum, partitions, dayid, outputPath , cdrSecondaryETLTable, appName)

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
