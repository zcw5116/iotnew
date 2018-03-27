package com.zyuc.stat.iot.etl

import com.zyuc.stat.iot.etl.util.{BaseStationConverterUtils, TerminalConvertUtils}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{CharacterEncodeConversion, FileUtils}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * 4G 基站信息的ETL
  *
  * @author zhoucw
  * @version 1.0
  */
object BaseStation4gETL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/hadoop/IOT/data/IotBSInfo/srcdata/all_iotbsinfo.txt")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/hadoop/IOT/data/basic/IotBSInfo/")

    val loadTime = appName.substring(appName.lastIndexOf("_") + 1)
    val srcRDD = CharacterEncodeConversion.transfer(sc, inputPath, "GBK")

    //terminalDF = sqlContext.createDataFrame(terminalDF.map(x =>TerminalConvertUtils.parseLine(x.getString(0))), TerminalConvertUtils.struct)
   //  过滤第一行表头
    val head = srcRDD.first
    val resultDF = sqlContext.createDataFrame(srcRDD.filter(x=>x!=head).map(x=>BaseStationConverterUtils.parseLine(x)).filter(_.length !=1),  BaseStationConverterUtils.struct)

    val tempPath = outputPath + "temp"

    resultDF.coalesce(1).write.mode(SaveMode.Overwrite).format("orc").save(tempPath)

    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)
    FileUtils.moveTempFilesToData(fileSystem, outputPath, loadTime)

    }
}
