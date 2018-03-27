package com.zyuc.stat.iot.etl

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 3G 基站信息的ETL
  *
  * @author zhoucw
  * @version 1.0
  */
object BaseStation3gETL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/hadoop/IOT/data/IotBSInfo/srcdata/bs3g.txt")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/hadoop/IOT/data/basic/IotBS3gInfo/")
    val loadTime = appName.substring(appName.lastIndexOf("_") + 1)

    val struct = StructType(Array(
      StructField("provcode", StringType),
      StructField("provname", StringType),
      StructField("bsidpre", StringType),
      StructField("citycode", StringType),
      StructField("cityname", StringType)
    ))

    val textRDD = sqlContext.read.format("text").load(inputPath).map(x=>x.getString(0))
    val head = textRDD.first()
    val textRowRDD = textRDD.filter(x=>x!=head).map(x=>x.split("\t", 5)).map(x=>Row(x(0), x(1), Integer.toHexString(Integer.valueOf(x(2))).toUpperCase, x(3), x(4)))

    val resultDF = sqlContext.createDataFrame(textRowRDD, struct)

    val tempPath = outputPath + "temp"
    resultDF.coalesce(1).write.mode(SaveMode.Overwrite).format("orc").save(tempPath)

    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)
    FileUtils.moveTempFilesToData(fileSystem, outputPath, loadTime)

    }
}
