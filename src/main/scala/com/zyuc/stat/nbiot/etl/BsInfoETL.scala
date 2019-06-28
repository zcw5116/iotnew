package com.zyuc.stat.nbiot.etl

import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-6-13.
  */
object BsInfoETL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20180504")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/basic/IotBSInfo/src/")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/basic/IotBSInfo/data/")
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)


    import sqlContext.implicits._
    val bsPath = inputPath + s"bts_info_$dataTime.csv"
    val path = new Path(bsPath)

    /*val rddGBK2 = sc.hadoopFile(filePath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1).
      map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
      .filter(_.length>3)
      .map(line=>line.replace("\"", "").split(",",9)).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
      .toDF("enbid", "provId", "provName", "cityId", "cityName", "zhLabel", "userLabel", "vendorId", "vndorName")
      .registerTempTable(terminalTable)*/
    if (FileUtils.getFilesByWildcard(fileSystem, path.toString).length > 0) {
      val df = sc.textFile(bsPath)
        .filter(_.length>3)
        .map(line=>line.replace("\"", "").split(",",9)).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
        .toDF("enbid", "provId", "provName", "cityId", "cityName", "zhLabel", "userLabel", "vendorId", "vndorName")
        .write.format("orc").mode(SaveMode.Overwrite)save(outputPath)
    }



  }
}
