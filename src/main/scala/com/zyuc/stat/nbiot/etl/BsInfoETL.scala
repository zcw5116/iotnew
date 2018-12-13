package com.zyuc.stat.nbiot.etl

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

    import sqlContext.implicits._
    val terminalTable = "bsTable"
    val filePath = "/user/iot/tmp/bsinfo.csv"

    /*val rddGBK2 = sc.hadoopFile(filePath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1).
      map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
      .filter(_.length>3)
      .map(line=>line.replace("\"", "").split(",",9)).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
      .toDF("enbid", "provId", "provName", "cityId", "cityName", "zhLabel", "userLabel", "vendorId", "vndorName")
      .registerTempTable(terminalTable)*/

    val rddGBK2 = sc.textFile(filePath)
      .filter(_.length>3)
      .map(line=>line.replace("\"", "").split(",",9)).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
      .toDF("enbid", "provId", "provName", "cityId", "cityName", "zhLabel", "userLabel", "vendorId", "vndorName")
      .registerTempTable(terminalTable)

      val sql =
        s"""
           |select * from ${terminalTable}
         """.stripMargin
    sqlContext.sql(sql).write.format("orc").mode(SaveMode.Overwrite)save("/user/iot/data/basic/IotBSInfo/data/")
  }
}
