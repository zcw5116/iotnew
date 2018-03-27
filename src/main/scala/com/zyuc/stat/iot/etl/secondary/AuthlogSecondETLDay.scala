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
  * Created by dell on 2017/8/26.
  */
object AuthlogSecondETLDay extends Logging {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name") // name_20170731
    val inputPath = sc.getConf.get("spark.app.inputPath") // "hadoop/IOT/ANALY_PLATFORM/AuthLog/secondETLData/"
    val outputPath = sc.getConf.get("spark.app.outputPath") //"hadoop/IOT/data/authlog/ETL/day/${SUMMERYTYPE}/"
    var authlogType:String = sc.getConf.get("spark.app.item.type") //"3g,4g,vpdn"
    val authLogDayDayTable = sc.getConf.get("spark.app.table.stored") // "iot_userauth_3gaaa_d"
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt //128
    val timeid = sc.getConf.get("spark.app.timeid")//yyyymmddhhmiss
    if (authlogType != "3g" && authlogType != "4g" && authlogType != "vpdn") {
      logError("[" + appName + "] 日志类型authlogType错误, 期望值： " + "3gaaa" + "," + "4gaaa" + "," + "vpdn")
      return
    }

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    // sqlContext.sql("set spark.sql.shuffle.partitions=500")

    val dayid = timeid.substring(0,8) //"20170731"
    val partitionD = dayid.substring(2,8)
    val inputLocation = inputPath + "/d=" + partitionD
    try {

      sqlContext.setConf("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      val authDF = sqlContext.read.format("orc").load(inputLocation)

      def getAuthLogDF(authlogType: String): DataFrame = {
        // 关联出字段, userDF: vpdncompanycode, custprovince
        var resultDF: DataFrame = null
        if (authlogType == "3g") {

          resultDF = authDF.
            select(authDF.col("auth_result"), authDF.col("authtime"), authDF.col("device"), authDF.col("imsicdma"),
              authDF.col("imsilte"), authDF.col("mdn"), authDF.col("nai_sercode"), authDF.col("nasport"),
              authDF.col("nasportid"), authDF.col("nasporttype"), authDF.col("pcfip"), authDF.col("srcip"),
              authDF.col("result"), authDF.col("vpdncompanycode"), authDF.col("custprovince")).withColumn("d", lit(partitionD))

        } else if (authlogType == "4g") {

          resultDF = authDF.
            select(authDF.col("auth_result"), authDF.col("authtime"), authDF.col("device"), authDF.col("imsicdma"),
              authDF.col("imsilte"), authDF.col("mdn"), authDF.col("nai_sercode"),
              authDF.col("nasportid"), authDF.col("nasporttype"), authDF.col("pcfip"),
              authDF.col("result"), authDF.col("vpdncompanycode"), authDF.col("custprovince")).withColumn("d", lit(partitionD))

        } else if (authlogType == "vpdn") {

          resultDF = authDF.
            select(authDF.col("auth_result"), authDF.col("authtime"), authDF.col("device"), authDF.col("imsicdma"),
              authDF.col("imsilte"), authDF.col("mdn"), authDF.col("nai_sercode"),
              authDF.col("entname"), authDF.col("lnsip"), authDF.col("pdsnip"),
              authDF.col("result"), authDF.col("vpdncompanycode"), authDF.col("custprovince")).withColumn("d", lit(partitionD))

        }
        resultDF
      }


      val resultDF = getAuthLogDF(authlogType)


      // 计算cloalesce的数量
      val coalesceNum = makeCoalesce(fileSystem, inputLocation, coalesceSize)
      logInfo(s"$inputPath , $coalesceSize, $coalesceNum")

      // 结果数据分区字段
      val partitions = "d"
      // 将数据存入到HDFS， 并刷新分区表
      CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, resultDF, coalesceNum, partitions,dayid, outputPath, authLogDayDayTable, appName)
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
