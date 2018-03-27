package com.zyuc.stat.iot.etl.secondary

import com.zyuc.stat.iot.etl.util.CommonETLUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.makeCoalesce
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2017/8/29.
  */
object MMESecondETLDay {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name") // name_20170731
    val inputPath = sc.getConf.get("spark.app.inputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/ANALY_PLATFORM/MME/secondETLData/data
    val outputPath = sc.getConf.get("spark.app.outputPath") //hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/mme/ETL/day/data
    val authLogDayDayTable = sc.getConf.get("spark.app.table.stored") // "iot_mme_log_d"
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt //128
    val timeid = sc.getConf.get("spark.app.timeid")//yyyymmddhhmiss

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    // sqlContext.sql("set spark.sql.shuffle.partitions=500")

    val dayid = timeid.substring(0,8) //"20170731"
    val partitionD = timeid.substring(2,8)
    val inputLocation = inputPath + "/d=" + partitionD
    try {

      sqlContext.setConf("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      val authDF = sqlContext.read.format("orc").load(inputLocation)

        // 关联出字段, userDF: vpdncompanycode, custprovince
      val resultDF = authDF.
          select(authDF.col("mdn"), authDF.col("province"), authDF.col("imei"), authDF.col("procedureid"),
            authDF.col("starttime"), authDF.col("acctype"), authDF.col("imsi"),
            authDF.col("sergw"), authDF.col("pcause"), authDF.col("ci"),
            authDF.col("newmmecode"), authDF.col("newmtmsi"), authDF.col("mmetype"),
            authDF.col("result"), authDF.col("tac"), authDF.col("modelname"),
            authDF.col("devicetype"), authDF.col("vpdncompanycode"), authDF.col("custprovince"),
            authDF.col("enbid"), authDF.col("uemme"), authDF.col("newgrpid")).withColumn("d", lit(partitionD))


      // 计算cloalesce的数量
      val coalesceNum = makeCoalesce(fileSystem, inputLocation, coalesceSize)


      // 结果数据分区字段
      val partitions = "d"
      // 将数据存入到HDFS， 并刷新分区表
      CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, resultDF, coalesceNum, partitions,dayid, outputPath, authLogDayDayTable, appName)
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    finally {
      sc.stop()
    }
  }

}
