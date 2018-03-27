package com.zyuc.stat.iot.etl.secondary

import com.zyuc.stat.iot.etl.util.CommonETLUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

/**
  * Created by zhoucw on 17-8-16.
  */
object RadiusSecondETL {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)


    val appName = sc.getConf.get("spark.app.name") // name_{type}_h_2017073111
    //val radiusTable = sc.getConf.get("spark.app.table.radiusTable") // pgwradius_out
    val radiusInputPath_pgw = sc.getConf.get("spark.app.inputPath.pgw")  // hdfs://EPC-IOT-ES-06:8020/user/hive/warehouse/iot.db/pgwradius_out/
    val radiusInputPath_ha = sc.getConf.get("spark.app.inputPath.ha")  // hdfs://EPC-IOT-ES-06:8020/user/hive/warehouse/iot.db/pgwradius_out/
    val outputPath = sc.getConf.get("spark.app.outputPath")  //  hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/radius/output/
    val radiustime = sc.getConf.get("spark.app.timeid")//yyyymmddhhmiss
    val userTable = sc.getConf.get("spark.app.user.table") //"iot_customer_userinfo"
    val userTablePatitionDayid = sc.getConf.get("spark.app.user.userTablePatitionDayid")
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt //128
    val sourceTablename = sc.getConf.get("spark.app.table.source")//pgwradius_out/
    val storedTablename = sc.getConf.get("spark.app.table.stored")//iot_radius_data_d

    val userDF = sqlContext.table(userTable).filter("d=" + userTablePatitionDayid).
      selectExpr("mdn", "belo_prov as custprovince", "companycode").
      cache()

    val dayid = radiustime.substring(0,8)
    val srcLocation_pgw = radiusInputPath_pgw + "/dayid="+dayid
    val srcLocation_ha = radiusInputPath_ha + "/dayid="+dayid
    val radiusSrcDF_pgw = sqlContext.read.format("orc").load(srcLocation_pgw).select("mdn","nettype","time","status","terminatecause")
    val radiusSrcDF_ha = sqlContext.read.format("orc").load(srcLocation_ha).select("mdn","nettype","time","status","terminatecause")
    val radiusSrcDF = radiusSrcDF_pgw.unionAll(radiusSrcDF_ha)
    val radiusDF = radiusSrcDF.join(userDF, radiusSrcDF.col("mdn")===userDF.col("mdn"), "left").
      select(radiusSrcDF.col("mdn"), radiusSrcDF.col("nettype"), radiusSrcDF.col("time"), radiusSrcDF.col("status"),
        radiusSrcDF.col("terminatecause"),userDF.col("custprovince"), userDF.col("vpdncompanycode")).
      withColumn("d", lit(dayid.substring(2,8)))


    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)
    val coalesceNum_pgw = FileUtils.makeCoalesce(fileSystem, srcLocation_pgw, coalesceSize.toInt)
    val coalesceNum_ha = FileUtils.makeCoalesce(fileSystem, srcLocation_ha, coalesceSize.toInt)

    val partitions="d"
    val executeResult = CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, radiusDF, coalesceNum_pgw+coalesceNum_ha, partitions, dayid, outputPath, storedTablename, appName)

  }

}
