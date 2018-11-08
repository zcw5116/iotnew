package com.zyuc.stat.nbiot.analysis.tmpTotal

import java.io.File

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-11-8.
  */
object HaccgM5Output {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("NbM5Analysis_201805161510")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/cdr/analy_realtime/haccg")
    ///d=180627/h=10/m5=10
    val localPath = sc.getConf.get("spark.app.localPath", "file:///home/slview/nms/bigdata/sparkapp/cdr_3g_backup/")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d= dataTime.substring(2,8)
    val h= dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)
    val childrenPath = "/d=" + d + "/h=" + h + "/m5=" + m5

    val nbPath = inputPath + childrenPath
    sqlContext.read.format("orc").load(nbPath).coalesce(1)
      .rdd.saveAsTextFile(localPath + "tmp" + childrenPath)

  }

}
