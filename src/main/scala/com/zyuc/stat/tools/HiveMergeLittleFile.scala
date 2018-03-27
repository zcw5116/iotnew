package com.zyuc.stat.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path, FileSystem}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wangpf on 2017/6/28.
 * desc:合并hive数据库的小文件，目前只支持日分区
 */
object HiveMergeLittleFile extends App {
  var table: String = null
  var day: String = null
  var partition: Int = 0
  if (args.length >= 3){
    table = args(0)
    day = args(1)
    partition = args(2).toInt
  }
  else
    System.exit(0)

  // Hadoop集群下
  val hdfsconf = new Configuration()
  val fs = FileSystem.get(hdfsconf)

  val savePath = s"/user/slview/mergehive/${table}/dayid=${day}"
  val tablePath = s"/user/hive/warehouse/iot.db/${table}"
  val dataPath = tablePath + s"/dayid=${day}"

  println("合并文件保存目录：" + savePath)
  println("原始数据文件保存目录：" + dataPath)
  // 检查保存目录
  if (fs.exists(new Path(savePath))) {
    println("删除目录：" + savePath)
    fs.delete(new Path(savePath), true)
  }

  val conf = new SparkConf()
    .setAppName("HiveMergeLittleFile")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)

  sqlContext.sql("set hive.merge.size.per.task=134217728")

  sqlContext.sql(s"select * from iot.${table} where dayid = '${day}'")
    .coalesce(partition)
    .write
    .orc(savePath)

  sc.stop()

  val sourceStatus = fs.globStatus(new Path(tablePath + "/*"), new RegexPathFilter(".*\\.hive-staging_hive.*", RegexPathFilter.MOOD_SELECT))

  val listedPaths = FileUtil.stat2Paths(sourceStatus)

  // 遍历删除垃圾文件
  for (i <- 0 until listedPaths.length ) {
    println(listedPaths(i).getName)
    fs.delete(listedPaths(i), true)
  }

  // 删除hive数据目录
  if (fs.exists(new Path(dataPath))) {
    println("删除目录：" + dataPath)
    fs.delete(new Path(dataPath), true)
  }

  // 将合并后的文件上传到hive目录下
  fs.rename(new Path(savePath), new Path(dataPath))

  fs.close()
}
