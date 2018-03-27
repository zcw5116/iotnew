package com.zyuc.stat.epc.etl


import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * Created by zhoucw on 下午2:27.
  */
object Bs4gDim {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("test_20180207")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)


    val input = sc.getConf.get("spark.app.input", "/tmp/jiangxi/bs4g/bs100.csv")
    val output = sc.getConf.get("spark.app.output", "/tmp/jiangxi/bs4g/output")
    val appName = sc.getConf.get("spark.app.name")
    val coalesceSize = sc.getConf.get("spark.app.coalesceSizeMB", "128")

    val loadTime = appName.substring(appName.lastIndexOf("_") + 1)

    val coalesceNum = FileUtils.computePartitionNum(fileSystem, input, coalesceSize.toInt)

    import sqlContext.implicits._

    val inputRDD = sc.hadoopFile(input,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],coalesceNum).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    val inputDF = inputRDD.map(x=>x.replaceAll("\"","").split(",",11)).
      filter(x => x(0)!="ENB_ID").
      map(x=>(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10))).
      toDF("enb_id", "province_id", "province_name", "city_id", "city_name", "region_id", "region_name", "zh_label", "userlabel", "vendor_id", "vendor_name")

    val  outputTempDir = output + "/tmp"
    val outputDataDir = output + "/data"
    inputDF.coalesce(coalesceNum).write.format("orc").mode(SaveMode.Overwrite).save(outputTempDir)
    FileUtils.moveFiles(fileSystem, loadTime, outputTempDir, outputDataDir, true)
    inputDF.show
  }

}
