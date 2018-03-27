package com.zyuc.stat.epc.etl

import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoucw on 下午2:27.
  */
object MdnSectionDim {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[3]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)


    val input = sc.getConf.get("spark.app.input", "/tmp/jiangxi/mdnsection/mdnsection.csv")
    val output = sc.getConf.get("spark.app.output", "/tmp/jiangxi/mdnsection/output")
    val loadTime = sc.getConf.get("spark.app.loadTime", "2018020612500")
    val coalesceSize = sc.getConf.get("spark.app.coalesceSizeMB", "128")

    val coalesceNum = FileUtils.computePartitionNum(fileSystem, input, coalesceSize.toInt)

    import sqlContext.implicits._
    val inputDF = sqlContext.read.format("text").load(input).
      map(x=>x.getString(0).replaceAll("\"","").split(",",5)).
      filter(x => x(0)!="SECTIONNO").
      map(x=>(x(0), x(1), x(2), x(3), x(4))).
      toDF("sectionno","atrb_pr", "user_pr", "user_city", "network")

    val  outputTempDir = output + "/tmp"
    val outputDataDir = output + "/data"
    inputDF.coalesce(coalesceNum).write.format("orc").mode(SaveMode.Overwrite).save(outputTempDir)
    FileUtils.moveFiles(fileSystem, loadTime, outputTempDir, outputDataDir, true)
    inputDF.show
  }

}
