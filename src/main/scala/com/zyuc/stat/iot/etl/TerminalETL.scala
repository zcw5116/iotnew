package com.zyuc.stat.iot.etl

import com.zyuc.stat.iot.etl.util.TerminalConvertUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-7-26.
  */
object TerminalETL extends Logging {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val inputPath = sc.getConf.get("spark.app.inputPath")
    //val inputPath = "/hadoop/IOT/data/terminal/srcdata/"
    val outputPath = sc.getConf.get("spark.app.outputPath")
    //val outputPath = "/hadoop/IOT/data/terminal/output/data"
    val fileWildcard = sc.getConf.get("spark.app.fileWildcard")
    // val fileWildcard = "iot_dim_terminal.txt"
    val fileLocation = inputPath + "/" + fileWildcard
    val crttime = DateUtils.getNowTime("yyyy-MM-dd HH:mm:ss")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val fileExists = if (fileSystem.globStatus(new Path(fileLocation)).length > 0) true else false

    if(!fileExists){
      logInfo("No Files during time: " + crttime)
      System.exit(1)
    }

    var terminalDF = sqlContext.read.format("text").load(fileLocation)

    terminalDF = sqlContext.createDataFrame(terminalDF.map(x =>TerminalConvertUtils.parseLine(x.getString(0))), TerminalConvertUtils.struct)

    terminalDF.coalesce(1).write.mode(SaveMode.Overwrite).format("orc").save(outputPath)


    logInfo("ETL success")
  }
}
