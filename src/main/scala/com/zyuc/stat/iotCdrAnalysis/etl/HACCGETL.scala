package com.zyuc.stat.iotCdrAnalysis.etl

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zyuc.stat.iot.etl.util.CDRConverterUtils
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat

import scala.collection.mutable

/**
  * Created by liuzk on 18-5-28.
  */
object HACCGETL extends Logging{
  def doJob(parentContext: SQLContext, fileSystem: FileSystem, params: JSONObject): String = {
    val sqlContext = parentContext.newSession()
    val sc = sqlContext.sparkContext
    //sqlContext.sql("use +" + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = params.getString("appName")
    val loadTime = params.getString("loadTime")
    val inputPath = params.getString("inputPath")
    val outputPath = params.getString("outputPath")
    val fileWildcard = params.getString("fileWildcard")

    var dataDF: DataFrame = null
    val partitionArr = Array("d", "h", "m5")

    def getTemplet: String = {
      var templet = ""
      partitionArr.foreach(par => {
        templet = templet + "/" + par + "=*"
      })
      templet
    }

    val dirTodo = inputPath + "/" + loadTime
    val dirDoing = inputPath + "/" + loadTime + "_doing"
    val dirDone = inputPath + "/" + loadTime + "_done"
    var isRenamed: Boolean = false
    var result: String = null

    // 目录改名为doing后缀
    isRenamed = FileUtils.renameHDFSDir(fileSystem, dirTodo, dirDoing)
    result = if (isRenamed) "Success" else "Failed"
    logInfo(s"$result to rename $dirTodo to $dirDoing")
    if (!isRenamed) {
      return s"appName: $appName: $result to rename $dirTodo to $dirDoing"
    }

    try {
      val location = dirDoing + "/" + fileWildcard

      if (FileUtils.getFilesByWildcard(fileSystem, location).length > 0) {
        val jsonRDD = sc.hadoopFile(location,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],1)
          .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
        val fileDF = sqlContext.read.json(jsonRDD)
        dataDF = CDRConverterUtils.parse(fileDF, CDRConverterUtils.LOG_TYPE_HACCG)
      }
      else {
        logInfo(s"No file found during time: $loadTime")
        // 目录改名为done后缀
        FileUtils.renameHDFSDir(fileSystem, dirDoing, dirDone)
        return s"appName: $appName: No file found from path $inputPath"
      }

      val tempDestDir = outputPath + "/temp/" + loadTime

      logInfo(s"Write data into $tempDestDir")

      dataDF.coalesce(1).write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitionArr: _*)
        .save(tempDestDir)

      val fileParSet = new mutable.HashSet[String]()

      FileUtils.getFilesByWildcard(fileSystem, tempDestDir + getTemplet + "/*.orc")
        .foreach(outFile => {
          val tmpPath = outFile.getPath.toString
          val filePar = tmpPath.substring(0, tmpPath.lastIndexOf("/"))
            .replace(tempDestDir, "").substring(1)

          fileParSet.+=(filePar)
        })

      logInfo(s"Move temp files")
      FileUtils.moveTempFiles(fileSystem, outputPath + "/", loadTime, getTemplet, fileParSet)

      // 维护HIVE表,判断是否要增加分区
      //            val hiveTab = "xxxx"
      //            chkHiveTabPartition(sqlContext, hiveTab, fileParSet)

    } catch {
      case e: Exception =>
        e.printStackTrace()
        val cleanPath = outputPath + "/temp/" + loadTime
        FileUtils.cleanFilesByPath(fileSystem, cleanPath)
        logError(s"[$appName] 失败  处理异常" + e.getMessage)
        s"appName: $appName: ETL Failed. "
    }

    // 目录改名为done后缀
    isRenamed = FileUtils.renameHDFSDir(fileSystem, dirDoing, dirDone)
    result = if (isRenamed) "Success" else "Failed"
    logInfo(s"$result to rename $dirDoing to $dirDone")

    s"appName: $appName: ETL Success. "
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("mh_testCDR")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val paramString: String =
      """
        |{
        | "appName"      : "mh_testCDR",
        | "loadTime"     : "201805281526",
        | "inputPath"    : "hdfs://nameservice1/user/iot/data/cdr/src/haccg",
        | "outputPath"   : "hdfs://nameservice1/user/iot/data/cdr/transform/haccg",
        | "fileWildcard" : "*"
        |}
      """.stripMargin
    val params = JSON.parseObject(paramString)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val rst = doJob(sqlContext, fileSystem, params)
  }
}

