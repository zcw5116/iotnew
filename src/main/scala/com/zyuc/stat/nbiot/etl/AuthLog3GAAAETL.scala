package com.zyuc.stat.nbiot.etl

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zyuc.stat.utils.FileUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by liuzk on  2019/7/2 15:38
  */
object AuthLog3GAAAETL extends Logging{

//  字段： time, user, callid,    from, nas-port, nas-port-id,    nas-port-type, pcfip, result
//  含义： 认证时间；          username的值,   固网时为MAC地址,C网时为IMSI,
//        NAS客户端IP地址,    nas-port,      固网业务时有用C网业务时为空,
//        nas-port-type,    接入的基站标识,  认证结果
  val struct3GAAA = StructType(Array(
    StructField("user", StringType),
    StructField("callid", StringType),
    StructField("pdsnip", StringType),
    StructField("nas_port", StringType),
    StructField("nas_port_id", StringType),
    StructField("nas_port_type", StringType),
    StructField("pcfip", StringType),
    StructField("result", StringType),

    StructField("acctime", StringType),
    StructField("d", StringType),
    StructField("h", StringType),
    StructField("m5", StringType)
  ))

  /**
    * from unix time to yyyyMMddHHmmss
    */
  def getTime(time: String): Tuple4[String, String, String, String] = {
    try {
      val targetfdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
      val timeStr = targetfdf.format(time.toLong * 1000)
      val d = timeStr.substring(0, 10).replaceAll("-", "")
      val h = timeStr.substring(11, 13)
      val m5 = timeStr.substring(14, 15) + (timeStr.substring(15, 16).toInt / 5) * 5
      (timeStr, d, h, m5)
    } catch {
      case e: Exception => {
        ("0", "0", "0", "0")
      }
    }
  }

  def parse3GAAA(line: String): Row = {
    try {
      val arr = line.split(",", 9)
      val timeTuple = getTime(arr(0))
      Row(arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), arr(8), timeTuple._1, timeTuple._2, timeTuple._3, timeTuple._4)
    } catch {
      case e: Exception => {
        Row("-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1")
      }
    }
  }

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
        val rdd = sc.textFile(location).map(x=>parse3GAAA(x))
        dataDF = sqlContext.createDataFrame(rdd,struct3GAAA)
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
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("liuzk_3gaaa")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val paramString: String =
      """
        |{
        | "appName"      : "liuzk_3gaaa",
        | "loadTime"     : "201907021500",
        | "inputPath"    : "hdfs://spark1234:8020/user/iot/data/cdr/src/authlog/3gaaa",
        | "outputPath"   : "hdfs://spark1234:8020/user/iot/data/cdr/transform/authlog/3gaaa",
        | "fileWildcard" : "*"
        |}
      """.stripMargin
    val params = JSON.parseObject(paramString)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val rst = doJob(sqlContext, fileSystem, params)
  }
}
