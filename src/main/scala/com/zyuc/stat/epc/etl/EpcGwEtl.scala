package com.zyuc.stat.epc.etl

import com.zyuc.stat.epc.etl.utils.EpcGwEtlTransfrom
import com.zyuc.stat.iot.etl.CDRETL.logInfo
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import com.zyuc.stat.utils.FileUtils.renameHDFSDir
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by zhoucw on 下午1:54.
  */
object EpcGwEtl extends Logging {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("test_201802071340").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)
    // val loadTime = sc.getConf.get("spark.app.loadtime", "201802071340")
    val appName = sc.getConf.get("spark.app.name", "test_")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/tmp/jiangxi/pgw/data")
    val outputPath = sc.getConf.get("spark.app.outputPath", "hdfs://spark123:8020/tmp/jiangxi/pgw/data/")
    val mdnSectionPath = sc.getConf.get("spark.app.mdnSectionPath", "/tmp/jiangxi/mdnsection/output/data/*")
    val gwTableName = sc.getConf.get("spark.app.sgwTableName", "gwTableName")
    val gwType = sc.getConf.get("spark.app.gwType", "EPC_PGW") // EPC_SGW  EPC_PGW

    val loadTime = appName.substring(appName.lastIndexOf("_") + 1)

    doEpcGwEtl(hiveContext, loadTime, inputPath, outputPath, mdnSectionPath, gwTableName, gwType)

  }

  def doEpcGwEtl(parentContext: SQLContext, loadTime: String, inputPath: String, outputPath: String, mdnSectionPath: String, gwTableName: String, gwType: String): String = {

    try {
      val sc = parentContext.sparkContext
      val sqlContext = parentContext.newSession()
      sqlContext.sql("use " + ConfigProperties.EPC_HIVE_DATABASE)
      val fileSystem = FileSystem.get(sc.hadoopConfiguration)

      val srcLocation = inputPath + "/" + loadTime
      val srcDoingLocation = inputPath + "/" + loadTime + "_doing"
      val isRename = renameHDFSDir(fileSystem, srcLocation, srcDoingLocation)
      var result = "Success"
      if (!isRename) {
        result = "Failed"
        logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)
        return "loadTime:" + loadTime + ": " + s"$srcLocation rename to $srcDoingLocation :" + result + ". "
      }
      logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)


      val gwSrcTable = "sgwSrcTable_" + loadTime
      val mdnSectionTable = "mdnSectionTable_" + loadTime

      // 计算收敛大小
      val coalesceNum = FileUtils.computePartitionNum(fileSystem, srcDoingLocation, 128)
      // val coalesceNum = FileUtils.makeCoalesce(fileSystem, inputPath + "/" + loadTime, 128)

      import sqlContext.implicits._
      val inputRDD = sc.hadoopFile(srcDoingLocation + "/*", classOf[TextInputFormat], classOf[LongWritable], classOf[Text], coalesceNum).
        map(p => new String(p._2.getBytes, 0, p._2.getLength, "UTF8"))

      val inputDF = sqlContext.read.json(inputRDD)
      inputDF.show(false)


      val mdnSectionDim = sqlContext.read.format("orc").load(mdnSectionPath).map(x => (x.getString(0), x.getString(1),
        x.getString(2), x.getString(3), x.getString(4))).collect()
      val mdnSectionBD = sc.broadcast[Array[(String, String, String, String, String)]](mdnSectionDim)

      val mdnSectionDF = sc.parallelize(mdnSectionBD.value).map(x => (x._1, x._2, x._3, x._4, x._5)).
        toDF("sectionno", "atrb_pr", "user_pr", "user_city", "network")

      mdnSectionDF.registerTempTable(mdnSectionTable)
      //mdnSectionDF.show()

      val newSgwDF = EpcGwEtlTransfrom.parse(inputDF, gwType)
      newSgwDF.registerTempTable(gwSrcTable)

      val resultDF = sqlContext.sql(
        s"""
           |select g.*,
           |       (case when s.network='IOT' then '物联网' else s.atrb_pr end ) as mdnprovince,
           |       s.user_pr, s.user_city, s.network
           |       from ${gwSrcTable} g left join ${mdnSectionTable} s
           |on(sectionno = mdnsection)
       """.stripMargin)

      //resultDF.show()
      //mdnSectionDim.collect().foreach(println)


      val partitions = "d,h,m5"

      def getTemplate: String = {
        var template = ""
        val partitionArray = partitions.split(",")
        for (i <- 0 until partitionArray.length)
          template = template + "/" + partitionArray(i) + "=*"
        template // rename original dir
      }

      //.coalesce(coalesceNum)
      resultDF.repartition(coalesceNum).write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitions.split(","): _*).save(outputPath + "temp/" + loadTime)
      logInfo(s"write data to temp/${loadTime}")

      val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + loadTime + getTemplate + "/*.orc"))
      val filePartitions = new mutable.HashSet[String]
      for (i <- 0 until outFiles.length) {
        val nowPath = outFiles(i).getPath.toString
        filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + loadTime, "").substring(1))
      }

      FileUtils.moveTempFiles(fileSystem, outputPath, loadTime, getTemplate, filePartitions)
      logInfo(s"moveTempFiles ")

      val srcDoneLocation = inputPath + "/" + loadTime + "_done"
      val isDoneRename = renameHDFSDir(fileSystem, srcDoingLocation, srcDoneLocation)
      var doneResult = "Success"
      if (!isDoneRename) {
        doneResult = "Failed"
        logInfo(s"$srcDoingLocation rename to $srcDoneLocation :" + doneResult)
        return "loadTime:" + loadTime + ": " + s"$srcDoingLocation rename to $srcDoneLocation :" + doneResult + ". "
      }


      filePartitions.foreach(partition => {
        var d = ""
        var h = ""
        var m5 = ""
        partition.split("/").map(x => {
          if (x.startsWith("d=")) {
            d = x.substring(2)
          }
          if (x.startsWith("h=")) {
            h = x.substring(2)
          }
          if (x.startsWith("m5=")) {
            m5 = x.substring(3)
          }
          null
        })
        var sql = ""
        if (d.nonEmpty && h.nonEmpty && m5.nonEmpty) {
          sql = s"alter table $gwTableName add IF NOT EXISTS partition(d='$d', h='$h', m5='$m5')"
        }
        else if (d.nonEmpty && h.nonEmpty) {
          sql = s"alter table $gwTableName add IF NOT EXISTS partition(d='$d', h='$h')"
        } else if (d.nonEmpty) {
          sql = s"alter table $gwTableName add IF NOT EXISTS partition(d='$d')"
        }
        logInfo(s"partition $sql")
        if (sql.nonEmpty) {
          try {
            sqlContext.sql(sql)
          } catch {
            case e: Exception => {
              logError(s"partition $sql excute failed " + e.getMessage)
            }
          }
        }
      })

      return "[" + loadTime + "]  ETL  success."
    } catch {
      case e: Exception => {
        e.printStackTrace()
        logError("[" + loadTime + "] 失败 处理异常" + e.getMessage)
        return "[" + loadTime + "] 失败 处理异常"
      }
    }


  }

}
