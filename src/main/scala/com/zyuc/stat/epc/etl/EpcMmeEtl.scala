package com.zyuc.stat.epc.etl

/**
  * Created by zhoucw on 下午8:15.
  */

import com.zyuc.stat.epc.etl.utils.EpcMmeEtlTransfrom
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import com.zyuc.stat.utils.FileUtils.renameHDFSDir
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame,SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by zhoucw on 下午1:54.
  */
object EpcMmeEtl extends Logging{

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("test_201802071340").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)
    // val loadTime = sc.getConf.get("spark.app.loadtime", "201802071340")
    val appName = sc.getConf.get("spark.app.name", "test_")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/tmp/jiangxi/mme/src")
    val outputPath = sc.getConf.get("spark.app.outputPath", "hdfs://spark123:8020/tmp/jiangxi/mme/src/output/")
    val mdnSectionPath = sc.getConf.get("spark.app.mdnSectionPath", "/tmp/jiangxi/mdnsection/output/data/*")
    val mmeTableName = sc.getConf.get("spark.app.mmeTableName", "mmeTableName")
    val ztMMWildcard = sc.getConf.get("spark.app.ztMMWildcard", "_mm_")
    val ztSMWildcard = sc.getConf.get("spark.app.ztSMWildcard", "_sm_")
    val ztPagingWildcard = sc.getConf.get("spark.app.ztPagingWildcard", "_paging_")

    val loadTime = appName.substring(appName.lastIndexOf("_") + 1)

    doEpcMmeEtl(hiveContext, loadTime, inputPath, outputPath,mdnSectionPath, mmeTableName, ztMMWildcard, ztSMWildcard, ztPagingWildcard)

  }

  def doEpcMmeEtl(parentContext: SQLContext, loadTime:String, inputPath:String, outputPath:String, mdnSectionPath:String,
                 mmeTableName : String, ztMMWildcard:String, ztSMWildcard:String, ztPagingWildcard:String): String ={

    try{
      val sqlContext = parentContext.newSession()
      val sc = sqlContext.sparkContext
      sqlContext.sql("use " + ConfigProperties.EPC_HIVE_DATABASE)
      val fileSystem = FileSystem.get(sc.hadoopConfiguration)

      val srcLocation = inputPath + "/" + loadTime
      val srcDoingLocation = inputPath + "/" + loadTime + "_doing"
      val srcDoneLocation = inputPath + "/" + loadTime + "_done"
      val isRename = renameHDFSDir(fileSystem, srcLocation, srcDoingLocation)
      var result = "Success"
      if (!isRename) {
        result = "Failed"
        logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)
        return "loadTime:" + loadTime + ": " + s"$srcLocation rename to $srcDoingLocation :" + result + ". "
      }
      logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)



      val srcTable = "srcTable_" + loadTime
      val mdnSectionTable = "mdnSectionTable_" + loadTime

      // 计算收敛大小
      val coalesceNum = FileUtils.computePartitionNum(fileSystem, srcDoingLocation, 128)
      // val coalesceNum = FileUtils.makeCoalesce(fileSystem, inputPath + "/" + loadTime, 128)

      import sqlContext.implicits._


      val ztMMLocation = srcDoingLocation + "/" + ztMMWildcard
      val ztSMLocation = srcDoingLocation + "/" + ztSMWildcard
      val ztPagingLocation = srcDoingLocation + "/" + ztPagingWildcard

      val ztMMFileExists = if (fileSystem.globStatus(new Path(ztMMLocation)).length > 0) true else false
      val ztSMFileExists = if (fileSystem.globStatus(new Path(ztSMLocation)).length > 0) true else false
      val ztPagingFileExists = if (fileSystem.globStatus(new Path(ztPagingLocation)).length > 0) true else false

      if (!ztMMFileExists && !ztSMFileExists && !ztPagingFileExists) {
        logInfo("No Files during time: " + loadTime)
        // System.exit(1)
        return "loadTime:" + loadTime + ":No Files ."
      }

      var ztMMDF: DataFrame = null
      var ztSMDF: DataFrame = null
      var ztPagingDF: DataFrame = null
      var allInputDF: DataFrame = null

      if (ztMMFileExists) {
        val srcZtMMDF = sqlContext.read.format("json").load(ztMMLocation)
        ztMMDF = EpcMmeEtlTransfrom.parse(srcZtMMDF, EpcMmeEtlTransfrom.ZTMM)
        if (allInputDF == null && ztMMDF!=null) {
          allInputDF = ztMMDF
        } else if(ztMMDF!=null) {
          allInputDF = allInputDF.unionAll(ztMMDF)
        }
      }

      if (ztSMFileExists) {
        val srcZtSMDF = sqlContext.read.format("json").load(ztSMLocation)
        ztSMDF = EpcMmeEtlTransfrom.parse(srcZtSMDF, EpcMmeEtlTransfrom.ZTSM)
        if (allInputDF == null && ztSMDF!=null) {
          allInputDF = ztSMDF
        } else if(ztSMDF!=null) {
          allInputDF = allInputDF.unionAll(ztSMDF)
        }
      }

      if (ztPagingFileExists) {
        val srcZtPagingDF = sqlContext.read.format("json").load(ztPagingLocation)
        ztPagingDF = EpcMmeEtlTransfrom.parse(srcZtPagingDF, EpcMmeEtlTransfrom.ZTPaging)
        if (allInputDF == null && ztPagingDF!=null) {
          allInputDF = ztPagingDF
        } else if(ztPagingDF!=null) {
          allInputDF = allInputDF.unionAll(ztPagingDF)
        }
      }
      allInputDF.registerTempTable(srcTable)


      val mdnSectionDim = sqlContext.read.format("orc").load(mdnSectionPath).map(x=>(x.getString(0), x.getString(1),
        x.getString(2), x.getString(3), x.getString(4))).collect()
      val mdnSectionBD = sc.broadcast[Array[(String, String, String, String, String)]](mdnSectionDim)

      val mdnSectionDF = sc.parallelize(mdnSectionBD.value).map(x=>(x._1, x._2, x._3, x._4, x._5)).
        toDF("sectionno","atrb_pr", "user_pr", "user_city", "network")

      mdnSectionDF.registerTempTable(mdnSectionTable)
      //mdnSectionDF.show()



      val resultDF = sqlContext.sql(
        s"""
           |select s.*,
           |       (case when m.network='IOT' then '物联网' else m.atrb_pr end ) as mdnprovince,
           |       m.user_pr, m.user_city, m.network
           |       from ${srcTable} s left join ${mdnSectionTable} m
           |on(sectionno = mdnsection)
       """.stripMargin)

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

      renameHDFSDir(fileSystem, srcDoingLocation, srcDoneLocation)

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
          sql = s"alter table $mmeTableName add IF NOT EXISTS partition(d='$d', h='$h', m5='$m5')"
        }
        else if (d.nonEmpty && h.nonEmpty) {
          sql = s"alter table $mmeTableName add IF NOT EXISTS partition(d='$d', h='$h')"
        } else if (d.nonEmpty) {
          sql = s"alter table $mmeTableName add IF NOT EXISTS partition(d='$d')"
        }
        logInfo(s"partition $sql")
        if (sql.nonEmpty) {
          try {
            sqlContext.sql(sql)
          }catch {
            case e:Exception => {
              logError(s"partition $sql excute failed " + e.getMessage)
            }
          }
        }
      })
      return "[" + loadTime + "]  ETL  success."
    }catch {
      case e:Exception =>{
        e.printStackTrace()
        logError("[" + loadTime + "] 失败 处理异常" + e.getMessage)
        return "[" + loadTime + "] 失败 处理异常"
      }
    }

  }

}
