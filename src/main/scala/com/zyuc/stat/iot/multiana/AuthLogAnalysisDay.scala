package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by dell on 2017/8/27.
  */
object AuthLogAnalysisDay {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val authlogTable = sc.getConf.get("spark.app.table.authlog.d") //"iot_userauth_3gaaa_h"
    val authlogType = sc.getConf.get("spark.app.authlogtype") //"3gaaa,4gaaa,pdsn"
    val ceiltime = sc.getConf.get("spark.app.date") //20170826 汇总天
    //val inputPath = sc.getConf.get("spark.app.inputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/ANALY_PLATFORM/AuthLog/secondETLData/3g/data
    val outputPath = sc.getConf.get("spark.app.outputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/authlog/hour/
    val localOutputPath =  sc.getConf.get("spark.app.localOutputPath") // /slview/test/limm/multiAna/authlog/hour/json/
    val partitionD = ceiltime.substring(2)
    val dayid      = ceiltime
    var wheresql = null
    var sourceDF:DataFrame = null
    var mdnitem:String=null
    if (authlogType == "3gaaa"){
      mdnitem = "imsicdma"
    }else{
      mdnitem = "mdn"
    }
    sourceDF = sqlContext.sql(
      s"""select  case when length(custprovince)=0 or custprovince is null then '其他' else custprovince end  as custprovince,
         |       case when length(vpdncompanycode)=0 or vpdncompanycode is null then 'N999999999' else vpdncompanycode end  as vpdncompanycode,
         |       result,auth_result as errorcode,
         |      count(distinct ${mdnitem}) as mdncnt,count(1) as requirecnt
         |from  ${authlogTable}
         |where d = ("${partitionD}")
         |group by case when length(custprovince)=0 or custprovince is null then '其他' else custprovince end,
         |         case when length(vpdncompanycode)=0 or vpdncompanycode is null then 'N999999999' else vpdncompanycode end,
         |         result,auth_result
       """.stripMargin
    )

    val resultDF = sourceDF.select(sourceDF.col("custprovince"),sourceDF.col("vpdncompanycode"),sourceDF.col("errorcode"),sourceDF.col("result"),
      sourceDF.col("requirecnt"),sourceDF.col("mdncnt")
    ).withColumn("datetime", lit(dayid))

    val coalesceNum = 1
    val outputLocatoin = outputPath + "json/data/" + dayid

    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(outputLocatoin)

    FileUtils.downFilesToLocal(fileSystem, outputLocatoin, localOutputPath, dayid, ".json")

    sc.stop()

  }

}
