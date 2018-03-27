package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2017/8/27.
  * //val inputPath = sc.getConf.get("spark.app.inputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/ANALY_PLATFORM/AuthLog/secondETLData/3g/data
  */
object AuthLogAnalysis15min {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val authlogTable = sc.getConf.get("spark.app.table.authlog.h") //"iot_userauth_3gaaa_h"
    val authlogType = sc.getConf.get("spark.app.authlogtype") //"3gaaa,4gaaa,pdsn"
    val ceiltime = sc.getConf.get("spark.app.ceiltime") //20170826004500 当前小时
    val floortime = sc.getConf.get("spark.app.floortime")//20170825234500 当前小时前15分钟时间
    val outputPath = sc.getConf.get("spark.app.outputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/authlog/15min/
    val localOutputPath =  sc.getConf.get("spark.app.localOutputPath") // /slview/test/limm/multiAna/authlog/15min/json/
    val partitionD = ceiltime.substring(2, 8)
    val partitionH = ceiltime.substring(8,10)
    val dayid      = ceiltime.substring(0, 8)
    val hourid = ceiltime.substring(0, 10)
    val lastpartitionD =  floortime.substring(2, 8)
    val lastpartitionH = floortime.substring(8,10)
    var mdnitem:String=null
    if (authlogType == "3gaaa"){
        mdnitem = "imsicdma"
    }else{
        mdnitem = "mdn"
    }
    var wheresql = null
    var sourceDF:DataFrame = null
    if(partitionH == "00"){
      sourceDF = sqlContext.sql(
        s"""select  case when length(custprovince)=0 or custprovince is null then '其他' else custprovince end  as custprovince,
           |       case when length(vpdncompanycode)=0 or vpdncompanycode is null
           |       then 'N999999999' else vpdncompanycode end  as vpdncompanycode,
           |       result,auth_result as errorcode,
           |       concat("${hourid}",floor(minute(from_unixtime(unix_timestamp(authtime,'yyyyMMddHHmmss') + 900))/15)*15) as datetime,
           |       count(distinct ${mdnitem}) as mdncnt,count(1) as requirecnt
           |from   ${authlogTable}
           |where  d in ("${partitionD}","${lastpartitionD}")
           |and    h in ("${partitionH}","${lastpartitionH}")
           |and    authtime >= "${floortime}"
           |and    authtime < "${ceiltime}"
           |group  by case when length(custprovince)=0 or custprovince is null then '其他' else custprovince end,
           |          case when length(vpdncompanycode)=0 or vpdncompanycode is null
           |          then 'N999999999' else vpdncompanycode end,
           |          result,auth_result,
           |          concat("${hourid}",floor(minute(from_unixtime(unix_timestamp(authtime,'yyyyMMddHHmmss') + 15*60))/15)*15)
       """.stripMargin
      )
    } else if(partitionH != "00"){
      sourceDF = sqlContext.sql(
        s"""select case when length(custprovince)=0 or custprovince is null then '其他' else custprovince end  as custprovince,
           |      case when length(vpdncompanycode)=0 or vpdncompanycode is null
           |      then 'N999999999' else vpdncompanycode end  as vpdncompanycode,
           |      result,auth_result as errorcode,
           |      concat("${hourid}",floor(minute(from_unixtime(unix_timestamp(authtime,'yyyyMMddHHmmss') + 15*60))/15)*15) as datetime,
           |      count(distinct ${mdnitem}) as mdncnt,count(1) as requirecnt
           |from  ${authlogTable}
           |where d = "${partitionD}"
           |and   h in ("${partitionH}","${lastpartitionH}")
           |and   authtime >= "${floortime}"
           |and   authtime < "${ceiltime}"
           |group by case when length(custprovince)=0 or custprovince is null then '其他' else custprovince end,
           |         case when length(vpdncompanycode)=0 or vpdncompanycode is null
           |         then 'N999999999' else vpdncompanycode end,
           |         result,auth_result,
           |         concat("${hourid}",floor(minute(from_unixtime(unix_timestamp(authtime,'yyyyMMddHHmmss') + 900))/15)*15)
       """.stripMargin
      )
    }

    val resultDF = sourceDF.select(sourceDF.col("custprovince"),sourceDF.col("vpdncompanycode").alias("companycode"),
      sourceDF.col("result"),sourceDF.col("errorcode"),
       sourceDF.col("requirecnt"),
       sourceDF.col("mdncnt"),
      sourceDF.col("datetime")
    )

    val coalesceNum = 1
    val outputLocatoin = outputPath + "json/data/" + dayid + "/" + partitionH

    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(outputLocatoin)

    FileUtils.downFilesToLocal(fileSystem, outputLocatoin, localOutputPath + "/"+ dayid + "/", partitionH, ".json")

    sc.stop()

  }

}
