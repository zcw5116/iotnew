package com.zyuc.stat.nbiot.analysis.day.neimengView

import java.io.InputStream
import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 19-7-11.
  *
  * 内蒙视图 14629
  * online flux  attach/tau
  */
object GwMmeMonthAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_201907")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath_pgw = sc.getConf.get("spark.app.inputPath", "/user/epciot/data/pgwNM/data")
    val inputPath_sgw = sc.getConf.get("spark.app.inputPath", "/user/epciot/data/sgwNM/data")
    val inputPath_mme = sc.getConf.get("spark.app.inputPath", "/user/epciot/data/mmeNM/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/epciot/data/neimeng/summ_m/")

    val bsinfoPath =  sc.getConf.get("spark.app.table.bsinfoTab", "/user/epciot/data/basic/bs4g/data/")//epc_basic_bs4g
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val month = dataTime.substring(2, 6)
    val partitionPath = s"/d=$month*/h=*/m5=*"

    var df: DataFrame = null
    var result: Array[Row] = null

    //基站与对应地区
    val bsinfoTab = "bsinfoTab"
    sqlContext.read.format("orc").load(bsinfoPath).registerTempTable(bsinfoTab)
    val table_cacheBS = "table_cacheBS"
    sqlContext.sql(
      s"""
         |cache table ${table_cacheBS}
         |as
         |select *
         |from ${bsinfoTab}
       """.stripMargin)

    try{
      // online---sgw
      val table_sgw = "table_sgw"
      sqlContext.read.format("orc").load(inputPath_sgw + partitionPath).registerTempTable(table_sgw)
      df = sqlContext.sql(
              s"""
                 |select nvl(b.province_id,'未知') province_id, nvl(b.province_name,'未知') province_name,
                 |       nvl(b.city_id,'未知') city_id, nvl(b.city_name,'未知') city_name,
                 |       'online' itemcode, '-1' reasoncode, count(distinct mdn) itemvalue,
                 |       '$dataTime' stattime, '内蒙古' provflag, 'month' timeflag
                 |from ${table_sgw} a
                 |left join ${table_cacheBS} b on(a.bsid = b.enb_id)
                 |group by b.province_id, b.province_name, b.city_id, b.city_name
             """.stripMargin)
      df.write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "tmp")
      df = sqlContext.read.format("orc").load(outputPath + "tmp")
      result = df.filter("length(province_name)>0 and length(city_name)>0").collect()
      GwMmeHourAnalysis.insertNeiMengByJDBC(result)

    }catch {
      case e: Exception => {
        e.printStackTrace()
        println("=========sgw当前无数据=========")
      }
    }

    try{
      // flux---pgw
      val table_pgw = "table_pgw"
      sqlContext.read.format("orc").load(inputPath_pgw + partitionPath).registerTempTable(table_pgw)
      df = sqlContext.sql(
              s"""
                 |select nvl(b.province_id,'未知') province_id, nvl(b.province_name,'未知') province_name,
                 |       nvl(b.city_id,'未知') city_id, nvl(b.city_name,'未知') city_name,
                 |       'flux' itemcode, '-1' reasoncode,
                 |       round(sum(l_datavolumeFBCUplink + l_datavolumeFBCDownlink)/1024/1024/1024,2) itemvalue,
                 |       '$dataTime' stattime, '内蒙古' provflag, 'month' timeflag
                 |from ${table_pgw} a
                 |inner join ${table_cacheBS} b on(a.enodebid = b.enb_id)
                 |group by b.province_id, b.province_name, b.city_id, b.city_name
             """.stripMargin)
      df.write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "tmp")// left join ==> inner join
      df = sqlContext.read.format("orc").load(outputPath + "tmp")
      result = df.filter("length(province_name)>0 and length(city_name)>0").collect()
      GwMmeHourAnalysis.insertNeiMengByJDBC(result)

    }catch {
      case e: Exception => {
        e.printStackTrace()
        println("=========pgw当前无数据=========")
      }
    }

    try{
      // attach tau---mme
      val table_mme = "table_mme"
      sqlContext.read.format("orc").load(inputPath_mme + partitionPath).registerTempTable(table_mme)
      df = sqlContext.sql(
              s"""
                 |select nvl(b.province_id,'未知') province_id, nvl(b.province_name,'未知') province_name,
                 |       nvl(b.city_id,'未知') city_id, nvl(b.city_name,'未知') city_name,
                 |       'attach' itemcode, pcause reasoncode, count(*) itemvalue,
                 |       '$dataTime' stattime, '内蒙古' provflag, 'month' timeflag
                 |from ${table_mme} a
                 |left join ${table_cacheBS} b on(a.enbid = b.enb_id)
                 |where a.processResult = 'fail' and a.processflag='attach'
                 |group by b.province_id, b.province_name, b.city_id, b.city_name, a.pcause
             """.stripMargin)
      df.write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "tmp")
      df = sqlContext.read.format("orc").load(outputPath + "tmp")
      result = df.filter("length(province_name)>0 and length(city_name)>0").collect()
      GwMmeHourAnalysis.insertNeiMengByJDBC(result)

      df = sqlContext.sql(
              s"""
                 |select nvl(b.province_id,'未知') province_id, nvl(b.province_name,'未知') province_name,
                 |       nvl(b.city_id,'未知') city_id, nvl(b.city_name,'未知') city_name,
                 |       'tau' itemcode, pcause reasoncode, count(*) itemvalue,
                 |       '$dataTime' stattime, '内蒙古' provflag, 'month' timeflag
                 |from ${table_mme} a
                 |left join ${table_cacheBS} b on(a.enbid = b.enb_id)
                 |where a.processResult = 'fail' and a.processflag='tau'
                 |group by b.province_id, b.province_name, b.city_id, b.city_name, a.pcause
             """.stripMargin)
      df.write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "tmp")
      df = sqlContext.read.format("orc").load(outputPath + "tmp")
      result = df.filter("length(province_name)>0 and length(city_name)>0").collect()
      GwMmeHourAnalysis.insertNeiMengByJDBC(result)

    }catch {
      case e: Exception => {
        e.printStackTrace()
        println("=========mme当前无数据=========")
      }
    }

  }
}
