package com.zyuc.stat.nbiot.analysis.realtime

import java.sql.Date
import java.text.SimpleDateFormat

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-7-3.
  */
object NbOnlineM5Alarm {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("NbM5Analysis_201805211430")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPathCity = sc.getConf.get("spark.app.inputPath", "/user/iot/data/online_alarm/transform/nb/data/city")
    val inputPathProv = sc.getConf.get("spark.app.inputPath", "/user/iot/data/online_alarm/transform/nb/data/prov")
    val inputPathNation = sc.getConf.get("spark.app.inputPath", "/user/iot/data/online_alarm/transform/nb/data/nation")
    val outputPathCity = sc.getConf.get("spark.app.outputPath", "/user/iot/data/online_alarm/analy_realtime/nb/city")
    val outputPathProv = sc.getConf.get("spark.app.outputPath", "/user/iot/data/online_alarm/analy_realtime/nb/prov")
    val outputPathNation = sc.getConf.get("spark.app.outputPath", "/user/iot/data/online_alarm/analy_realtime/nb/nation")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d= dataTime.substring(2,8)
    val h= dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    import sqlContext.implicits._
    // 将结果写入到tidb, 需要调整为upsert
    val alarmclass = "NB企业预警"
    val alarmtype = "在线数预警"


    def alarm2tidb(result: Array[(String, String, String,String, String)]) = {
      // 将结果写入到tidb
      var dbConn = DbUtils.getDBConnection
      dbConn.setAutoCommit(false)
      val sql =
        s"""
           |insert into IOT_CUST_RAW_ALARM
           |(EventTimestamp, CUST_ID, ALARM_REGION, ALARM_CLASS, ALARM_TYPE, PROV_NAME, CITY_NAME)
           |values (?,?,?,?,?,?,?)
       """.stripMargin

      val pstmt = dbConn.prepareStatement(sql)

      var i = 0
      for(r<-result){
        val EventTimestamp = (r._1)
        val CUST_ID = r._2
        val ALARM_REGION = r._3
        val PROV_NAME = r._4
        val CITY_NAME = r._5

        pstmt.setString(1, EventTimestamp)
        pstmt.setString(2, CUST_ID)
        pstmt.setString(3, ALARM_REGION)
        pstmt.setString(4, alarmclass)
        pstmt.setString(5, alarmtype)
        pstmt.setString(6, PROV_NAME)
        pstmt.setString(7, CITY_NAME)

        i += 1
        pstmt.addBatch()
        if (i % 1000 == 0) {
          pstmt.executeBatch
          dbConn.commit()
        }
      }
      pstmt.executeBatch
      dbConn.commit()
      pstmt.close()
      dbConn.close()
    }

    //city---------
    val cityPath = new Path(inputPathCity + "/" + dataTime + "00.csv")
    if(fileSystem.exists(cityPath)){
      val cityDF = sc.textFile(inputPathCity + "/" + dataTime + "00.csv").map(x=>x.split(","))
        .filter(x=>x(1).length>1).map(x=>(x(0), x(1), x(2), x(3), x(4)))
        .toDF("eventtimestamp", "custid", "prov", "city", "status")
      val cityTable = "cityTable"
      cityDF.registerTempTable(cityTable)
      /////
      val cityDF1 = sqlContext.sql(
        s"""
           |select eventtimestamp, custid, nvl(prov, '-') as prov, nvl(city, '-') as city, status
           |from ${cityTable}
       """.stripMargin)

      val cityresultDF = cityDF1.withColumn("alarm_region", lit("CITY"))

      val onlineFcstCityDF = cityresultDF.selectExpr("eventtimestamp","custid", "alarm_region", "prov", "city", "status")

      val outputCityResult = outputPathCity + "/d=" + d + "/h=" + h + "/m5=" + m5
      onlineFcstCityDF.write.mode(SaveMode.Overwrite).format("orc").save(outputCityResult)
      /////
      val resultCity = sqlContext.read.format("orc").load(outputCityResult).
        map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getString(4))).collect()

      alarm2tidb(resultCity)
    }



    //prov------------
    val provPath = new Path(inputPathProv + "/" + dataTime + "00.csv")
    if(fileSystem.exists(provPath)){
      val provDF = sc.textFile(inputPathProv + "/" + dataTime + "00.csv").map(x=>x.split(","))
        .filter(x=>x(1).length>1).map(x=>(x(0), x(1), x(2), x(3)))
        .toDF("eventtimestamp", "custid", "prov", "status")
      val provTable = "provTable"
      provDF.registerTempTable(provTable)
      ////
      val provDF1 = sqlContext.sql(
        s"""
           |select eventtimestamp, custid, nvl(prov, '-') as prov, '-' as city, status
           |from ${provTable}
       """.stripMargin)

      val provresultDF = provDF1.withColumn("alarm_region", lit("PROV"))

      val onlineFcstProvDF = provresultDF.selectExpr("eventtimestamp","custid", "alarm_region", "prov", "city", "status")

      val outputProvResult = outputPathProv + "/d=" + d + "/h=" + h + "/m5=" + m5
      onlineFcstProvDF.write.mode(SaveMode.Overwrite).format("orc").save(outputProvResult)
      ///
      val resultProv = sqlContext.read.format("orc").load(outputProvResult).
        map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getString(4))).collect()

      alarm2tidb(resultProv)
    }

    //nation------------
    val nationPath = new Path(inputPathNation + "/" + dataTime + "00.csv")
    if(fileSystem.exists(nationPath)){
      val nationDF = sc.textFile(inputPathNation + "/" + dataTime + "00.csv").map(x=>x.split(","))
        .filter(x=>x(1).length>1).map(x=>(x(0), x(1), x(2)))
        .toDF("eventtimestamp", "custid", "status")
      val nationTable = "nationTable"
      nationDF.registerTempTable(nationTable)
      ////
      val nationDF1 = sqlContext.sql(
        s"""
           |select eventtimestamp, custid, '-' as prov, '-' as city, status
           |from ${nationTable}
       """.stripMargin)

      val nationresultDF = nationDF1.withColumn("alarm_region", lit("NATION"))

      val onlineFcstNationDF = nationresultDF.selectExpr("eventtimestamp","custid", "alarm_region", "prov", "city", "status")

      val outputNationResult = outputPathNation + "/d=" + d + "/h=" + h + "/m5=" + m5
      onlineFcstNationDF.write.mode(SaveMode.Overwrite).format("orc").save(outputNationResult)
      ////
      val resultNation = sqlContext.read.format("orc").load(outputNationResult).
        map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getString(4))).collect()

      alarm2tidb(resultNation)
    }

  }

}
