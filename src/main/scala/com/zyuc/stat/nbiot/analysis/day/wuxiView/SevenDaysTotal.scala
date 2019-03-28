package com.zyuc.stat.nbiot.analysis.day.wuxiView

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 19-03-28.
  *
  * 无锡视图
  *  统计签约地市为无锡 前七天在线数
  *
  *  是用原子基表的：nb3.30后 pgw3.后 pdsn6.后
  */
object SevenDaysTotal {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("wuxiView_20190328")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val inputPathNB = sc.getConf.get("spark.app.inputPathNB", "/user/iot_ete/data/cdr/summ_d/nb/")
    val inputPathPGW = sc.getConf.get("spark.app.inputPathNB", "/user/iot_ete/data/cdr/summ_d/pgw/")
    val inputPathPDSN = sc.getConf.get("spark.app.inputPathNB", "/user/iot_ete/data/cdr/summ_d/pdsn/")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/iot/data/prov/wuxiView/")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val onedayago = sc.getConf.get("spark.app.onedayago","20190327")
    val twodayago = sc.getConf.get("spark.app.twodayago","20190326")
    val threedayago = sc.getConf.get("spark.app.threedayago","20190325")
    val fourdayago = sc.getConf.get("spark.app.fourdayago","20190324")
    val fivedayago = sc.getConf.get("spark.app.fivedayago","20190323")
    val sixdayago = sc.getConf.get("spark.app.sixdayago","20190322")
    val sevendayago = sc.getConf.get("spark.app.sevendayago","20190321")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    //  CRM
    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath)
        .filter("belocity='无锡电信'")
        .selectExpr("mdn","isnb","is4g","is3g")
    val tmpUserTable = "spark_tmpuser"
    userDF.registerTempTable(tmpUserTable)
    val userTable = "spark_user"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select * from ${tmpUserTable}
       """.stripMargin)


    val tableNB = "tableNB"
    val tablePGW = "tablePGW"
    val tablePDSN = "tablePDSN"

    def createTmpTable(inputPath:String, tmpTable:String): Unit ={
      sqlContext.read.format("orc").load(inputPath + "dayid=" + onedayago,
        inputPath + "dayid=" + twodayago, inputPath + "dayid=" + threedayago,
        inputPath + "dayid=" + fourdayago, inputPath + "dayid=" + fivedayago,
        inputPath + "dayid=" + sixdayago, inputPath + "dayid=" + sevendayago)
        .selectExpr("mdn").registerTempTable(tmpTable)
    }

    createTmpTable(inputPathNB, tableNB)
    createTmpTable(inputPathPGW, tablePGW)
    createTmpTable(inputPathPDSN, tablePDSN)

    val dfnb = sqlContext.sql(
      s"""
         |select 'NB' as NETTYPE, count(distinct c.mdn) as NUM
         |from ${tableNB} c, ${userTable} u
         |where c.mdn = u.mdn and u.isnb='1'
           """.stripMargin)


    val dfpgw = sqlContext.sql(
      s"""
         |select '4G' as NETTYPE, count(distinct c.mdn) as NUM
         |from ${tablePGW} c, ${userTable} u
         |where c.mdn = u.mdn and u.is4g='Y'
           """.stripMargin)

    val dfpdsn = sqlContext.sql(
      s"""
         |select '3G' as NETTYPE, count(distinct c.mdn) as NUM
         |from ${tablePDSN} c, ${userTable} u
         |where c.mdn = u.mdn and u.is4g='Y' and u.is3g='Y'
           """.stripMargin)

    val resultDF = dfnb.unionAll(dfpgw).unionAll(dfpdsn)
    resultDF.write.format("orc").mode(SaveMode.Overwrite).save(outputPath + dataTime)


    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into IOT_CITY_ACTIVE_INFO
         |(DataTime, NETTYPE, NUM)
         |values (?,?,?)
         |on duplicate key update NUM=?
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)
    val result = sqlContext.read.format("orc").load(outputPath + dataTime).
      map(x=>(x.getString(0), x.getLong(1))).collect()

    var i = 0
    for(r<-result){
      val DataTime = dataTime
      val NETTYPE = r._1
      val NUM = r._2

      pstmt.setString(1, DataTime)
      pstmt.setString(2, NETTYPE)
      pstmt.setInt(3, NUM.toInt)
      pstmt.setInt(4, NUM.toInt)

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

    //CommonUtils.updateBreakTable("IOT_CITY_ACTIVE_INFO", dataTime)

  }
}
