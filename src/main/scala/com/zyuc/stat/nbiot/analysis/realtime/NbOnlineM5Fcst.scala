package com.zyuc.stat.nbiot.analysis.realtime

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-6-29.
  */
object NbOnlineM5Fcst {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("NbM5Analysis_201805211430")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPathCity = sc.getConf.get("spark.app.inputPath", "/user/iot/data/online_fcst/transform/nb/data/city")
    val inputPathProv = sc.getConf.get("spark.app.inputPath", "/user/iot/data/online_fcst/transform/nb/data/prov")
    val inputPathNation = sc.getConf.get("spark.app.inputPath", "/user/iot/data/online_fcst/transform/nb/data/nation")
    val outputPathCity = sc.getConf.get("spark.app.outputPath", "/user/iot/data/online_fcst/analy_realtime/nb/city")
    val outputPathProv = sc.getConf.get("spark.app.outputPath", "/user/iot/data/online_fcst/analy_realtime/nb/prov")
    val outputPathNation = sc.getConf.get("spark.app.outputPath", "/user/iot/data/online_fcst/analy_realtime/nb/nation")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d= dataTime.substring(2,8)
    val h= dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)

    import sqlContext.implicits._
    val cityDF = sc.textFile(inputPathCity + "/*" + "/*" + dataTime + "*").map(x=>x.split(",")).filter(_.length==6)
      .filter(x=>x(1).length>1).map(x=>(x(0), x(1), x(2), x(3), x(4), x(5)))
      .toDF("custid", "prov", "city", "low_value", "high_value", "fcst_value")
    val cityTable = "cityTable"
    cityDF.registerTempTable(cityTable)

    val provDF = sc.textFile(inputPathProv + "/*" + "/*" + dataTime + "*").map(x=>x.split(",")).filter(_.length==5)
      .filter(x=>x(1).length>1).map(x=>(x(0), x(1), x(2), x(3), x(4)))
      .toDF("custid", "prov", "low_value", "high_value", "fcst_value")
    val provTable = "provTable"
    provDF.registerTempTable(provTable)

    val nationDF = sc.textFile(inputPathNation + "/*" + "/*" + dataTime + "*").map(x=>x.split(",")).filter(_.length==4)
      .filter(x=>x(1).length>1).map(x=>(x(0), x(1), x(2), x(3)))
      .toDF("custid", "low_value", "high_value", "fcst_value")
    val nationTable = "nationTable"
    nationDF.registerTempTable(nationTable)

    //city
    val cityDF1 = sqlContext.sql(
      s"""
         |select custid, nvl(prov, '-') as prov, nvl(city, '-') as city, low_value, high_value, fcst_value
         |from ${cityTable}
       """.stripMargin)

    val cityresultDF = cityDF1.withColumn("gather_cycle", lit(dataTime + "00")).
      withColumn("gather_date", lit(dataTime.substring(0,8))).
      withColumn("gather_time", lit(dataTime.substring(8,12) + "00"))

    val onlineFcstCityDF = cityresultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "city", "prov",
      "'CITY' as gather_type", "low_value", "high_value", "fcst_value")

    val outputCityResult = outputPathCity + "/d=" + d + "/h=" + h + "/m5=" + m5
    onlineFcstCityDF.write.mode(SaveMode.Overwrite).format("orc").save(outputCityResult)
    //prov
    val provDF1 = sqlContext.sql(
      s"""
         |select custid, nvl(prov, '-') as prov, '-' as city, low_value, high_value, fcst_value
         |from ${provTable}
       """.stripMargin)

    val provresultDF = provDF1.withColumn("gather_cycle", lit(dataTime + "00")).
      withColumn("gather_date", lit(dataTime.substring(0,8))).
      withColumn("gather_time", lit(dataTime.substring(8,12) + "00"))

    val onlineFcstProvDF = provresultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "city", "prov",
      "'PROV' as gather_type", "low_value", "high_value", "fcst_value")

    val outputProvResult = outputPathProv + "/d=" + d + "/h=" + h + "/m5=" + m5
    onlineFcstProvDF.write.mode(SaveMode.Overwrite).format("orc").save(outputProvResult)
    //nation
    val nationDF1 = sqlContext.sql(
      s"""
         |select custid, '-' as prov, '-' as city, low_value, high_value, fcst_value
         |from ${nationTable}
       """.stripMargin)

    val nationresultDF = nationDF1.withColumn("gather_cycle", lit(dataTime + "00")).
      withColumn("gather_date", lit(dataTime.substring(0,8))).
      withColumn("gather_time", lit(dataTime.substring(8,12) + "00"))

    val onlineFcstNationDF = nationresultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "city", "prov",
      "'NATION' as gather_type", "low_value", "high_value", "fcst_value")

    val outputNationResult = outputPathNation + "/d=" + d + "/h=" + h + "/m5=" + m5
    onlineFcstNationDF.write.mode(SaveMode.Overwrite).format("orc").save(outputNationResult)


    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_5min_nb_online_fcst
         |(gather_cycle, gather_date, gather_time,cust_id, city, province, gather_type, low_value, high_value, fcst_value)
         |values (?,?,?,?,?,?,?,?,?,?)
         |on duplicate key update fcst_value=?
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)
    val resultCity = sqlContext.read.format("orc").load(outputCityResult).
      map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
        x.getString(4), x.getString(5), x.getString(6), x.getString(7), x.getString(8), x.getString(9)))//.collect()
    val resultProv = sqlContext.read.format("orc").load(outputProvResult).
      map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
        x.getString(4), x.getString(5), x.getString(6), x.getString(7), x.getString(8), x.getString(9)))//.collect()
    val resultNation = sqlContext.read.format("orc").load(outputNationResult).
      map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
        x.getString(4), x.getString(5), x.getString(6), x.getString(7), x.getString(8), x.getString(9)))//.collect()
    //三个合并
    val result = resultCity.union(resultProv).union(resultNation).collect()

    var i = 0
    for(r<-result){
      val gather_cycle = r._1
      val gather_date = r._2
      val gather_time = r._3
      val custid = r._4
      val prov = r._5
      val city = r._6
      val gather_type = r._7
      val low_value = r._8
      val high_value = r._9
      val fcst_value = r._10

      pstmt.setString(1, gather_cycle)
      pstmt.setString(2, gather_date)
      pstmt.setString(3, gather_time)
      pstmt.setString(4, custid)
      pstmt.setString(5, prov)
      pstmt.setString(6, city)
      pstmt.setString(7, gather_type)
      pstmt.setString(8, low_value)
      pstmt.setString(9, high_value)
      pstmt.setString(10, fcst_value)
      pstmt.setString(11, fcst_value)

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

    // 更新断点时间
    CommonUtils.updateBreakTable("iot_ana_5min_nb_online_fcst", dataTime+"00")



  }
}
