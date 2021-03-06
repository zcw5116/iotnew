package com.zyuc.stat.nbiot.analysis.realtime
import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-6-21.
  */
object NbOfflineM5Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("NbM5Analysis_201805211430")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/offline/transform/nb/data")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/iot/data/offline/analy_realtime/nb")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d= dataTime.substring(2,8)
    val h= dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)

    import sqlContext.implicits._
    val dataDF = sc.textFile(inputPath + "/*" + "/*" + dataTime + "*").map(x=>x.split(",")).filter(_.length==7)
      .filter(x=>x(1).length>1).map(x=>(x(1), x(2), x(3), x(4), x(5), x(6)))
      .toDF("custid", "prov", "city", "TerminateCause","num","sessionNum")
    val tmpTable = "spark_tmpoffline"
    dataDF.registerTempTable(tmpTable)

    val statDF = sqlContext.sql(
      s"""
         |select custid, nvl(prov, '-') as prov, nvl(city, '-') as city, TerminateCause, num,sessionNum from ${tmpTable}
       """.stripMargin)

    val resultDF = statDF.withColumn("gather_cycle", lit(dataTime + "00")).
      withColumn("gather_date", lit(dataTime.substring(0,8))).
      withColumn("gather_time", lit(dataTime.substring(8,12) + "00"))

    val onlineDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "city", "prov", "TerminateCause",
      "'OFFLINE' as gather_type", "num as gather_value", "sessionNum as session_value")

    val outputResult = outputPath + "/d=" + d + "/h=" + h + "/m5=" + m5
    onlineDF.write.mode(SaveMode.Overwrite).format("orc").save(outputResult)

    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_5min_nb_offline
         |(gather_cycle, gather_date, gather_time,cust_id, city, province, TerminateCause, gather_type, gather_value,session_value)
         |values (?,?,?,?,?,?,?,?,?,?)
         |on duplicate key update gather_value=?
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)
    val result = sqlContext.read.format("orc").load(outputResult).
      map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
        x.getString(4), x.getString(5), x.getString(6), x.getString(7), x.getString(8), x.getString(9))).collect()

    var i = 0
    for(r<-result){
      val gather_cycle = r._1
      val gather_date = r._2
      val gather_time = r._3
      val custid = r._4
      val prov = r._5
      val city = r._6
      val TerminateCause = r._7
      val gather_type = r._8
      val gather_value = r._9
      val session_value = r._10

      pstmt.setString(1, gather_cycle)
      pstmt.setString(2, gather_date)
      pstmt.setString(3, gather_time)
      pstmt.setString(4, custid)
      pstmt.setString(5, prov)
      pstmt.setString(6, city)
      pstmt.setString(7, TerminateCause)
      pstmt.setString(8, gather_type)
      pstmt.setString(9, gather_value)
      pstmt.setString(10, session_value)
      pstmt.setString(11, gather_value)

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
    CommonUtils.updateBreakTable("iot_ana_5min_nb_offline", dataTime+"00")
  }
}

