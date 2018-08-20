package com.zyuc.stat.nbiot.analysis.realtime.nb_prov

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 18-8-7.
  */
object NbOnlineM5Analysis_prov {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("NbCdrM5AnalysisProv_201808061732)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name","NbOnlineM5AnalysisProv_201808061732")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/online_prov/transform/nb/data")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/iot/data/online_prov/analy_realtime/nb")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d= dataTime.substring(2,8)
    val h= dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)

    import sqlContext.implicits._
    sc.textFile(inputPath + "/*" + "/*" + dataTime + "*")
      .map(line=>line.split(",")).map(x=>(x(0)+"00",x(1),x(2),x(3),x(5),x(4),x(6),x(7)))//---x(5),x(4),
      .toDF("gather_cycle","btsid","province","city","town","region","bts", "num").registerTempTable("provTable")

    val statDF = sqlContext.sql("select * from provTable")
    val resultDF = statDF.withColumn("gather_date", lit(dataTime.substring(0,8)))
      .withColumn("gather_time", lit(dataTime.substring(8,12) + "00"))

    val onlineDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "btsid", "province", "city","town","region","bts", "'ONLINEUSERS' as gather_type", "num as gather_value")

    val outputResult = outputPath + "/d=" + d + "/h=" + h + "/m5=" + m5
    onlineDF.coalesce(1).write.mode(SaveMode.Overwrite).format("orc").save(outputResult)

    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_5min_prov_nb_stat
         |(gather_cycle, gather_date, gather_time, btsid, province, city, town, region, bts, gather_type, gather_value)
         |values (?,?,?,?,?,?,?,?,?,?,?)
         |on duplicate key update gather_value=?
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)
    val result = sqlContext.read.format("orc").load(outputResult).
      map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),x.getString(4),
        x.getString(5), x.getString(6), x.getString(7), x.getString(8), x.getString(9), x.getString(10))).collect()

    var i = 0
    for(r<-result){
      val gather_cycle = r._1
      val gather_date = r._2
      val gather_time = r._3
      val btsid = r._4
      val province = r._5
      val city = r._6
      val town = r._7
      val region = r._8
      val bts = r._9
      val gather_type = r._10
      val gather_value = r._11

      pstmt.setString(1, gather_cycle)
      pstmt.setString(2, gather_date)
      pstmt.setString(3, gather_time)
      pstmt.setString(4, btsid)
      pstmt.setString(5, province)
      pstmt.setString(6, city)
      pstmt.setString(7, town)
      pstmt.setString(8, region)
      pstmt.setString(9, bts)
      pstmt.setString(10, gather_type)
      pstmt.setString(11, gather_value)
      pstmt.setString(12, gather_value)

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

    CommonUtils.updateBreakTable("iot_ana_5min_prov_nb_stat_online", dataTime+"00")


  }

}
