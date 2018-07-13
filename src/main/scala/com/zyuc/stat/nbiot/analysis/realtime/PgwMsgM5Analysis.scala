package com.zyuc.stat.nbiot.analysis.realtime

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-7-13.
  */
object PgwMsgM5Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("PgwMsgM5Analysis_201805101400")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/msg/transform/nb/data")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/iot/data/msg/analy_realtime/pgw")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d= dataTime.substring(2,8)
    val h= dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)

    val pgwpath = inputPath + "/d=" + d + "/h=" + h + "/m5=" + m5

    val pgwM5DF = sqlContext.read.format("orc").load(pgwpath)
    val pgwM5Table = "spark_pgwm5"
    pgwM5DF.registerTempTable(pgwM5Table)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("isnb='0'")
    val tmpUserTable = "spark_tmpuser"
    userDF.registerTempTable(tmpUserTable)
    val userTable = "spark_user"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn, custid
         |from ${tmpUserTable}
         |where isnb = '0'
       """.stripMargin)

    // 关联custid
    val pgwTable = "spark_pgw"
    sqlContext.sql(
      s"""
         |select u.custid,
         |       'r' type, u.mdn
         |from ${pgwM5Table} n, ${userTable} u
         |where n.called_number = u.mdn
         |union all
         |select u.custid,
         |       's' type, u.mdn
         |from ${pgwM5Table} n, ${userTable} u
         |where n.calling_number = u.mdn
       """.stripMargin).registerTempTable(pgwTable)


    // 统计分析
    val resultStatDF = sqlContext.sql(
      s"""
         |select custid, count(*) as msgnum
         |from ${pgwTable}
         |group by custid
        """.stripMargin)

    val resultDF = resultStatDF.
      withColumn("prov", lit("-1")).
      withColumn("city", lit("-1")).
      withColumn("gather_cycle", lit(dataTime + "00")).
      withColumn("gather_date", lit(dataTime.substring(0,8))).
      withColumn("gather_time", lit(dataTime.substring(8,12) + "00")).
      withColumn("gather_type", lit("MSGS"))




    // 将结果写入到hdfs
    val outputResult = outputPath + "/d=" + d + "/h=" + h + "/m5=" + m5
    resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "prov", "city",
      "msgnum as gather_value", "gather_type").write.mode(SaveMode.Overwrite).format("orc").save(outputResult)

    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_5min_4g_msg
         |(gather_cycle, gather_date, gather_time,cust_id, city, province, gather_type, gather_value)
         |values (?,?,?,?,?,?,?,?)
         |on duplicate key update gather_value=?
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)
    val result = sqlContext.read.format("orc").load(outputResult).
      map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
        x.getString(4), x.getString(5), x.getLong(6), x.getString(7))).collect()

    var i = 0
    for(r<-result){
      val gather_cycle = r._1
      val gather_date = r._2
      val gather_time = r._3
      val custid = r._4
      val prov = r._5
      val city = r._6
      val gather_type = r._8
      val gather_value = r._7

      pstmt.setString(1, gather_cycle)
      pstmt.setString(2, gather_date)
      pstmt.setString(3, gather_time)
      pstmt.setString(4, custid)
      pstmt.setString(5, prov)
      pstmt.setString(6, city)
      pstmt.setString(7, gather_type)
      pstmt.setLong(8, gather_value)
      pstmt.setLong(9, gather_value)

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
    CommonUtils.updateBreakTable("iot_ana_5min_4g_msg", dataTime+"00")

  }
}
