package com.zyuc.stat.nbiot.analysis.realtime

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-5-14.
  */

object NbMmeM5Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("NBM5Analysis_201805041530")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/mme/transform/nb/data")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/iot/data/mme/analy_realtime/nb")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val iotBSInfoPath = sc.getConf.get("spark.app.IotBSInfoPath", "/user/iot/data/basic/IotBSInfo/data/")
    val bsInfoTable = "IOTBSInfoTable"
    sqlContext.read.format("orc").load(iotBSInfoPath).registerTempTable(bsInfoTable)

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d= dataTime.substring(2,8)
    val h= dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)

    val nbpath = inputPath + "/d=" + d + "/h=" + h + "/m5=" + m5

    val nbM5DF = sqlContext.read.format("orc").load(nbpath)
    val nbM5Table = "spark_nbM5"
    nbM5DF.registerTempTable(nbM5Table)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("isnb='1'")
    val tmpUserTable = "spark_tmpUser"
    userDF.registerTempTable(tmpUserTable)
    val userTable = "spark_User"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn, custid
         |from ${tmpUserTable}
         |where isnb = '1'
       """.stripMargin)

    // 关联custid
    val nbTable = "spark_nb"
    sqlContext.sql(
      s"""
         |select u.custid, u.mdn, b.provname as prov, nvl(b.cityname,'-') as city, m.result
         |from ${nbM5Table} m, ${userTable} u, ${bsInfoTable} b
         |where u.mdn=m.msisdn and m.enbid=b.enbid
       """.stripMargin).registerTempTable(nbTable)

    val statDF = sqlContext.sql(
      s"""
         |select a.custid, a.prov, a.city, a.reqcnt, a.failreqcnt, nvl(b.failusers,0) as failusers
         |from
         |(select custid, prov, city, count(*) as reqcnt,
         |       sum(case when result='failed' then 1 else 0 end) as failreqcnt
         |from ${nbTable}
         |group by custid, prov, city
         |)a
         |left join
         |(select custid, prov, city, count(distinct mdn) as failusers
         |from ${nbTable} where result='failed' group by custid, prov, city
         |)b
         |on (a.custid=b.custid and a.prov=b.prov and a.city=b.city)
       """.stripMargin)


    val resultDF = statDF.
      withColumn("gather_cycle", lit(dataTime + "00")).
      withColumn("gather_date", lit(dataTime.substring(0,8))).
      withColumn("gather_time", lit(dataTime.substring(8,12) + "00"))

    val reqsDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "city", "prov",
      "reqcnt as gather_value").
      withColumn("gather_type", lit("REQS"))

    val failreqsDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "city", "prov",
      "failreqcnt as gather_value").
      withColumn("gather_type", lit("FAILREQS"))

    val failusersDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "city", "prov",
      "failusers as gather_value").
      withColumn("gather_type", lit("FAILUSERS"))

    // 将结果写入到 hdfs
    val outputResult = outputPath + "/d=" + d + "/h=" + h + "/m5=" + m5
    reqsDF.unionAll(failreqsDF).unionAll(failusersDF).write.mode(SaveMode.Overwrite).format("orc").save(outputResult)

    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_5min_nb_mme
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
      val city = r._5
      val province = r._6
      val gather_type = r._8
      val gather_value = r._7

      pstmt.setString(1, gather_cycle)
      pstmt.setString(2, gather_date)
      pstmt.setString(3, gather_time)
      pstmt.setString(4, custid)
      pstmt.setString(5, city)
      pstmt.setString(6, province)
      pstmt.setString(7, gather_type)
      pstmt.setDouble(8, gather_value)
      pstmt.setDouble(9, gather_value)

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
    CommonUtils.updateBreakTable("iot_ana_5min_nb_mme", dataTime+"00")


  }
}
