package com.zyuc.stat.iotNBLiuzk.analysis.realtime

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.iotNBLiuzk.analysis.realtime.utils.CommonUtils
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
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/epciot/data/mme/transform/nb/data")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/epciot/data/mme/analy_realtime/nb")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/epciot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val IotBSInfoPath = sc.getConf.get("spark.app.IotBSInfoPath", "/user/epciot/data/basic/IotBSInfo/data/")
    val IOTBSInfoTable = "IOTBSInfoTable"
    sqlContext.read.format("orc").load(IotBSInfoPath).registerTempTable(IOTBSInfoTable)

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
         |select u.custid, i.provname as prov, i.cityname as city,
         |count(*) as REQS ,
         |sum(case when m.result='failed' then 1 else 0 end) as FAILREQS ,
         |count(distinct (case when m.result = 'failed' then m.MSISDN else '' end)) as FAILUSERS
         |from ${nbM5Table} m
         |left join
         |${IOTBSInfoTable} i
         |on m.enbid=i.enbid and m.province=i.provname
         |inner join
         |${userTable} u
         |on u.mdn=m.msisdn
         |group by u.custid, i.provname, i.cityname
       """.stripMargin).registerTempTable(nbTable)

    // 统计分析
    val resultStatDF = sqlContext.sql(
      s"""
         |select custid, prov, city, REQS, FAILREQS, FAILUSERS
         |from ${nbTable}
         |group by custid, prov, city, REQS, FAILREQS, FAILUSERS
        """.stripMargin)

    val resultDF = resultStatDF.
      withColumn("gather_cycle", lit(dataTime + "00")).
      withColumn("gather_date", lit(dataTime.substring(0,8))).
      withColumn("gather_time", lit(dataTime.substring(8,12) + "00"))

    val reqsDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "prov", "city", "REQS as gather_value").
      withColumn("gather_type", lit("REQS"))

    val failreqsDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "prov", "city", "FAILREQS as gather_value").
      withColumn("gather_type", lit("FAILREQS"))

    val failusersDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "prov", "city", "FAILUSERS as gather_value").
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
        x.getString(4), x.getString(5), x.getDouble(6), x.getString(7))).collect()

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
      pstmt.setDouble(8, gather_value)
      pstmt.setDouble(9, gather_value)
      pstmt.setDouble(10, gather_value)
      pstmt.addBatch()
      if (i % 1000 == 0) {
        pstmt.executeBatch
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
