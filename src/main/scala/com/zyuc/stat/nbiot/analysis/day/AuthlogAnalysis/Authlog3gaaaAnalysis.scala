package com.zyuc.stat.nbiot.analysis.day.AuthlogAnalysis

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 19-7-4.
  */
object Authlog3gaaaAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_201907041616")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/cdr/transform/auth/3gaaa/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/summ_d/authlog/3gaaa/")
    val pdsnipprovPath = sc.getConf.get("spark.app.pdsnipprovPath","/user/iot/data/cdr/transform/auth/3gaaa/pdsnIP_PROV")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(0, 8)
    val h = dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 11)
    val partitionPath = s"/d=$d/h=$h/m5=$m5*"

    // user, callid,    pdsnip, nas-port, nas-port-id,    nas-port-type, pcfip, result
    val table_3gaaa = "table_3gaaa"
    sqlContext.read.format("orc").load(inputPath + partitionPath).registerTempTable(table_3gaaa)
    // pdsnip, prov
    val table_ipprov = "table_ipprov"
    sqlContext.read.format("orc").load(pdsnipprovPath).registerTempTable(table_ipprov)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("is3g='Y' or is4g='Y'").selectExpr("custid","IMSI_3G")
    val tmpUserTable = "spark_tmpUser"
    userDF.registerTempTable(tmpUserTable)
    val userTable = "spark_User"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select *
         |from ${tmpUserTable}
       """.stripMargin)

    // 关联基本信息
    val df = sqlContext.sql(
      s"""
         |select custid, prov, a.pdsnip, count(*) cnt, count(distinct user) users,
         |       sum(case when result='0000' then 1 else 0 end) succCnt
         |from ${table_3gaaa} a
         |inner join ${table_ipprov} b on(a.pdsnip=b.pdsnip)
         |inner join ${userTable} c on(a.callid=c.IMSI_3G)
         |group by custid, prov, a.pdsnip
       """.stripMargin)

    df.selectExpr("custid", "prov", "pdsnip", "cnt", "users", "succCnt", "(succCnt/cnt) as rates")//round(succCnt/cnt,2)
      .write.format("orc").mode(SaveMode.Overwrite).save(outputPath + d + "/" + h + "/" + m5 + "0/")

    //return

//
//    val EHRPD_resultDF = EHRPD_provResultDF.filter("meas_value!=0")
//    // 将结果写入到hdfs
//    val EHRPD_outputResult = outputPath_EHRPD + dayPath
//    EHRPD_resultDF.coalesce(100).write.format("orc").mode(SaveMode.Overwrite).save(EHRPD_outputResult)
//
//    // ****************************************************************
//
//
//
//
//    // 将结果写入到tidb, 需要调整为upsert
//    var dbConn = DbUtils.getDBConnection
//    dbConn.setAutoCommit(false)
//    val sql =
//      s"""
//         |insert into iot_ana_3g_data_summ_d_$dd
//         |(summ_cycle, cust_id, city, province, district, dim_type, dim_obj, meas_obj, meas_value, meas_rank, nettype)
//         |values (?,?,?,?,?,?,?,?,?,?,?)
//       """.stripMargin
//
//    val pstmt = dbConn.prepareStatement(sql)
//    val result = sqlContext.read.format("orc").load(outputResult, EHRPD_outputResult).
//      map(x=>(x.getString(0), x.getString(1), x.getString(2),x.getString(3),x.getString(4),
//        x.getString(5), x.getString(6),x.getString(7), x.getDouble(8), x.getInt(9), x.getString(10))).collect()
//    //x.getLong(8)  -> x.getDouble(8)
//    var i = 0
//    for(r<-result){
//      val summ_cycle = r._1
//      val cust_id = r._2
//      val city = r._3
//      val province = r._4
//      val district = r._5
//      val dim_type = r._6
//      val dim_obj = r._7
//      val meas_obj = r._8
//      val meas_value = r._9
//      val meas_rank = r._10
//      val nettype = r._11
//
//      pstmt.setString(1, summ_cycle)
//      pstmt.setString(2, cust_id)
//      pstmt.setString(3, city)
//      pstmt.setString(4, province)
//      pstmt.setString(5, district)
//      pstmt.setString(6, dim_type)
//      pstmt.setString(7, dim_obj)
//      pstmt.setString(8, meas_obj)
//      pstmt.setLong(9, meas_value.toLong)
//      //pstmt.setLong(9, meas_value)
//      pstmt.setInt(10, meas_rank)
//      pstmt.setString(11, nettype)
//
//      i += 1
//      pstmt.addBatch()
//      if (i % 1000 == 0) {
//        pstmt.executeBatch
//        dbConn.commit()
//      }
//    }
//    pstmt.executeBatch
//    dbConn.commit()
//    pstmt.close()
//    dbConn.close()
//
//    //CommonUtils.updateBreakTable("iot_3g_TermType", dd)
//    CommonUtils.updateBreakTable("iot_3g_ActiveUser", dd)
//    CommonUtils.updateBreakTable("iot_3g_FluxDay", dd)

  }
}
