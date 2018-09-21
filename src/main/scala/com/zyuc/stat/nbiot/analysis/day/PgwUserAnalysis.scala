package com.zyuc.stat.nbiot.analysis.day

import java.sql.PreparedStatement

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoucw on 18-5-15 下午1:39.
  */
object PgwUserAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("userAnalysis_20180510").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/baseuser/data/")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/baseuser/summ_d/pgw/")
    val userDataDayid=sc.getConf.get("spark.app.userDataDayid","20180510")

    val dayid = appName.substring(appName.indexOf("_") + 1)


    val inputData = inputPath + "d=" + userDataDayid
    val pgwUserDF = sqlContext.read.format("orc").load(inputData).filter("is4g='Y'")
    val pgwTable = "spark_nb"
    pgwUserDF.registerTempTable(pgwTable)
    val statDF = sqlContext.sql(
      s"""
         |select custid,prodtype as dim_obj, cnt as meas_value,
         |ROW_NUMBER() over(partition by custid order by cnt desc) as meas_rank
         |from
         |(
         |    select custid, prodtype, count(*) as cnt
         |    from ${pgwTable}
         |    group by custid, prodtype
         |) t       """.stripMargin)

    val resultDF = statDF.
      withColumn("summ_cycle", lit(dayid)).
      withColumn("city", lit("-1")).
      withColumn("prov", lit("-1")).
      withColumn("district", lit("-1")).
      withColumn("dim_type", lit("INDUSTRY")).
      withColumn("meas_obj", lit("CRTUSERS"))

    // 将结果写入到hdfs
    val outputResult = outputPath + dayid
    resultDF.repartition(1).selectExpr("summ_cycle", "custid", "city", "prov", "district",
      "dim_type","dim_obj", "meas_obj", "meas_value", "meas_rank")
      .write.format("orc").mode(SaveMode.Overwrite).save(outputResult)

    // 将结果写入到tidb
    var dbConn = DbUtils.getDBConnection

    // 先删除结果
    val deleteSQL =
      s"""
         |delete from iot_ana_4g_data_summ_d where summ_cycle=? and meas_obj=?
       """.stripMargin
    var pstmt: PreparedStatement = null
    pstmt = dbConn.prepareStatement(deleteSQL)
    pstmt.setString(1, dayid)
    pstmt.setString(2, "CRTUSERS")
    pstmt.executeUpdate()
    pstmt.close()

    // 执行insert操作
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_4g_data_summ_d
         |(summ_cycle, cust_id, city, province, district, dim_type, dim_obj, meas_obj, meas_value,meas_rank)
         |values (?,?,?,?,?,?,?,?,?,?)
       """.stripMargin

    pstmt = dbConn.prepareStatement(sql)

    val result = sqlContext.read.format("orc").load(outputResult).
      map(x=>(x.getString(0), x.getString(1), x.getString(2),x.getString(3),x.getString(4),
        x.getString(5), x.getString(6),x.getString(7), x.getLong(8), x.getInt(9))).collect()

    var i = 0
    for(r<-result){
      //val size = r.productIterator.size
      val summ_cycle = r._1
      val custid = r._2
      val city = r._3
      val province = r._4
      val district = r._5
      val dim_type = r._6
      val dim_obj = r._7
      val meas_obj = r._8
      val meas_value = r._9
      val meas_rank = r._10

      pstmt.setString(1, summ_cycle)
      pstmt.setString(2, custid)
      pstmt.setString(3, city)
      pstmt.setString(4, province)
      pstmt.setString(5, district)
      pstmt.setString(6, dim_type)
      pstmt.setString(7, dim_obj)
      pstmt.setString(8, meas_obj)
      pstmt.setLong(9, meas_value)
      pstmt.setInt(10, meas_rank)

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

    CommonUtils.updateBreakTable("iot_4g_base", dayid)

  }
}

