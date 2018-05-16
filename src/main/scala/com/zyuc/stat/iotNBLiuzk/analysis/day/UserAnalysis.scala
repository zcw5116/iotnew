package com.zyuc.stat.iotNBLiuzk.analysis.day

import java.sql.PreparedStatement

import com.zyuc.iot.utils.DbUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoucw on 18-5-15 下午1:39.
  */
object UserAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("userAnalysis_20180510").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/epciot/data/baseuser/data/")

    val dayid = appName.substring(appName.indexOf("_") + 1)

    val inputData = inputPath + "d=" + dayid
    val nbUserDF = sqlContext.read.format("orc").load(inputData).filter("isnb='1'")
    val nbTable = "spark_nb"
    nbUserDF.registerTempTable(nbTable)
    val statDF = sqlContext.sql(
      s"""
         |select custid, prodtype as dim_obj, cnt as meas_value,
         |ROW_NUMBER() over(partition by custid order by cnt desc) as meas_rank
         |from
         |(
         |    select custid, prodtype, count(*) as cnt
         |    from ${nbTable}
         |    group by custid, prodtype
         |) t       """.stripMargin)

    val resultDF = statDF.
      withColumn("summ_cycle", lit(dayid)).
      withColumn("dim_type", lit("INDUSTRY")).
      withColumn("meas_obj", lit("CRTUSERS"))

    val result = resultDF.selectExpr("summ_cycle", "custid", "dim_type",
      "dim_obj", "meas_obj",
      "meas_rank", "meas_value").
      map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
        x.getString(4), x.getInt(5),x.getLong(6))).collect()


    // 将结果写入到tidb
    var dbConn = DbUtils.getDBConnection

    // 先删除结果
    val deleteSQL =
      s"""
         |delete from iot_ana_nb_data_summ_d where summ_cycle=? and meas_obj=?
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
         |insert into iot_ana_nb_data_summ_d
         |(summ_cycle, cust_id, dim_type, dim_obj, meas_obj, meas_rank, meas_value)
         |values (?,?,?,?,?,?,?)
       """.stripMargin

    pstmt = dbConn.prepareStatement(sql)

    var i = 0
    for(r<-result){
      //val size = r.productIterator.size
      val summ_cycle = r._1
      val custid = r._2
      val dim_type = r._3
      val dim_obj = r._4
      val meas_obj = r._5
      val meas_rank = r._6
      val meas_value = r._7

      pstmt.setString(1, summ_cycle)
      pstmt.setString(2, custid)
      pstmt.setString(3, dim_type)
      pstmt.setString(4, dim_obj)
      pstmt.setString(5, meas_obj)
      pstmt.setInt(6, meas_rank)
      pstmt.setLong(7, meas_value)
      pstmt.addBatch()
      if (i % 1000 == 0) {
        pstmt.executeBatch
      }
    }
    pstmt.executeBatch
    dbConn.commit()
    pstmt.close()
    dbConn.close()

  }
}