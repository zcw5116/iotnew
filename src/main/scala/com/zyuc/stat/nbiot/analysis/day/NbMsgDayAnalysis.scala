package com.zyuc.stat.nbiot.analysis.day

import java.sql.PreparedStatement

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoucw on 18-5-11 下午10:29.
  */
object NbMsgDayAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("NbM5Analysis_20180510")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/msg/transform/nb/data")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/iot/data/msg/summ_d/nb")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dayid = appName.substring(appName.lastIndexOf("_") + 1)
    val d= dayid.substring(2,8)

    val nbpath = inputPath + "/d=" + d + "/h=*/m5*/"

    val nbM5DF = sqlContext.read.format("orc").load(nbpath)
    val nbM5Table = "spark_nbm5"
    nbM5DF.registerTempTable(nbM5Table)

    // 过滤NB的用户，将用户的数据广播出去，如果用户的数据过大， 需要修改参数spark.sql.autoBroadcastJoinThreshold，
    // 具体根据spark web ui的DGA执行计划调整, spark.sql.autoBroadcastJoinThreshold值默认为10M
    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("isnb='1'")
    val tmpUserTable = "spark_tmpuser"
    userDF.registerTempTable(tmpUserTable)
    val userTable = "spark_user"
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
         |select u.custid,
         |       'r' type, u.mdn
         |from ${nbM5Table} n, ${userTable} u
         |where n.called_number = u.mdn
         |union all
         |select u.custid,
         |       's' type, u.mdn
         |from ${nbM5Table} n, ${userTable} u
         |where n.calling_number = u.mdn
       """.stripMargin).registerTempTable(nbTable)


    // 统计分析
     val resultStatDF = sqlContext.sql(
       s"""
          |select custid, count(*) as msgnum, row_number() over(partition by custid order by custid) rn
          |from ${nbTable}
          |group by custid
        """.stripMargin)

    val resultDF = resultStatDF.
      withColumn("summ_cycle", lit(dayid)).
      withColumn("city", lit("-1")).
      withColumn("prov", lit("-1")).
      withColumn("district", lit("-1")).
      withColumn("dim_type", lit("-1")).
      withColumn("dim_obj", lit("-1")).
      withColumn("meas_obj", lit("MSGS")).
      withColumn("meas_rank", lit(1))


    // 将结果写入到hdfs
    val outputResult = outputPath + "/d=" + d
    resultDF.selectExpr("summ_cycle","custid", "city", "prov","district","dim_type", "dim_obj", "meas_obj", "meas_rank",
      "msgnum as meas_value").write.mode(SaveMode.Overwrite).format("orc").save(outputResult)


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
    pstmt.setString(2, "MSGS")
    pstmt.executeUpdate()
    pstmt.close()

    // 执行insert操作
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_nb_data_summ_d
         |(summ_cycle, cust_id, city, province, district, dim_type, dim_obj, meas_obj, meas_rank, meas_value)
         |values (?,?,?,?,?,?,?,?,?,?)
       """.stripMargin

    pstmt = dbConn.prepareStatement(sql)
    val result = sqlContext.read.format("orc").load(outputResult).
      map(x=>(x.getString(0), x.getString(1), x.getString(2),x.getString(3),x.getString(4), x.getString(5),
        x.getString(6), x.getString(7), x.getInt(8),x.getLong(9))).collect()

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
      val meas_rank = r._9
      val meas_value = r._10

      pstmt.setString(1, summ_cycle)
      pstmt.setString(2, custid)
      pstmt.setString(3, city)
      pstmt.setString(4, province)
      pstmt.setString(5, district)
      pstmt.setString(6, dim_type)
      pstmt.setString(7, dim_obj)
      pstmt.setString(8, meas_obj)
      pstmt.setInt(9, meas_rank)
      pstmt.setLong(10, meas_value)

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


  }
}
