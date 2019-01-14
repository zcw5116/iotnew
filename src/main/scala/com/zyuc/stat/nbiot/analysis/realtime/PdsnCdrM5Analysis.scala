package com.zyuc.stat.nbiot.analysis.realtime

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-5-11 下午10:29.
  */
object PdsnCdrM5Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("PdsnM5Analysis_201810291510")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot_ete/data/cdr/transform/pdsn/data")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/iot/data/cdr/analy_realtime/pdsn")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val repartitionNum = sc.getConf.get("spark.app.repartitionNum", "10").toInt

    //3g : pdsn SID
    val pdsnSIDPath = sc.getConf.get("spark.app.pdsnSIDPath", "/user/iot/data/basic/pdsnSID/pdsnSID.txt")
    val pdsnSIDTable = "pdsnSIDTable"
    import sqlContext.implicits._
    sc.textFile(pdsnSIDPath).map(x=>x.split("\\t"))
      .map(x=>(x(0),x(1),x(2))).toDF("sid","provname","cityname").registerTempTable(pdsnSIDTable)

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d= dataTime.substring(2,8)
    val h= dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)

    val pdsnpath = inputPath + "/d=" + d + "/h=" + h + "/m5=" + m5

    val pdsnM5DF = sqlContext.read.format("orc").load(pdsnpath)
    val pdsnM5Table = "spark_nbm5"
    pdsnM5DF.registerTempTable(pdsnM5Table)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("is3g='Y' and is4g='N'")
    val tmpUserTable = "spark_tmpuser"
    userDF.registerTempTable(tmpUserTable)
    val userTable = "spark_user"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn, custid
         |from ${tmpUserTable}
       """.stripMargin)

    // 关联custid
    val pdsnTable = "spark_pdsn"
    sqlContext.sql(
      s"""
         |select u.custid, b.provname as prov, nvl(b.cityname,'-') as city,
         |       n.originating as upflow,
         |       n.termination as downflow
         |from ${pdsnM5Table} n, ${userTable} u, ${pdsnSIDTable} b
         |where n.mdn = u.mdn and n.sid=b.sid and n.acce_province=b.provname
       """.stripMargin).registerTempTable(pdsnTable)

    // 统计分析
     val resultStatDF = sqlContext.sql(
       s"""
          |select  custid, prov, city, upflow, downflow, (upflow + downflow) as totalflow
          |from
          |(
          |    select custid, prov, city, sum(upflow) as upflow, sum(downflow) as downflow
          |    from ${pdsnTable}
          |    group by custid, prov, city
          |) t
        """.stripMargin)

    val resultDF = resultStatDF.
      withColumn("gather_cycle", lit(dataTime + "00")).
      withColumn("gather_date", lit(dataTime.substring(0,8))).
      withColumn("gather_time", lit(dataTime.substring(8,12) + "00"))

    val inflowDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "city", "prov", "upflow as gather_value").
      withColumn("gather_type", lit("OUTFLOW"))

    val outflowDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "city", "prov", "downflow as gather_value").
      withColumn("gather_type", lit("INFLOW"))

    val totalflowDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "custid", "city", "prov", "totalflow as gather_value").
      withColumn("gather_type", lit("TOTALFLOW"))

    // 将结果写入到hdfs
    val outputResult = outputPath + "/d=" + d + "/h=" + h + "/m5=" + m5
    inflowDF.unionAll(outflowDF).unionAll(totalflowDF).filter("gather_value!=0").repartition(repartitionNum).write.mode(SaveMode.Overwrite).format("orc").save(outputResult)

    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_5min_3g_cdr_20${d}
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
    CommonUtils.updateBreakTable("iot_ana_5min_3g_cdr", dataTime+"00")

  }
}
