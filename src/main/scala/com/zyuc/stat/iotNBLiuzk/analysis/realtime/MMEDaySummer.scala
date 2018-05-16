package com.zyuc.stat.iotNBLiuzk.analysis.realtime

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.iotNBLiuzk.analysis.realtime.utils.CommonUtils
import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-5-11.
  */
object MMEDaySummer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("name_20180504")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/epciot/data/mme/transform/nb/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/epciot/data/mme/summ_d/nb")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/epciot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val dd = dataTime.substring(0, 8)
    val partitionPath = s"/d=$d/h=*/m5=*"
    val dayPath = s"/d=$d"

    val MMETempTable = "MMETempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath).registerTempTable(MMETempTable)

    val IOTBSInfoTable = "IOTBSInfoTable"
    sqlContext.read.format("orc").load("/user/epciot/data/basic/IotBSInfo/data/").registerTempTable(IOTBSInfoTable)

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

    val sqlDayAll =
      s"""
         |select '${dd}' as summ_cycle, u.custid, i.provname as prov, i.cityname as city, i.enbid, m.pcause,
         |count(*) as REQS,
         |sum(case when m.result='failed' then 1 else 0 end) as FAILREQS,
         |count(distinct (case when m.result = 'failed' then m.MSISDN else '' end)) as FAILUSERS
         |from
         |${MMETempTable} m
         |left join
         |${IOTBSInfoTable} i
         |on (m.enbid=i.enbid and m.province=i.provname)
         |inner join
         |${userTable} u
         |on (u.mdn=m.msisdn)
         |group by u.custid, i.provname, i.cityname, i.enbid, m.pcause
       """.stripMargin

    //val rowTable = "rowtable"
    val resultDF = sqlContext.sql(sqlDayAll)//.registerTempTable(rowTable)

    val reqsDF4 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","pcause", "REQS as meas_value").
      withColumn("meas_obj", lit("REQS"))
    val reqsDF3 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","null", "REQS as meas_value").
      withColumn("meas_obj", lit("REQS"))
    val reqsDF2 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","null","null", "REQS as meas_value").
      withColumn("meas_obj", lit("REQS"))
    val reqsDF1 = resultDF.selectExpr("summ_cycle", "custid", "prov", "null","null","null", "REQS as meas_value").
      withColumn("meas_obj", lit("REQS"))

    val failreqsDF4 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","pcause", "FAILREQS as meas_value").
      withColumn("meas_obj", lit("FAILREQS"))
    val failreqsDF3 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","null", "FAILREQS as meas_value").
      withColumn("meas_obj", lit("FAILREQS"))
    val failreqsDF2 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","null","null", "FAILREQS as meas_value").
      withColumn("meas_obj", lit("FAILREQS"))
    val failreqsDF1 = resultDF.selectExpr("summ_cycle", "custid", "prov", "null","null","null", "FAILREQS as meas_value").
      withColumn("meas_obj", lit("FAILREQS"))

    val failusersDF4 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","pcause", "FAILUSERS as meas_value").
      withColumn("meas_obj", lit("FAILUSERS"))
    val failusersDF3 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","null", "FAILUSERS as meas_value").
      withColumn("meas_obj", lit("FAILUSERS"))
    val failusersDF2 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","null","null", "FAILUSERS as meas_value").
      withColumn("meas_obj", lit("FAILUSERS"))
    val failusersDF1 = resultDF.selectExpr("summ_cycle", "custid", "prov", "null","null","null", "FAILUSERS as meas_value").
      withColumn("meas_obj", lit("FAILUSERS"))

    // 将结果写入到hdfs
    val outputResult = outputPath + dayPath
    reqsDF4.unionAll(reqsDF3).unionAll(reqsDF2).unionAll(reqsDF1)
      .unionAll(failreqsDF4).unionAll(failreqsDF3).unionAll(failreqsDF2).unionAll(failreqsDF1)
      .unionAll(failusersDF4).unionAll(failusersDF3).unionAll(failusersDF2).unionAll(failusersDF1)
      .repartition(20)
      .write.mode(SaveMode.Overwrite).format("orc").save(outputResult)

    // 将结果写入到tidb, 需要调整为upsert
/*    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_nb_data_summMME_d
         |(summ_cycle, custid, prov,city, enbid, pcause, meas_obj, meas_value)
         |values (?,?,?,?,?,?,?,?)
         |on duplicate key update meas_value=?
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
    dbConn.close()*/

  }

}
