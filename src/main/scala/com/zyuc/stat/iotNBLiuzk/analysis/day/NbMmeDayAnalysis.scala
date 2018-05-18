package com.zyuc.stat.iotNBLiuzk.analysis.day

import java.sql.PreparedStatement

import com.zyuc.iot.utils.DbUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoucw on 18-5-11.
  */
object NbMmeDayAnalysis {
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

    val mmeTmpTable = "MMETempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath).registerTempTable(mmeTmpTable)

    val bsInfoTable = "IOTBSInfoTable"
    sqlContext.read.format("orc").load("/user/epciot/data/basic/IotBSInfo/data/").registerTempTable(bsInfoTable)

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

    // 关联必要的信息
    val mdnTable = "spark_mdn"
    val mdnDF = sqlContext.sql(
      s"""
         |select u.mdn, u.custid, m.pcause, m.result, b.enbid, b.provname as prov, b.cityname as city
         |from ${mmeTmpTable} m
         |inner join ${userTable} u on(m.msisdn = u.mdn)
         |left join ${bsInfoTable} b on(m.enbid = b.enbid)
       """.stripMargin)
    mdnDF.registerTempTable(mdnTable)

    // 先删除数据
    mmeDelete(dataTime, "MMEREQS")
    mmeDelete(dataTime, "MMEFAILREQS")
    mmeDelete(dataTime, "MMEFAILUSERS")


    // MME成功率
    // 1. MME成功率-基站维度
    val bsTmpTable = "spark_bstmp"
     val bsStatDF = sqlContext.sql(
       s"""
          |select m.custid, m.enbid, count(*) as reqcnt,
          |       sum(case when m.result='failed' then 1 else 0 end) as failcnt
          |from ${mdnTable} m
          |group by m.custid, m.enbid
        """.stripMargin)
    bsStatDF.registerTempTable(bsTmpTable)
    val bsReqDF = bsStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "'BS' as dim_type", "enbid as dim_obj",
    "'-1' as dim_obj_2", "'MMEREQS' as meas_obj", "reqcnt as meas_value", "-1 as meas_rank")
    val bsReqFailDF = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'BS' as dim_type", "enbid as dim_obj",
      "'-1' as dim_obj_2", "'MMEFAILREQS' as meas_obj", "failcnt as meas_value", "-1 as meas_rank")
    val bsResultDF = bsReqFailDF.unionAll(bsReqDF)

    val bsResult = bsReqDF.map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
      x.getString(4), x.getString(5), x.getLong(6), x.getInt(7))).collect()

    // 数据入tidb
    mme2tidb(bsResult)

    //  2. MME成功率-市维度
    val cityStatDF = sqlContext.sql(
      s"""
         |select t.custid, b.cityname as city,
         |       sum(reqcnt) as reqcnt, sum(failcnt) as failcnt
         |from ${bsTmpTable} t, ${bsInfoTable} b
         |where t.enbid = b.enbid
         |group by t.custid, b.cityname
       """.stripMargin)
    val cityReqDF = cityStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "'CITY' as dim_type", "city as dim_obj",
      "'-1' as dim_obj_2", "'MMEREQS' as meas_obj", "reqcnt as meas_value", "-1 as meas_rank")
    val cityReqFailDF = cityStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'CITY' as dim_type", "city as dim_obj",
      "'-1' as dim_obj_2", "'MMEFAILREQS' as meas_obj", "failcnt as meas_value", "-1 as meas_rank")
    val cityResultDF = cityReqDF.unionAll(cityReqFailDF)

    val cityResult = cityResultDF.map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
      x.getString(4), x.getString(5), x.getLong(6), x.getInt(7))).collect()

    // 数据入tidb
   mme2tidb(cityResult)



    //  3. MME成功率-省维度
    val provStatDF = sqlContext.sql(
      s"""
         |select t.custid, b.provname as prov,
         |       sum(reqcnt) as reqcnt, sum(failcnt) as failcnt
         |from ${bsTmpTable} t, ${bsInfoTable} b
         |where t.enbid = b.enbid
         |group by t.custid, b.provname
       """.stripMargin)

    val provReqDF = provStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "'PROV' as dim_type", "prov as dim_obj",
      "'-1' as dim_obj_2", "'MMEREQS' as meas_obj", "reqcnt as meas_value", "-1 as meas_rank")
    val provReqFailDF = provStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'PROV' as dim_type", "prov as dim_obj",
      "'-1' as dim_obj_2", "'MMEFAILREQS' as meas_obj", "failcnt as meas_value", "-1 as meas_rank")
    val provResultDF = provReqDF.unionAll(provReqFailDF)

    val provResult = provResultDF.map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
      x.getString(4), x.getString(5), x.getLong(6), x.getInt(7))).collect()

    // 数据入tidb
    mme2tidb(provResult)


    //  4. MME成功率-企业维度
    val custStatDF = sqlContext.sql(
      s"""
         |select t.custid,
         |       sum(reqcnt) as reqcnt, sum(failcnt) as failcnt
         |from ${bsTmpTable} t, ${bsInfoTable} b
         |where t.enbid = b.enbid
         |group by t.custid
       """.stripMargin)

    val custReqDF = custStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "'-1' as dim_type", "'-1' as dim_obj",
      "'-1' as dim_obj_2", "'MMEREQS' as meas_obj", "reqcnt as meas_value", "-1 as meas_rank")
    val custReqFailDF = custStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'-1' as dim_type", "'-1' as dim_obj",
      "'-1' as dim_obj_2", "'MMEFAILREQS' as meas_obj", "failcnt as meas_value", "-1 as meas_rank")
    val custResultDF = custReqDF.unionAll(custReqFailDF)

    val custResult = custResultDF.map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
      x.getString(4), x.getString(5), x.getLong(6), x.getInt(7))).collect()

    // 数据入tidb
    mme2tidb(custResult)




    // 1. 失败原因-企业维度
    val custFailDF = sqlContext.sql(
      s"""
         |select custid, pcause, failcnt, failusers,
         |       row_number() over(partition by custid order by failcnt desc) as failrank
         |from
         |(
         |    select m.custid, m.pcause, count(*) failcnt, count(distinct mdn) as failusers
         |    from ${mdnTable} m
         |    where m.result='failed'
         |    group by m.custid, m.pcause
         |) t
       """.stripMargin)

    val custFailcntDF = custFailDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "'-1' as dim_type", "'-1' as dim_obj",
      "pcause as dim_obj_2", "'MMEFAILREQS' as meas_obj", "failcnt as meas_value", "failrank as meas_rank")
    val custFailReqDF = custFailDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'-1' as dim_type", "'-1' as dim_obj",
      "pcause as dim_obj_2", "'MMEFAILUSERS' as meas_obj", "failusers as meas_value", "-1 as meas_rank")
    val custFailResultDF = custFailcntDF.unionAll(custFailReqDF)

    val custFailResult = custFailResultDF.map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
      x.getString(4), x.getString(5), x.getLong(6), x.getInt(7))).collect()
    // 数据入tidb
    mme2tidb(custFailResult)




    // 2. 失败原因-企业维度
    val provFailDF = sqlContext.sql(
      s"""
         |select custid, prov, pcause, failcnt, failusers,
         |       row_number() over(partition by custid order by failcnt desc) as failrank
         |from
         |(
         |    select m.custid, m.prov, m.pcause, count(*) failcnt, count(distinct mdn) as failusers
         |    from ${mdnTable} m
         |    where m.result='failed'
         |    group by m.custid, m.prov, m.pcause
         |) t
       """.stripMargin)

    val provFailcntDF = provFailDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "'PROV' as dim_type", "prov as dim_obj",
      "pcause as dim_obj_2", "'MMEFAILREQS' as meas_obj", "failcnt as meas_value", "failrank as meas_rank")
    val provFailReqDF = provFailDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'PROV' as dim_type", "prov as dim_obj",
      "pcause as dim_obj_2", "'MMEFAILUSERS' as meas_obj", "failusers as meas_value", "-1 as meas_rank")
    val provFailResultDF = provFailcntDF.unionAll(provFailReqDF)

    val provFailResult = provFailResultDF.map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
      x.getString(4), x.getString(5), x.getLong(6), x.getInt(7))).collect()
    // 数据入tidb
    mme2tidb(provFailResult)


    // 3. 失败原因-地市维度
    val cityFailDF = sqlContext.sql(
      s"""
         |select custid, city, pcause, failcnt, failusers,
         |       row_number() over(partition by custid order by failcnt desc) as failrank
         |from
         |(
         |    select m.custid, m.city, m.pcause, count(*) failcnt, count(distinct mdn) as failusers
         |    from ${mdnTable} m
         |    where m.result='failed'
         |    group by m.custid, m.city, m.pcause
         |) t
       """.stripMargin)
    val cityFailcntDF = cityFailDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "'CITY' as dim_type", "city as dim_obj",
      "pcause as dim_obj_2", "'MMEFAILREQS' as meas_obj", "failcnt as meas_value", "failrank as meas_rank")
    val cityFailReqDF = cityFailDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'CITY' as dim_type", "city as dim_obj",
      "pcause as dim_obj_2", "'MMEFAILUSERS' as meas_obj", "failusers as meas_value", "-1 as meas_rank")
    val cityFailResultDF = cityFailcntDF.unionAll(cityFailReqDF)

    val cityFailResult = cityFailResultDF.map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
      x.getString(4), x.getString(5), x.getLong(6), x.getInt(7))).collect()
    // 数据入tidb
    mme2tidb(cityFailResult)



    // 4. 失败原因-基站维度
    val bsFailDF = sqlContext.sql(
      s"""
         |select custid, enbid, pcause, failcnt, failusers,
         |       row_number() over(partition by custid order by failcnt desc) as failrank
         |from
         |(
         |    select m.custid, m.enbid, m.pcause, count(*) failcnt, count(distinct mdn) as failusers
         |    from ${mdnTable} m
         |    where m.result='failed'
         |    group by m.custid, m.enbid, m.pcause
         |) t
       """.stripMargin)

    val bsFailcntDF = bsFailDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "'BS' as dim_type", "enbid as dim_obj",
      "pcause as dim_obj_2", "'MMEFAILREQS' as meas_obj", "failcnt as meas_value", "failrank as meas_rank")
    val bsFailReqDF = bsFailDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'BS' as dim_type", "enbid as dim_obj",
      "pcause as dim_obj_2", "'MMEFAILUSERS' as meas_obj", "failusers as meas_value", "-1 as meas_rank")
    val bsFailResultDF = bsFailcntDF.unionAll(bsFailReqDF)

    val bsFailResult = bsFailResultDF.map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
      x.getString(4), x.getString(5), x.getLong(6), x.getInt(7))).collect()
    // 数据入tidb
    mme2tidb(bsFailResult)

  }

  def mmeDelete(summCycle:String, meas_obj:String) = {
    // 将结果写入到tidb
    var dbConn = DbUtils.getDBConnection

    // 先删除结果
    val deleteSQL =
      s"""
         |delete from iot_ana_nb_data_summ_d where summ_cycle=? and meas_obj=?
       """.stripMargin
    var pstmt: PreparedStatement = null
    pstmt = dbConn.prepareStatement(deleteSQL)
    pstmt.setString(1, summCycle)
    pstmt.setString(2, meas_obj)
    pstmt.executeUpdate()
    pstmt.close()
    dbConn.close()
  }

  def mme2tidb(result: Array[(String, String, String, String, String, String, Long, Int)]) = {
    // 将结果写入到tidb
    var dbConn = DbUtils.getDBConnection

    // 执行insert操作
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_nb_data_summ_d
         |(summ_cycle, cust_id, dim_type, dim_obj, dim_obj_2, meas_obj, meas_value, meas_rank)
         |values (?,?,?,?,?,?,?,?)
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)

    var i = 0
    for(r<-result){
      //val size = r.productIterator.size
      val summ_cycle = r._1
      val custid = r._2
      val dim_type = r._3
      val dim_obj = r._4
      val dim_obj_2 = r._5
      val meas_obj = r._6
      val meas_value = r._7
      val meas_rank = r._8


      pstmt.setString(1, summ_cycle)
      pstmt.setString(2, custid)
      pstmt.setString(3, dim_type)
      pstmt.setString(4, dim_obj)
      pstmt.setString(5, dim_obj_2)
      pstmt.setString(6, meas_obj)
      pstmt.setLong(7, meas_value)
      pstmt.setLong(8, meas_rank)
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
