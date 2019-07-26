package com.zyuc.stat.nbiot.analysis.day

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-10-29.
  */
object PdsnCdrDayAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20181029")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot_ete/data/cdr/transform/pdsn/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/summ_d/pdsn")
    //pdsn天汇总中间表临时缓存目录
    val tmpCachePathPdsn = sc.getConf.get("spark.app.tmpCachePathPdsn","/user/iot/data/cdr/summ_d/pdsn_tmpCache")
    val tmpCachePathEhrpd = sc.getConf.get("spark.app.tmpCachePathEhrpd","/user/iot/data/cdr/summ_d/pgw_ehrpd_tmpCache")
    val outputPath_EHRPD = sc.getConf.get("spark.app.outputPath_EHRPD","/user/iot/data/cdr/summ_d/ehrpd")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val dd = dataTime.substring(0, 8)
    val partitionPath = s"/d=$d/h=*/m5=*"
    val dayPath = s"/d=$d"

    val cdrTempTable = "CDRTempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath)
      .filter("acct_status_type='2' and (service_option='59' or service_option='33')")//新增结束 代表真实流量 59:3g 33:2g
      .selectExpr("acce_province as prov","acce_region as city","sid as enbid","originating as upflow",
        "termination as downflow", "mdn","account_session_id as chargingid", "service_option")
      .registerTempTable(cdrTempTable)

    import sqlContext.implicits._
//    val terminalTable = "IOTTerminalTable"
//    sc.textFile("/user/iot/data/basic/IOTTerminal/iotterminal.csv")
//      .filter(_.length>3)
//      .map(line=>line.replace("\"", "").split(",",5)).map(x=>(x(0),x(3)))
//      .toDF("tac", "modelname").registerTempTable(terminalTable)

    //3g : pdsn SID
    val pdsnSIDPath = sc.getConf.get("spark.app.pdsnSIDPath", "/user/iot/data/basic/pdsnSID/pdsnSID.txt")
    val pdsnSIDTable = "pdsnSIDTable"
    sc.textFile(pdsnSIDPath).map(x=>x.split("\\t"))
      .map(x=>(x(0),x(1),x(2))).toDF("sid","provname","cityname").registerTempTable(pdsnSIDTable)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("is3g='Y' or is4g='Y'")
    val tmpUserTable = "spark_tmpUser"
    userDF.registerTempTable(tmpUserTable)
    val userTable = "spark_User"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn, custid
         |from ${tmpUserTable}
       """.stripMargin)

    // 关联基本信息
    val mdnDF = sqlContext.sql(
      s"""
         |select nvl(u.custid,'未知') as custid, c.mdn, c.enbid, b.provname as prov, nvl(b.cityname,'-') as city,
         |       c.upflow, c.downflow,  c.chargingid, service_option
         |from ${cdrTempTable} c
         |left join ${userTable} u on(c.mdn = u.mdn)
         |left join ${pdsnSIDTable} b on(c.enbid = b.sid and c.prov=b.provname)
       """.stripMargin)

    // 中间结果集 先保存到缓存路径
    val cdrMdnTable = "spark_cdrmdn"
    mdnDF.repartition(100).write.format("orc").mode(SaveMode.Overwrite).save(tmpCachePathPdsn + "/table_joined")
    sqlContext.read.format("orc").load(tmpCachePathPdsn + "/table_joined").registerTempTable(cdrMdnTable)

    // 基站的信息
    val bsStatDF = sqlContext.sql(
      s"""select custid, enbid, prov, city, '-1' as district, service_option,
         |       upflow, downflow,(upflow + downflow) as totalflow, activeUsers, activeSessions,
         |       row_number() over(partition by custid order by upflow) as uprank,
         |       row_number() over(partition by custid order by downflow) as downrank,
         |       row_number() over(partition by custid order by activeUsers) as activerank,
         |       row_number() over(partition by custid order by activeSessions) as sessionrank
         |
         |from(
         |    select custid, enbid, prov, city, service_option,
         |           sum(upflow) as upflow, sum(downflow) as downflow,
         |           count(distinct mdn) as activeUsers, count(chargingid) as activeSessions
         |    from ${cdrMdnTable}
         |    group by custid, enbid, prov, city, service_option
         |) t
       """.stripMargin)
    val bsUpFlow = bsStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid","city","prov", "district","'BS' as dim_type", "enbid as dim_obj", "'OUTFLOW' as meas_obj", "upflow as meas_value", "uprank as meas_rank")
    val bsDownFlow = bsStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid", "city","prov","district","'BS' as dim_type", "enbid as dim_obj", "'INFLOW' as meas_obj", "downflow as meas_value", "downrank as meas_rank")
    val bsTotalFlow = bsStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid", "city","prov","district","'BS' as dim_type", "enbid as dim_obj", "'TOTALFLOW' as meas_obj", "totalflow as meas_value", "downrank as meas_rank")
    val bsActiveFlow = bsStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid", "city","prov","district","'BS' as dim_type", "enbid as dim_obj", "'ACTIVEUSERS' as meas_obj", "activeUsers as meas_value", "activerank as meas_rank")
    val bsSessionFlow = bsStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid", "city","prov","district","'BS' as dim_type", "enbid as dim_obj", "'SESSIONS' as meas_obj", "activeSessions as meas_value", "sessionrank as meas_rank")

    // nettype    ehrpd evdo  cdma1x
    val bsResultDF = bsUpFlow.unionAll(bsDownFlow).unionAll(bsTotalFlow).unionAll(bsActiveFlow).unionAll(bsSessionFlow)

    // 省份
    val provStatDF = sqlContext.sql(
      s"""select custid, prov, '-1' as city, '-1' as district, service_option,
         |       upflow, downflow, (upflow + downflow) as totalflow,activeUsers, activeSessions,
         |       row_number() over(partition by custid order by upflow) as uprank,
         |       row_number() over(partition by custid order by downflow) as downrank,
         |       row_number() over(partition by custid order by activeUsers) as activerank,
         |       row_number() over(partition by custid order by activeSessions) as sessionrank
         |from
         |(
         |    select custid, prov, service_option,
         |           sum(upflow) as upflow, sum(downflow) as downflow,
         |           count(distinct mdn) as activeUsers, count(chargingid) as activeSessions
         |    from ${cdrMdnTable}
         |    group by custid, prov, service_option
         |) t
       """.stripMargin)
    val provUpFlow = provStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'OUTFLOW' as meas_obj", "upflow as meas_value", "uprank as meas_rank")
    val provDownFlow = provStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid","city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'INFLOW' as meas_obj", "downflow as meas_value", "downrank as meas_rank")
    val provTotalFlow = provStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid","city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'TOTALFLOW' as meas_obj", "totalflow as meas_value", "downrank as meas_rank")
    val provActiveFlow = provStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid","city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'ACTIVEUSERS' as meas_obj", "activeUsers as meas_value", "activerank as meas_rank")
    val provSessionFlow = provStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid","city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'SESSIONS' as meas_obj", "activeSessions as meas_value", "sessionrank as meas_rank")

    val provResultDF = provUpFlow.unionAll(provDownFlow).unionAll(provTotalFlow).unionAll(provActiveFlow).unionAll(provSessionFlow)
    //地市
    val cityStatDF = sqlContext.sql(
      s"""
         |select custid, prov, city, '-1' as district, service_option,
         |       upflow, downflow,(upflow + downflow) as totalflow, activeUsers, activeSessions,
         |       row_number() over(partition by custid order by upflow) as uprank,
         |       row_number() over(partition by custid order by downflow) as downrank,
         |       row_number() over(partition by custid order by activeUsers) as activerank,
         |       row_number() over(partition by custid order by activeSessions) as sessionrank
         |from
         |(
         |    select custid, prov, city, service_option,
         |           sum(upflow) as upflow, sum(downflow) as downflow,
         |           count(distinct mdn) as activeUsers, count(chargingid) as activeSessions
         |    from ${cdrMdnTable}
         |    group by custid, prov, city, service_option
         |) t
       """.stripMargin)
    val cityUpFlow = cityStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'CITY' as dim_type", "city as dim_obj", "'OUTFLOW' as meas_obj", "upflow as meas_value", "uprank as meas_rank")
    val cityDownFlow = cityStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'CITY' as dim_type", "city as dim_obj", "'INFLOW' as meas_obj", "downflow as meas_value", "downrank as meas_rank")
    val cityTotalFlow = cityStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'CITY' as dim_type", "city as dim_obj", "'TOTALFLOW' as meas_obj", "totalflow as meas_value", "downrank as meas_rank")
    val cityActiveFlow = cityStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'CITY' as dim_type", "city as dim_obj", "'ACTIVEUSERS' as meas_obj", "activeUsers as meas_value", "activerank as meas_rank")
    val citySessionFlow = cityStatDF.selectExpr("service_option", s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'CITY' as dim_type", "city as dim_obj", "'SESSIONS' as meas_obj", "activeSessions as meas_value", "sessionrank as meas_rank")

    val cityResultDF = cityUpFlow.unionAll(cityDownFlow).unionAll(cityTotalFlow).unionAll(cityActiveFlow).unionAll(citySessionFlow)


    val resultDF = bsResultDF.unionAll(cityResultDF).unionAll(provResultDF).filter("meas_value!=0")
    // 先临时写入hdfs
    resultDF.write.format("orc").mode(SaveMode.Overwrite).save(tmpCachePathPdsn + "/table_serviceOption")

    val df3g = sqlContext.read.format("orc").load(tmpCachePathPdsn + "/table_serviceOption")
      .filter("service_option='59'").selectExpr("summ_cycle", "custid", "city","prov", "district", "dim_type", "dim_obj", "meas_obj", "meas_value", "meas_rank")
      .withColumn("nettype", lit("EVDO"))

    val df2g = sqlContext.read.format("orc").load(tmpCachePathPdsn + "/table_serviceOption")
      .filter("service_option='33'").selectExpr("summ_cycle", "custid", "city","prov", "district", "dim_type", "dim_obj", "meas_obj", "meas_value", "meas_rank")
      .withColumn("nettype", lit("CDMA1X"))
    // 23g合并写入hdfs
    val outputResult = outputPath + dayPath
    df3g.unionAll(df2g).coalesce(100).write.format("orc").mode(SaveMode.Overwrite).save(outputResult)




    // ***********此处新增4G话单中取rat='102'的EHRPD，只到省份 ***********
    val EHRPDinputPath = sc.getConf.get("spark.app.EHRPDinputPath", "/user/iot_ete/data/cdr/transform/pgw/data")

    val EHRPD_cdrTempTable = "EHRPD_cdrTempTable"
    sqlContext.read.format("orc").load(EHRPDinputPath + partitionPath).filter("rattype='102'")
      .selectExpr("prov","city","enbid","l_datavolumefbcuplink as upflow","l_datavolumefbcdownlink as downflow",
        "substr(servedimeisv,1,8) as tac","mdn","chargingid")
      .registerTempTable(EHRPD_cdrTempTable)


    // 关联基本信息
    val EHRPD_mdnDF = sqlContext.sql(
      s"""
         |select nvl(u.custid,'未知') as custid, c.mdn, c.enbid, c.prov, '未知' as city,
         |       c.upflow, c.downflow, c.tac, c.chargingid
         |from ${EHRPD_cdrTempTable} c
         |left join ${userTable} u on(c.mdn = u.mdn)
       """.stripMargin)

    EHRPD_mdnDF.repartition(100).write.format("orc").mode(SaveMode.Overwrite).save(tmpCachePathEhrpd)
    val EHRPD_cdrMdnTable = "EHRPD_cdrMdnTable"
    sqlContext.read.format("orc").load(tmpCachePathEhrpd).registerTempTable(EHRPD_cdrMdnTable)

    // 省份
    val EHRPD_provStatDF = sqlContext.sql(
      s"""select custid, prov, '未知' as city, '-1' as district,
         |       upflow, downflow, (upflow + downflow) as totalflow,activeUsers, activeSessions,
         |       row_number() over(partition by custid order by upflow) as uprank,
         |       row_number() over(partition by custid order by downflow) as downrank,
         |       row_number() over(partition by custid order by activeUsers) as activerank,
         |       row_number() over(partition by custid order by activeSessions) as sessionrank
         |from
         |(
         |    select custid, prov,
         |           sum(upflow) as upflow, sum(downflow) as downflow,
         |           count(distinct mdn) as activeUsers, count(chargingid) as activeSessions
         |    from ${EHRPD_cdrMdnTable}
         |    group by custid, prov
         |) t
       """.stripMargin)
    val EHRPD_provUpFlow = EHRPD_provStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'OUTFLOW' as meas_obj", "upflow as meas_value", "uprank as meas_rank", "'EHRPD' as nettype")
    val EHRPD_provDownFlow = EHRPD_provStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid","city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'INFLOW' as meas_obj", "downflow as meas_value", "downrank as meas_rank", "'EHRPD' as nettype")
    val EHRPD_provTotalFlow = EHRPD_provStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid","city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'TOTALFLOW' as meas_obj", "totalflow as meas_value", "downrank as meas_rank", "'EHRPD' as nettype")
    val EHRPD_provActiveFlow = EHRPD_provStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid","city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'ACTIVEUSERS' as meas_obj", "activeUsers as meas_value", "activerank as meas_rank", "'EHRPD' as nettype")
    val EHRPD_provSessionFlow = EHRPD_provStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid","city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'SESSIONS' as meas_obj", "activeSessions as meas_value", "sessionrank as meas_rank", "'EHRPD' as nettype")

    val EHRPD_provResultDF = EHRPD_provUpFlow.unionAll(EHRPD_provDownFlow).unionAll(EHRPD_provTotalFlow).unionAll(EHRPD_provActiveFlow).unionAll(EHRPD_provSessionFlow)


    val EHRPD_resultDF = EHRPD_provResultDF.filter("meas_value!=0")
    // 将结果写入到hdfs
    val EHRPD_outputResult = outputPath_EHRPD + dayPath
    EHRPD_resultDF.coalesce(100).write.format("orc").mode(SaveMode.Overwrite).save(EHRPD_outputResult)

    // ****************************************************************




    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_3g_data_summ_d_$dd
         |(summ_cycle, cust_id, city, province, district, dim_type, dim_obj, meas_obj, meas_value, meas_rank, nettype)
         |values (?,?,?,?,?,?,?,?,?,?,?)
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)
    val result = sqlContext.read.format("orc").load(outputResult, EHRPD_outputResult).
      map(x=>(x.getString(0), x.getString(1), x.getString(2),x.getString(3),x.getString(4),
        x.getString(5), x.getString(6),x.getString(7), x.getDouble(8), x.getInt(9), x.getString(10))).collect()
    //x.getLong(8)  -> x.getDouble(8)
    var i = 0
    for(r<-result){
      val summ_cycle = r._1
      val cust_id = r._2
      val city = r._3
      val province = r._4
      val district = r._5
      val dim_type = r._6
      val dim_obj = r._7
      val meas_obj = r._8
      val meas_value = r._9
      val meas_rank = r._10
      val nettype = r._11

      pstmt.setString(1, summ_cycle)
      pstmt.setString(2, cust_id)
      pstmt.setString(3, city)
      pstmt.setString(4, province)
      pstmt.setString(5, district)
      pstmt.setString(6, dim_type)
      pstmt.setString(7, dim_obj)
      pstmt.setString(8, meas_obj)
      pstmt.setLong(9, meas_value.toLong)
      //pstmt.setLong(9, meas_value)
      pstmt.setInt(10, meas_rank)
      pstmt.setString(11, nettype)

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

    //CommonUtils.updateBreakTable("iot_3g_TermType", dd)
    CommonUtils.updateBreakTable("iot_3g_ActiveUser", dd)
    CommonUtils.updateBreakTable("iot_3g_FluxDay", dd)

  }
}
