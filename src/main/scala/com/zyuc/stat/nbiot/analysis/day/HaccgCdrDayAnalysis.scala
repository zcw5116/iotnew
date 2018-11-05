package com.zyuc.stat.nbiot.analysis.day

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-10-29.
  */
object HaccgCdrDayAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20181029")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot_ete/data/cdr/transform/haccg/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/summ_d/haccg")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val dd = dataTime.substring(0, 8)
    val partitionPath = s"/d=$d/h=*/m5=*"
    val dayPath = s"/d=$d"

    val cdrTempTable = "CDRTempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath).filter("acct_status_type=2")
      .selectExpr("acce_province as prov","acce_region as city","sid as enbid","originating as upflow",
        "termination as downflow", "mdn","account_session_id as chargingid")
      .registerTempTable(cdrTempTable)

    import sqlContext.implicits._
    val terminalTable = "IOTTerminalTable"
    sc.textFile("/user/iot/data/basic/IOTTerminal/iotterminal.csv")
      .filter(_.length>3)
      .map(line=>line.replace("\"", "").split(",",5)).map(x=>(x(0),x(3)))
      .toDF("tac", "modelname").registerTempTable(terminalTable)

    //3g : haccg SID
    val haccgSIDPath = sc.getConf.get("spark.app.haccgSIDPath", "/user/iot/data/basic/pdsnSID/pdsnSID.txt")
    val haccgSIDTable = "haccgSIDTable"
    sc.textFile(haccgSIDPath).map(x=>x.split("\\t"))
      .map(x=>(x(0),x(1),x(2))).toDF("sid","provname","cityname").registerTempTable(haccgSIDTable)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("is3g='Y' and is4g='N'")
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
         |       c.upflow, c.downflow,  c.chargingid
         |from ${cdrTempTable} c
         |left join ${userTable} u on(c.mdn = u.mdn)
         |left join ${haccgSIDTable} b on(c.enbid = b.sid and c.prov=b.provname)
       """.stripMargin)
    val cdrMdnTable = "spark_cdrmdn"
    mdnDF.registerTempTable(cdrMdnTable)

    // 基站的信息
    val bsStatDF = sqlContext.sql(
      s"""select custid, enbid, prov, city, '-1' as district,
         |       upflow, downflow,(upflow + downflow) as totalflow, activeUsers, activeSessions,
         |       row_number() over(partition by custid order by upflow) as uprank,
         |       row_number() over(partition by custid order by downflow) as downrank,
         |       row_number() over(partition by custid order by activeUsers) as activerank,
         |       row_number() over(partition by custid order by activeSessions) as sessionrank
         |
         |from(
         |    select custid, enbid, prov, city,
         |           sum(upflow) as upflow, sum(downflow) as downflow,
         |           count(distinct mdn) as activeUsers, count(chargingid) as activeSessions
         |    from ${cdrMdnTable}
         |    group by custid, enbid, prov, city
         |) t
       """.stripMargin)
    val bsUpFlow = bsStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid","city","prov", "district","'BS' as dim_type", "enbid as dim_obj", "'OUTFLOW' as meas_obj", "upflow as meas_value", "uprank as meas_rank" )
    val bsDownFlow = bsStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "city","prov","district","'BS' as dim_type", "enbid as dim_obj", "'INFLOW' as meas_obj", "downflow as meas_value", "downrank as meas_rank" )
    val bsTotalFlow = bsStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "city","prov","district","'BS' as dim_type", "enbid as dim_obj", "'TOTALFLOW' as meas_obj", "totalflow as meas_value", "downrank as meas_rank" )
    val bsActiveFlow = bsStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "city","prov","district","'BS' as dim_type", "enbid as dim_obj", "'ACTIVEUSERS' as meas_obj", "activeUsers as meas_value", "activerank as meas_rank" )
    val bsSessionFlow = bsStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "city","prov","district","'BS' as dim_type", "enbid as dim_obj", "'SESSIONS' as meas_obj", "activeSessions as meas_value", "sessionrank as meas_rank" )

    val bsResultDF = bsUpFlow.unionAll(bsDownFlow).unionAll(bsTotalFlow).unionAll(bsActiveFlow).unionAll(bsSessionFlow)


    // 省份
    val provStatDF = sqlContext.sql(
      s"""select custid, prov, '-1' as city, '-1' as district,
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
         |    from ${cdrMdnTable}
         |    group by custid, prov
         |) t
       """.stripMargin)
    val provUpFlow = provStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'OUTFLOW' as meas_obj", "upflow as meas_value", "uprank as meas_rank" )
    val provDownFlow = provStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid","city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'INFLOW' as meas_obj", "downflow as meas_value", "downrank as meas_rank" )
    val provTotalFlow = provStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid","city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'TOTALFLOW' as meas_obj", "totalflow as meas_value", "downrank as meas_rank" )
    val provActiveFlow = provStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid","city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'ACTIVEUSERS' as meas_obj", "activeUsers as meas_value", "activerank as meas_rank" )
    val provSessionFlow = provStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid","city","prov", "district", "'PROV' as dim_type", "prov as dim_obj", "'SESSIONS' as meas_obj", "activeSessions as meas_value", "sessionrank as meas_rank" )

    val provResultDF = provUpFlow.unionAll(provDownFlow).unionAll(provTotalFlow).unionAll(provActiveFlow).unionAll(provSessionFlow)

    //地市
    val cityStatDF = sqlContext.sql(
      s"""
         |select custid, prov, city, '-1' as district,
         |       upflow, downflow,(upflow + downflow) as totalflow, activeUsers, activeSessions,
         |       row_number() over(partition by custid order by upflow) as uprank,
         |       row_number() over(partition by custid order by downflow) as downrank,
         |       row_number() over(partition by custid order by activeUsers) as activerank,
         |       row_number() over(partition by custid order by activeSessions) as sessionrank
         |from
         |(
         |    select custid, prov, city,
         |           sum(upflow) as upflow, sum(downflow) as downflow,
         |           count(distinct mdn) as activeUsers, count(chargingid) as activeSessions
         |    from ${cdrMdnTable}
         |    group by custid, prov, city
         |) t
       """.stripMargin)
    val cityUpFlow = cityStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'CITY' as dim_type", "city as dim_obj", "'OUTFLOW' as meas_obj", "upflow as meas_value", "uprank as meas_rank" )
    val cityDownFlow = cityStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'CITY' as dim_type", "city as dim_obj", "'INFLOW' as meas_obj", "downflow as meas_value", "downrank as meas_rank" )
    val cityTotalFlow = cityStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'CITY' as dim_type", "city as dim_obj", "'TOTALFLOW' as meas_obj", "totalflow as meas_value", "downrank as meas_rank" )
    val cityActiveFlow = cityStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'CITY' as dim_type", "city as dim_obj", "'ACTIVEUSERS' as meas_obj", "activeUsers as meas_value", "activerank as meas_rank" )
    val citySessionFlow = cityStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'CITY' as dim_type", "city as dim_obj", "'SESSIONS' as meas_obj", "activeSessions as meas_value", "sessionrank as meas_rank" )

    val cityResultDF = cityUpFlow.unionAll(cityDownFlow).unionAll(cityTotalFlow).unionAll(cityActiveFlow).unionAll(citySessionFlow)


    //tac
    /*val tacStatDF = sqlContext.sql(
      s"""select custid, modelname, '-1' as prov, '-1' as city, '-1' as district, cnt,
         |       row_number() over(partition by custid order by cnt) as modelrank
         |from
         |(
         |    select custid, modelname, tac,
         |           count(distinct mdn) as cnt
         |    from ${cdrMdnTable}
         |    group by custid,modelname, tac
         |) t
       """.stripMargin)
    val tacResultDF = tacStatDF.selectExpr(s"'${dataTime}' as summ_cycle", "custid", "city","prov", "district", "'TERMDETAIL' as dim_type", "modelname as dim_obj", "'-1' as meas_obj", "cnt as meas_value", "modelrank as meas_rank" )
*/

    val resultDF = bsResultDF.unionAll(cityResultDF).unionAll(provResultDF).filter("meas_value!=0")
    // 将结果写入到hdfs
    val outputResult = outputPath + dayPath
    resultDF.write.format("orc").mode(SaveMode.Overwrite).save(outputResult)

    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_3g_data_summ_d
         |(summ_cycle, cust_id, city, province, district, dim_type, dim_obj, meas_obj, meas_value, meas_rank)
         |values (?,?,?,?,?,?,?,?,?,?)
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)
    val result = sqlContext.read.format("orc").load(outputResult).
      map(x=>(x.getString(0), x.getString(1), x.getString(2),x.getString(3),x.getString(4),
        x.getString(5), x.getString(6),x.getString(7), x.getDouble(8), x.getInt(9))).collect()
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