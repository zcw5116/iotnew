package com.zyuc.stat.iotNBLiuzk.analysis.day

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-5-10.
  */
object NbCdrDayAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("name_20180504")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/epciot/data/cdr/transform/nb/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/epciot/data/mme/summ_d/nb")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/epciot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val dd = dataTime.substring(0, 8)
    val partitionPath = s"/d=$d/h=*/m5=*"
    val dayPath = s"/d=$d"

    val cdrTempTable = "CDRTempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath)
      .selectExpr("prov","city","t804 as enbid","l_datavolumefbcuplink as upflow","l_datavolumefbcdownlink as downflow",
        "substr(servedimeisv,1,8) as tac")
      .registerTempTable(cdrTempTable)

    import sqlContext.implicits._
    val terminalTable = "IOTTerminalTable"
    sc.textFile("/user/epciot/data/basic/IOTTerminal/iotterminal.csv")
       .map(line=>line.replace("\"", "").split(",",5)).map(x=>(x(0),x(3)))
      .toDF("tac", "modelname").registerTempTable(terminalTable)

    // 基站
    val iotBSInfoPath = sc.getConf.get("spark.app.IotBSInfoPath", "/user/epciot/data/basic/IotBSInfo/data/")
    val bsInfoTable = "IOTBSInfoTable"
    sqlContext.read.format("orc").load(iotBSInfoPath).registerTempTable(bsInfoTable)

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

    // 关联基本信息
    val mdnDF = sqlContext.sql(
      s"""
         |select u.custid, c.mdn, t.modelname, c.enbid, b.provname as prov, b.cityname as city,
         |       c.upflow, c.downflow, c.tac
         |from ${cdrTempTable} c
         |inner join ${userTable} u on(c.mdn = u.mdn)
         |left join ${bsInfoTable} b on(c.enbid = b.enbid)
         |left join ${terminalTable} t on(c.tac = t.tac)
       """.stripMargin)
    val cdrMdnTable = "spark_cdrmdn"
    mdnDF.registerTempTable(cdrMdnTable)

    // 基站的信息
    val bsStatDF = sqlContext.sql(
      s"""select custid, enbid, upflow, downflow, activeUsers, activeSessions
         |       row_number() over(partition by custid order by upflow) as uprank,
         |       row_number() over(partition by custid order by downflow) as downrank,
         |       row_number() over(partition by custid order by activeUsers) as activerank,
         |       row_number() over(partition by custid order by activeSessions) as sessionrank
         |
         |from(
         |    select custid, enbid,
         |           sum(upflow) as upflow, sum(downflow) as downflow,
         |           count(distinct mdn) as activeUsers, count(chargingid) as activeSessions
         |    from ${cdrMdnTable}
         |    group by custid, enbid
         |) t
       """.stripMargin)
    val bsUpFlow = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'BS' as dim_type", "enbid as dim_obj", "'INFLOW' as meas_obj", "upflow as meas_value", "uprank as meas_rank" )
    val bsDownFlow = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'BS' as dim_type", "enbid as dim_obj", "'OUTFLOW' as meas_obj", "downflow as meas_value", "downrank as meas_rank" )
    val bsActiveFlow = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'BS' as dim_type", "enbid as dim_obj", "'ACTIVEUSERS' as meas_obj", "activeUsers as meas_value", "activerank as meas_rank" )
    val bsSessionFlow = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'BS' as dim_type", "enbid as dim_obj", "'SESSIONS' as meas_obj", "activeSessions as meas_value", "sessionrank as meas_rank" )

    val bsResultDF = bsUpFlow.unionAll(bsDownFlow).unionAll(bsActiveFlow).unionAll(bsSessionFlow)


    // 省份
    val provStatDF = sqlContext.sql(
      s"""select custid, prov, upflow, downflow, activeUsers, activeSessions
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
    val provUpFlow = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'PROV' as dim_type", "prov as dim_obj", "'INFLOW' as meas_obj", "upflow as meas_value", "uprank as meas_rank" )
    val provDownFlow = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'PROV' as dim_type", "prov as dim_obj", "'OUTFLOW' as meas_obj", "downflow as meas_value", "downrank as meas_rank" )
    val provActiveFlow = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'PROV' as dim_type", "prov as dim_obj", "'ACTIVEUSERS' as meas_obj", "activeUsers as meas_value", "activerank as meas_rank" )
    val provSessionFlow = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'PROV' as dim_type", "prov as dim_obj", "'SESSIONS' as meas_obj", "activeSessions as meas_value", "sessionrank as meas_rank" )

    val provResultDF = provUpFlow.unionAll(provDownFlow).unionAll(provActiveFlow).unionAll(provSessionFlow)

    //地市
    val cityStatDF = sqlContext.sql(
      s"""
         |select custid, city, upflow, downflow, activeUsers, activeSessions
         |       row_number() over(partition by custid order by upflow) as uprank,
         |       row_number() over(partition by custid order by downflow) as downrank,
         |       row_number() over(partition by custid order by activeUsers) as activerank,
         |       row_number() over(partition by custid order by activeSessions) as sessionrank
         |from
         |(
         |    select custid, city
         |           sum(upflow) as upflow, sum(downflow) as downflow,
         |           count(distinct mdn) as activeUsers, count(chargingid) as activeSessions
         |    from ${cdrMdnTable}
         |    group by custid, city
         |) t
       """.stripMargin)
    val cityUpFlow = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'CITY' as dim_type", "city as dim_obj", "'INFLOW' as meas_obj", "upflow as meas_value", "uprank as meas_rank" )
    val cityDownFlow = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'CITY' as dim_type", "city as dim_obj", "'OUTFLOW' as meas_obj", "downflow as meas_value", "downrank as meas_rank" )
    val cityActiveFlow = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'CITY' as dim_type", "city as dim_obj", "'ACTIVEUSERS' as meas_obj", "activeUsers as meas_value", "activerank as meas_rank" )
    val citySessionFlow = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'CITY' as dim_type", "city as dim_obj", "'SESSIONS' as meas_obj", "activeSessions as meas_value", "sessionrank as meas_rank" )

    val cityResultDF = cityUpFlow.unionAll(cityDownFlow).unionAll(cityActiveFlow).unionAll(citySessionFlow)


    //tac
    val tacStatDF = sqlContext.sql(
      s"""select custid, modelname, cnt,
         |       row_number() over(partition by custid order by cnt) as modelrank
         |from
         |(
         |    select custid, modelname
         |           count(distinct mdn) as cnt
         |    from ${cdrMdnTable}
         |    group by custid, tac
         |) t
       """.stripMargin)
    val tacResultDF = bsStatDF.selectExpr(s"${dataTime} as summ_cycle", "custid", "'TERMDETAIL' as dim_type", "modelname as dim_obj", "'-1' as meas_obj", "cnt as meas_value", "modelrank as meas_rank" )


    val resultDF = bsResultDF.unionAll(cityResultDF).unionAll(provResultDF).unionAll(tacResultDF)

    resultDF.write.format("orc").mode(SaveMode.Overwrite).save(outputPath + dayPath)

  }
}
