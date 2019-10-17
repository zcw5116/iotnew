package com.zyuc.stat.nbiot.analysis.day.bigCustomers

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 19-9-17.
  *
  * 13010 物联网与大客户网管数据接口
  */
object BigCustomers {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20190917")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPathNB = sc.getConf.get("spark.app.inputPathNB", "/user/iot_ete/data/cdr/summ_d/nb/")
    val inputPathPGW = sc.getConf.get("spark.app.inputPathPGW", "/user/iot_ete/data/cdr/summ_d/pgw/")
    val inputPathPDSN = sc.getConf.get("spark.app.inputPathPDSN", "/user/iot_ete/data/cdr/summ_d/pdsn/")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/tmp/BigCustomers/")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val dayid = dataTime.substring(0, 8)
    val yyyy = dataTime.substring(0, 4)
    val mm = dataTime.substring(4, 6)
    val dd = dataTime.substring(6, 8)

    // cache crm
    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath)//.filter("isnb='1'")
      .selectExpr("mdn", "custid","custname", "ind_type", "beloprov", "belocity", "isnb", "is4g", "is3g", "is2g")
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

    // NB
    val cdrTempTable = "nbTempTable"
    sqlContext.read.format("orc").load(inputPathNB + "dayid=" + dayid)
      .selectExpr("custname","custid","own_provid","own_lanid","upflow","downflow","mdn")
      .registerTempTable(cdrTempTable)

    sqlContext.sql(
      s"""
         |select a.custname custName, a.custid custCode, a.own_provid custProv, a.own_lanid custCity, 'NB' openType,
         |       openSum, inFlow, outFlow, activeNumber, '${yyyy}-${mm}-${dd}' statisticsDate
         |from
         |(select custname, custid, own_provid, own_lanid, sum(upflow) as inFlow, sum(downflow) as outFlow, count(distinct mdn) activeNumber
         |from ${cdrTempTable} group by custname, custid, own_provid, own_lanid) a
         |left join
         |(select custid, beloprov, belocity, count(*) as openSum from ${userTable} where isnb='1' group by custid, beloprov, belocity) b
         |on(a.custid=b.custid and a.own_provid=b.beloprov and a.own_lanid=b.belocity)
       """.stripMargin).repartition(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "NB_1")

    // 4G
    sqlContext.read.format("orc").load(inputPathPGW + "dayid=" + dayid).filter("rattype='6'")
      .selectExpr("custname","custid","own_provid","own_lanid","upflow","downflow","mdn")
      .registerTempTable(cdrTempTable)

    sqlContext.sql(
      s"""
         |select a.custname custName, a.custid custCode, a.own_provid custProv, a.own_lanid custCity, '4G' openType,
         |       openSum, inFlow, outFlow, activeNumber, '${yyyy}-${mm}-${dd}' statisticsDate
         |from
         |(select custname, custid, own_provid, own_lanid, sum(upflow) as inFlow, sum(downflow) as outFlow, count(distinct mdn) activeNumber
         |from ${cdrTempTable} group by custname, custid, own_provid, own_lanid) a
         |left join
         |(select custid, beloprov, belocity, count(*) as openSum from ${userTable} where is4g='Y' group by custid, beloprov, belocity) b
         |on(a.custid=b.custid and a.own_provid=b.beloprov and a.own_lanid=b.belocity)
       """.stripMargin).repartition(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "4G_1")

    // 3G
    sqlContext.read.format("orc").load(inputPathPGW + "dayid=" + dayid).filter("rattype='102'")
      .selectExpr("custname","custid","own_provid","own_lanid","upflow","downflow","mdn", "industry_level1")
      .unionAll(sqlContext.read.format("orc").load(inputPathPDSN + "dayid=" + dayid)
        .selectExpr("custname","custid","own_provid","own_lanid","upflow","downflow","mdn", "industry_level1"))
      .write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "3G_tmp")

    sqlContext.read.format("orc").load(outputPath + "3G_tmp").registerTempTable(cdrTempTable)

    sqlContext.sql(
      s"""
         |select a.custname custName, a.custid custCode, a.own_provid custProv, a.own_lanid custCity, '3G' openType,
         |       openSum, inFlow, outFlow, activeNumber, '${yyyy}-${mm}-${dd}' statisticsDate
         |from
         |(select custname, custid, own_provid, own_lanid, sum(upflow) as inFlow, sum(downflow) as outFlow, count(distinct mdn) activeNumber
         |from ${cdrTempTable} group by custname, custid, own_provid, own_lanid) a
         |left join
         |(select custid, beloprov, belocity, count(*) as openSum from ${userTable} where (is3g='Y' and is4g='N') or (is2g='Y' and is3g='Y' and is4g='Y')
         |group by custid, beloprov, belocity) b
         |on(a.custid=b.custid and a.own_provid=b.beloprov and a.own_lanid=b.belocity)
       """.stripMargin).repartition(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "3G_1")


    // NB2
    sqlContext.read.format("orc").load(outputPath + "NB_1")
      .unionAll(sqlContext.read.format("orc").load(outputPath + "4G_1"))
      .unionAll(sqlContext.read.format("orc").load(outputPath + "3G_1"))
      .registerTempTable(cdrTempTable)

    sqlContext.sql(
      s"""
         |select openType, sum(openSum) openSum, sum(activeNumber) activeSum, custProv area,
         |       sum(inFlow) inFlow, sum(outFlow) outFlow, statisticsDate
         |from ${cdrTempTable}
         |group by openType, custProv, statisticsDate
         |union all
         |select openType, sum(openSum) openSum, sum(activeNumber) activeSum, '全国' area,
         |       sum(inFlow) inFlow, sum(outFlow) outFlow, statisticsDate
         |from ${cdrTempTable}
         |group by openType, statisticsDate
       """.stripMargin).repartition(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "NB_2")

    // NB3
    sqlContext.read.format("orc").load(inputPathNB + "dayid=" + dayid)
      .selectExpr("industry_level1","upflow","downflow","mdn")
      .registerTempTable(cdrTempTable)

    sqlContext.sql(
      s"""
         |select a.industry_level1 industry, openSum, 'NB' openType,
         |       inFlow, outFlow, activeSum, '${yyyy}-${mm}-${dd}' statisticsDate
         |from
         |(select industry_level1, sum(upflow) as inFlow, sum(downflow) as outFlow, count(distinct mdn) activeSum
         |from ${cdrTempTable} group by industry_level1) a
         |left join
         |(select ind_type, count(*) as openSum from ${userTable} where isnb='1' group by ind_type) b
         |on(a.industry_level1=b.ind_type)
       """.stripMargin).repartition(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "NB_3")

    // 4G3
    sqlContext.read.format("orc").load(inputPathPGW + "dayid=" + dayid).filter("rattype='6'")
      .selectExpr("industry_level1","upflow","downflow","mdn")
      .registerTempTable(cdrTempTable)

    sqlContext.sql(
      s"""
         |select a.industry_level1 industry, openSum, '4G' openType,
         |       inFlow, outFlow, activeSum, '${yyyy}-${mm}-${dd}' statisticsDate
         |from
         |(select industry_level1, sum(upflow) as inFlow, sum(downflow) as outFlow, count(distinct mdn) activeSum
         |from ${cdrTempTable} group by industry_level1) a
         |left join
         |(select ind_type, count(*) as openSum from ${userTable} where is4g='Y' group by ind_type) b
         |on(a.industry_level1=b.ind_type)
       """.stripMargin).repartition(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "4G_3")

    // 3G3
    sqlContext.read.format("orc").load(outputPath + "3G_tmp")
      .selectExpr("industry_level1","upflow","downflow","mdn")
      .registerTempTable(cdrTempTable)

    sqlContext.sql(
      s"""
         |select a.industry_level1 industry, openSum, '3G' openType,
         |       inFlow, outFlow, activeSum, '${yyyy}-${mm}-${dd}' statisticsDate
         |from
         |(select industry_level1, sum(upflow) as inFlow, sum(downflow) as outFlow, count(distinct mdn) activeSum
         |from ${cdrTempTable} group by industry_level1) a
         |left join
         |(select ind_type, count(*) as openSum from ${userTable} where (is3g='Y' and is4g='N') or (is2g='Y' and is3g='Y' and is4g='Y')
         |group by ind_type) b
         |on(a.industry_level1=b.ind_type)
       """.stripMargin).repartition(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "3G_3")



    sqlContext.read.format("orc").load(outputPath + "NB_1", outputPath + "4G_1", outputPath + "3G_1")
      .coalesce(1).write.format("com.databricks.spark.csv").option("header","true").mode(SaveMode.Overwrite).save(outputPath + "result_1")

    sqlContext.read.format("orc").load(outputPath + "NB_2")
      .coalesce(1).write.format("com.databricks.spark.csv").option("header","true").mode(SaveMode.Overwrite).save(outputPath + "result_2")

    sqlContext.read.format("orc").load(outputPath + "NB_3", outputPath + "4G_3", outputPath + "3G_3")
      .coalesce(1).write.format("com.databricks.spark.csv").option("header","true").mode(SaveMode.Overwrite).save(outputPath + "result_3")

  }
}
