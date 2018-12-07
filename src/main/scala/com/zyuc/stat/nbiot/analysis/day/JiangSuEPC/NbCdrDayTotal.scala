package com.zyuc.stat.nbiot.analysis.day.JiangSuEPC

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-12-5.
  */
object NbCdrDayTotal {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20180504")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/cdr/transform/nb/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/summ_d/jiangsuEPC/nb/")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val dd = dataTime.substring(0, 8)
    val partitionPath = s"/d=$d/h=*/m5=*"
    val dayPath = s"/d=$d"

    val cdrTempTable = "CDRTempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath)
      .selectExpr("prov","l_datavolumefbcuplink as upflow","l_datavolumefbcdownlink as downflow", "mdn")
      //.filter("prov='江苏'")
      .registerTempTable(cdrTempTable)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("isnb='1'").selectExpr("mdn","beloprov")
    val tmpUserTable = "spark_tmpUser"
    userDF.registerTempTable(tmpUserTable)
    val userTable = "spark_User"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn, beloprov
         |from ${tmpUserTable}
       """.stripMargin)

    val baseTable = "baseTable"
    sqlContext.sql(
      s"""
         |select prov, upflow, downflow, c.mdn, beloprov
         |from ${cdrTempTable} c left join ${userTable} u
         |on(c.mdn=u.mdn)
       """.stripMargin).repartition(20).write.format("orc").mode(SaveMode.Overwrite).save(outputPath+d+"/tmpJoinTable")
    sqlContext.read.format("orc").load(outputPath + d+"/tmpJoinTable").registerTempTable(baseTable)
    //本省
    val localDF = sqlContext.sql(
                    s"""
                       |select prov,beloprov,count(distinct mdn) as activeUsers,
                       |       sum(upflow) as upflows, sum(downflow) as downflows, sum(upflow+downflow) as totalflows
                       |from ${baseTable}
                       |where prov='江苏' and beloprov='江苏'
                       |group by prov,beloprov
                     """.stripMargin)

    val localActiveUsers = localDF.selectExpr(s"${dd} as gather_date","beloprov as regprovince","prov as province",
                                  "'NB' as net_type","'USER_LOCAL' as gather_type","activeUsers as gather_value")
    val local_flux = localDF.selectExpr(s"${dd} as gather_date","beloprov as regprovince","prov as province",
                                  "'NB' as net_type","'FLUX_LOCAL' as gather_type","totalflows as gather_value")
    val local_flux_u = localDF.selectExpr(s"${dd} as gather_date","beloprov as regprovince","prov as province",
                                  "'NB' as net_type","'FLUX_LOCAL_U' as gather_type","upflows as gather_value")
    val local_flux_d = localDF.selectExpr(s"${dd} as gather_date","beloprov as regprovince","prov as province",
                                  "'NB' as net_type","'FLUX_LOCAL_D' as gather_type","downflows as gather_value")
    //漫出
    val roamoutDF = sqlContext.sql(
                      s"""
                         |select prov,beloprov,count(distinct mdn) as activeUsers,
                         |       sum(upflow) as upflows, sum(downflow) as downflows, sum(upflow+downflow) as totalflows
                         |from ${baseTable}
                         |where prov!='江苏' and beloprov='江苏'
                         |group by prov,beloprov
                      """.stripMargin)

    val roamoutActiveUsers = roamoutDF.selectExpr(s"${dd} as gather_date","beloprov as regprovince","prov as province",
      "'NB' as net_type","'USER_ROAMOUT' as gather_type","activeUsers as gather_value")

    val roamout_flux = roamoutDF.selectExpr(s"${dd} as gather_date","beloprov as regprovince","prov as province",
      "'NB' as net_type","'FLUX_ROAMOUT' as gather_type","totalflows as gather_value")

    val roamout_flux_u = roamoutDF.selectExpr(s"${dd} as gather_date","beloprov as regprovince","prov as province",
      "'NB' as net_type","'FLUX_ROAMOUT_U' as gather_type","upflows as gather_value")

    val roamout_flux_d = roamoutDF.selectExpr(s"${dd} as gather_date","beloprov as regprovince","prov as province",
      "'NB' as net_type","'FLUX_ROAMOUT_D' as gather_type","downflows as gather_value")

    //漫入
    val roaminDF = sqlContext.sql(
      s"""
         |select prov,beloprov,count(distinct mdn) as activeUsers,
         |       sum(upflow) as upflows, sum(downflow) as downflows, sum(upflow+downflow) as totalflows
         |from ${baseTable}
         |where prov='江苏' and beloprov!='江苏'
         |group by prov,beloprov
                      """.stripMargin)

    val roaminActiveUsers = roaminDF.selectExpr(s"${dd} as gather_date","beloprov as regprovince","prov as province",
      "'NB' as net_type","'USER_ROAMIN' as gather_type","activeUsers as gather_value")

    val roamin_flux = roaminDF.selectExpr(s"${dd} as gather_date","beloprov as regprovince","prov as province",
      "'NB' as net_type","'FLUX_ROAMIN' as gather_type","totalflows as gather_value")

    val roamin_flux_u = roaminDF.selectExpr(s"${dd} as gather_date","beloprov as regprovince","prov as province",
      "'NB' as net_type","'FLUX_ROAMIN_U' as gather_type","upflows as gather_value")

    val roamin_flux_d = roaminDF.selectExpr(s"${dd} as gather_date","beloprov as regprovince","prov as province",
      "'NB' as net_type","'FLUX_ROAMIN_D' as gather_type","downflows as gather_value")

    //    结果保存hdfs
    val resultDF = localActiveUsers.unionAll(local_flux).unionAll(local_flux_u).unionAll(local_flux_d)
      .unionAll(roamoutActiveUsers).unionAll(roamout_flux).unionAll(roamout_flux_u).unionAll(roamout_flux_d)
      .unionAll(roaminActiveUsers).unionAll(roamin_flux).unionAll(roamin_flux_u).unionAll(roamin_flux_d)
    resultDF.coalesce(10).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + d+"/data")


    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_day_prov_stat
         |(gather_date, regprovince, province, net_type, gather_type, gather_value)
         |values (?,?,?,?,?,?)
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)
    val result = sqlContext.read.format("orc").load(outputPath + d+"/data").
      map(x=>(x.getInt(0), x.getString(1), x.getString(2),x.getString(3),x.getString(4),x.getDouble(5))).collect()

    var i = 0
    for(r<-result){
      val gather_date = r._1
      val regprovince = r._2
      val province = r._3
      val net_type = r._4
      val gather_type = r._5
      val gather_value = r._6

      pstmt.setString(1, gather_date.toString)
      pstmt.setString(2, regprovince)
      pstmt.setString(3, province)
      pstmt.setString(4, net_type)
      pstmt.setString(5, gather_type)
      pstmt.setLong(6, gather_value.toLong)

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

    CommonUtils.updateBreakTable("iot_ana_day_prov_stat_nb", dd)


  }
}
