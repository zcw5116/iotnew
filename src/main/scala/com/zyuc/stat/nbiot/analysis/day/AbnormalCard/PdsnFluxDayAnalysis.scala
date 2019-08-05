package com.zyuc.stat.nbiot.analysis.day.AbnormalCard

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-12-14.
  * 异常卡：是用原子基表的：nb3点后 pgw2.30后 pdsn4.30后
  */
object PdsnFluxDayAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20181214")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot_ete/data/cdr/summ_d/pdsn/")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/abnormalCard/summ_d/pdsn")

    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    //val d = dataTime.substring(2, 8)
    val dayid = dataTime.substring(0, 8)

    val tableName = sc.getConf.get("spark.app.tableName","iot_ana_day_abnormal_user")

    val cdrTempTable = "CDRTempTable"
    sqlContext.read.format("orc").load(inputPath + "dayid=" + dayid)
      .filter("own_provid='江苏'  and (upflow!=0 or downflow!=0)")
      .filter("custid is not null and custid!=''")
      .selectExpr("custid", "custname", "own_provid", "own_lanid", "mdn","upflow","downflow")
      .coalesce(10)
      .write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + dayid + "/base")

    val baseTable = "baseTable"
    sqlContext.read.format("orc").load(outputPath + "/" + dayid + "/base")
      .registerTempTable(baseTable)


    val baseFlux = sqlContext.sql(
                      s"""
                         |select custid, custname, own_provid, own_lanid, avg(upflow) as avgUpflow, avg(downflow) as avgDownflow,
                         |       avg(upflow+downflow) as avgTotalFlow, count(distinct mdn) as cnt
                         |from ${baseTable}
                         |group by custid, custname, own_provid, own_lanid
                       """.stripMargin)

    val baseFluxDF = baseFlux.selectExpr("custid", "custname", "own_provid", "own_lanid", "'3G' as netType",
      "'日' as anaCycle", s"'${dayid}' as summ_cycle", "avgUpflow", "avgDownflow", "avgTotalFlow",
      "'-1' as upPacket", "'-1' as downPacket", "'-1' as totalPacket", "cnt")
    //基表保存到hdfs
    baseFluxDF.coalesce(10).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + dayid + "/baseFlux")


    //基表部分字段注册成临时表
    val baseFluxTable = "baseFluxTable"
    sqlContext.read.format("orc").load(outputPath + "/" + dayid + "/baseFlux").filter("cnt>100")
      .selectExpr("custid", "custname", "own_provid", "own_lanid", "avgTotalFlow").registerTempTable(baseFluxTable)

    val abnormalFlux = sqlContext.sql(
                        s"""
                           |select u.custid, u.custname, u.own_provid, u.own_lanid, mdn,
                           |       avgUpflow, avgDownflow, avgFlow, avgTotalFlow
                           |from
                           |( select custid, custname, own_provid, own_lanid, mdn,
                           |  avg(upflow) as avgUpflow, avg(downflow) as avgDownflow,avg(upflow+downflow) as avgFlow
                           |  from ${baseTable}
                           |  group by custid, custname, own_provid, own_lanid, mdn
                           |) u
                           |left join ${baseFluxTable} b on(u.custid=b.custid and u.own_lanid=b.own_lanid)
                         """.stripMargin).filter("avgFlow*100/avgTotalFlow>150")

    val abnormalFluxDF = abnormalFlux.selectExpr("'DAY' as gather_cycle", s"'${dayid}' as gather_date",
      "'ABNORMAL_FLUX' as gather_type", "'-1' as dim_obj", "own_provid as regprovince", "own_lanid as regcity",
      "'3G' as net_type", "custid", "custname",  "mdn",
      "avgUpflow as udata", "avgDownflow as ddata", "avgFlow as tdata",
      "'-1' as sendmsg", "'-1' as recemsg", "'-1' as tmsg", "avgTotalFlow as avgtdata", "'-1' as avgtmsg")
    //异常卡流量保存到hdfs
    abnormalFluxDF.coalesce(20).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + dayid + "/abnormalFlux")

    insert3GAbnormalByJDBC(outputPath + "/" + dayid + "/abnormalFlux", tableName, tableName+"_flux_3g")

    def insert3GAbnormalByJDBC(outputPath : String, tablename : String, bpname : String) = {
      // 将结果写入到tidb, 需要调整为upsert
      var dbConn = DbUtils.getDBConnection
      dbConn.setAutoCommit(false)
      val sql =
        s"""
           |insert into $tablename
           |(gather_cycle, gather_date, gather_type, dim_obj, regprovince, regcity, net_type, custid, custname, mdn,
           |udata, ddata, tdata, sendmsg, recemsg, tmsg, avgtdata, avgtmsg)
           |values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
       """.stripMargin

      val pstmt = dbConn.prepareStatement(sql)
      val result = sqlContext.read.format("orc").load(outputPath)
        .filter("length(regcity)>0")
        .map(x=>(x.getString(0), x.getString(1),
          x.getString(2),x.getString(3), x.getString(4), x.getString(5),
          x.getString(6),x.getString(7),x.getString(8),x.getString(9),
          x.getDouble(10),x.getDouble(11),x.getDouble(12),
          x.getString(13),x.getString(14),x.getString(15),x.getDouble(16),x.getString(17))).collect()

      var i = 0
      for(r<-result){
        val gather_cycle = r._1
        val gather_date = r._2
        val gather_type = r._3
        val dim_obj = r._4
        val regprovince = r._5
        val regcity = r._6
        val net_type = r._7
        val custid = r._8
        val custname = r._9
        val mdn = r._10
        val udata = r._11
        val ddata = r._12
        val tdata = r._13
        val sendmsg = r._14
        val recemsg = r._15
        val tmsg = r._16
        val avgtdata = r._17
        val avgtmsg =  r._18

        pstmt.setString(1, gather_cycle)
        pstmt.setString(2, gather_date)
        pstmt.setString(3, gather_type)
        pstmt.setString(4, dim_obj)
        pstmt.setString(5, regprovince)
        pstmt.setString(6, regcity)
        pstmt.setString(7, net_type)
        pstmt.setString(8, custid)
        pstmt.setString(9, custname)
        pstmt.setString(10, mdn)
        pstmt.setLong(11, udata.toLong)
        pstmt.setLong(12, ddata.toLong)
        pstmt.setLong(13, tdata.toLong)
        pstmt.setLong(14, sendmsg.toLong)
        pstmt.setLong(15, recemsg.toLong)
        pstmt.setLong(16, tmsg.toLong)
        pstmt.setLong(17, avgtdata.toLong)
        pstmt.setLong(18, avgtmsg.toLong)

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

      CommonUtils.updateBreakTable(bpname, dayid)
    }


  }
}
