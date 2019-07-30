package com.zyuc.stat.nbiot.analysis.day.AbnormalCard

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 19-7-30.
  *
  * 15070 物博会
  */
object NbMoveDayAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20190730")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val inputPathNB = sc.getConf.get("spark.app.inputPathNB", "/user/iot_ete/data/cdr/summ_d/nb/")
    val outputPathNB = sc.getConf.get("spark.app.outputPathNB","/user/iot/data/cdr/abnormalCard/summ_move/nb")
    val tableName = sc.getConf.get("spark.app.tableName","iot_ana_day_abnormal_user")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val dayid = dataTime.substring(0, 8)
    val cdrTempTable = "CDRTempTable"


    ////
    deviceETL(inputPathNB, outputPathNB, "NB",  tableName, tableName+"_move_nb")


    def deviceETL(inputPath : String, outputPath : String, net_type : String,
                   tableName : String, bpName : String) = {

      try{
        sqlContext.read.format("orc").load(inputPath + "dayid=" + dayid)
          .filter("own_provid='江苏' and own_lanid='无锡'")
          .filter("custid is not null and custid!=''")
          .selectExpr("custid", "custname", "own_provid", "own_lanid", "mdn", "eci", "apn")
          .coalesce(10)
          .registerTempTable(cdrTempTable)

        val table_move = "table_move"
        sqlContext.sql(
          s"""
             |select custid, custname, own_provid, own_lanid, mdn, count(distinct eci) as cnt
             |from ${cdrTempTable}
             |where apn='ctnb' or apn='psma.edrx0.ctnb' or apn='psmc.edrx0.ctnb' or apn='psmf.edrxc.ctnb'
             |group by custid, custname, own_provid, own_lanid, mdn
           """.stripMargin)
          .filter(("cnt>2")).registerTempTable(table_move)

        sqlContext.sql(
          s"""
             |select  b.custid, b.custname, b.own_provid, b.own_lanid, b.mdn,concat_ws(',',collect_set(eci)) as ecgi
             |from ${cdrTempTable} a
             |inner join ${table_move} b on(a.mdn=b.mdn)
             |group by b.custid, b.custname, b.own_provid, b.own_lanid, b.mdn
           """.stripMargin)
          .selectExpr("'DAY' as gather_cycle", s"'${dayid}' as gather_date",
            "'ABNORMAL_MOVE' as gather_type", "ecgi as dim_obj", "own_provid as regprovince", "own_lanid as regcity",
            s"'${net_type}' as net_type", "custid", "custname",  "mdn",
            "'-1' as udata", "'-1' as ddata", "'-1' as tdata",
            "'-1' as sendmsg", "'-1' as recemsg", "'-1' as tmsg", "'-1' as avgtdata", "'-1' as avgtmsg")
          .coalesce(20).write.format("orc").mode(SaveMode.Overwrite).save(outputPath)

        insertAbnormalByJDBC(outputPath, tableName, bpName)
      }catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }

    }



    def insertAbnormalByJDBC(outputPath : String, tablename : String, bpname : String) = {
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
        .map(x=>(x.getString(0), x.getString(1),
          x.getString(2),x.getString(3), x.getString(4), x.getString(5),
          x.getString(6),x.getString(7),x.getString(8),x.getString(9),
          x.getString(10),x.getString(11),x.getString(12),
          x.getString(13),x.getString(14),x.getString(15),x.getString(16),x.getString(17))).collect()

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
