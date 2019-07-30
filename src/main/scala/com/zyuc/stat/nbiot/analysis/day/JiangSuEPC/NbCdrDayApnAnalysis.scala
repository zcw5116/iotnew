package com.zyuc.stat.nbiot.analysis.day.JiangSuEPC

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 19-7-24.
  *
  * 15070 物博会
  */
object NbCdrDayApnAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20190724")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPathNB = sc.getConf.get("spark.app.inputPathNB", "/user/iot_ete/data/cdr/summ_d/nb/")
    val outputPathNB = sc.getConf.get("spark.app.outputPathNB","/user/iot/data/cdr/summ_d/jiangsuEPC/nb_apn/")

    val tableName = sc.getConf.get("spark.app.tableName","iot_ana_day_prov_stat")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val dayid = dataTime.substring(0, 8)


    val table_tmp = "table_tmp"

    jiangsuETL(inputPathNB, "NB", tableName, outputPathNB, tableName + "_nb_apn")


    def jiangsuETL(inputPath : String, net_type : String, tableName : String, outputPath : String, bpname : String) = {

      try{
        sqlContext.read.format("orc").load(inputPath + "dayid=" + dayid)
          .filter("own_provid='江苏'")
          .selectExpr("own_provid", "own_lanid", "mdn", "apn")
          .coalesce(10).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + dayid + "/tmp")

        sqlContext.read.format("orc").load(outputPath + dayid + "/tmp").registerTempTable(table_tmp)

        val df = sqlContext.sql(
          s"""
             |select own_provid, own_lanid, apn, count(distinct mdn) activeUsers
             |from
             |${table_tmp}
             |group by own_provid, own_lanid, apn
       """.stripMargin)

        df.selectExpr(s"${dayid} as gather_date", "own_provid as regprovince", "regexp_replace(own_lanid, '电信', '') as regcity",
          "'-1' as province", "'-1' as city", "apn as ind_type",
          s"'${net_type}' as net_type", "'APN_USERNUM' as gather_type", "activeUsers as gather_value")
          //.filter("regcity is not  null")
          .coalesce(10).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + dayid +"/data")

        //  结果集目录 : outputPath + dayid+"/data"
        insertJiangSuByJDBC(outputPath, tableName, bpname)

      }catch {
        case e: Exception => {
          e.printStackTrace()
          println(s"----------------${net_type} etl failed-----------------")
        }
      }

    }


    def insertJiangSuByJDBC(outputPath : String, tablename : String, bpname : String) = {
      // 将结果写入到tidb, 需要调整为upsert
      var dbConn = DbUtils.getDBConnection
      dbConn.setAutoCommit(false)
      val sql =
        s"""
           |insert into $tablename
           |(gather_date, regprovince, regcity, province, city, ind_type, net_type, gather_type, gather_value)
           |values (?,?,?,?,?,?,?,?,?)
           |on duplicate key update gather_value=?
       """.stripMargin

      val pstmt = dbConn.prepareStatement(sql)
      val result = sqlContext.read.format("orc").load(outputPath + dayid +"/data")
        .filter("regcity is not null")
        .map(x=>(x.getInt(0), x.getString(1), x.getString(2),x.getString(3), x.getString(4),
          x.getString(5),x.getString(6),x.getString(7),x.getLong(8))).collect()

      var i = 0
      for(r<-result){
        val gather_date = r._1
        val regprovince = r._2
        val regcity = r._3
        val province = r._4
        val city = r._5
        val ind_type = r._6
        val net_type = r._7
        val gather_type = r._8
        val gather_value = r._9

        pstmt.setString(1, gather_date.toString)
        pstmt.setString(2, regprovince)
        pstmt.setString(3, regcity.toString)
        pstmt.setString(4, province)
        pstmt.setString(5, city)
        pstmt.setString(6, ind_type)
        pstmt.setString(7, net_type)
        pstmt.setString(8, gather_type)
        pstmt.setLong(9, gather_value.toLong)
        pstmt.setLong(10, gather_value.toLong)

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
