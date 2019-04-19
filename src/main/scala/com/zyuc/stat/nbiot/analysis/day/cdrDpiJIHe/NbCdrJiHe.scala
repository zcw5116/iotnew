package com.zyuc.stat.nbiot.analysis.day.cdrDpiJIHe

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 19-4-16.
  *
  * 14418 nb话单/IOT-DPI信令和用户面稽查
  */
object NbCdrJiHe {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20190416")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/cdr/transform/nb/data/")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/summ_d/nb")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    //val dd = dataTime.substring(0, 8)
    val statime = dataTime.substring(0,4) + "/" + dataTime.substring(4,6) + "/" + dataTime.substring(6,8)
    val oraclrTime =new java.util.Date(statime)

    val jdbcDriver = "oracle.jdbc.driver.OracleDriver"
    val jdbcUrl = "jdbc:oracle:thin:@100.66.124.129:1521:dbnms"
    val jdbcUser = "epcslview"
    val jdbcPassword = "epc_slview129"


    val df = sqlContext.read.format("orc").load(inputPath + s"d=$d")
      .filter("p_gwaddress like '115.170.14.%' or p_gwaddress like '115.170.15.%'")//.cache()

    //信令面稽核 话单: 去重MDN数量 + 承载建立成功次数
    val tempTable_xl = "tempTable_xl"
    df.filter("accesspointnameni='ctnb'")
      .selectExpr("mdn","chargingid","p_gwaddress")
      .registerTempTable(tempTable_xl)

    val df1 = sqlContext.sql(
      s"""
         |select '$statime' as STA_TIME, 'NB话单C' as CHECKTYPE, 'PGW1' as PGW,
         |      count(distinct mdn) as USERNUM,
         |      count(distinct mdn,chargingid) as UPFLOW, '-1' as DOWNFLOW, '-1'as TOTALFLOW
         |from $tempTable_xl
         |where p_gwaddress like '115.170.14.%'
       """.stripMargin)

    val df2 = sqlContext.sql(
      s"""
         |select '$statime' as STA_TIME, 'NB话单C' as CHECKTYPE, 'PGW2' as PGW,
         |      count(distinct mdn) as USERNUM,
         |      count(distinct mdn,chargingid) as UPFLOW, '-1' as DOWNFLOW, '-1'as TOTALFLOW
         |from $tempTable_xl
         |where p_gwaddress like '115.170.15.%'
       """.stripMargin)




    //用户面稽核 话单：去重MDN数量 + 上行流量 + 下行流量 + 总流量
    val tempTable_yh = "tempTable_yh"
    df.filter("accesspointnameni='psma.edrx0.ctnb' and (l_datavolumefbcuplink>0 or l_datavolumefbcdownlink>0)")
      .selectExpr("mdn","p_gwaddress","l_datavolumefbcuplink as upflow","l_datavolumefbcdownlink as downflow")
      .registerTempTable(tempTable_yh)

    val df3 = sqlContext.sql(
      s"""
         |select '$statime' as STA_TIME, 'NB话单' as CHECKTYPE, 'PGW1' as PGW,
         |        count(distinct mdn) as USERNUM,
         |        sum(upflow) as UPFLOW, sum(downflow) as DOWNFLOW, sum(upflow+downflow) as TOTALFLOW
         |from $tempTable_yh
         |where p_gwaddress like '115.170.14.%'
       """.stripMargin)

    val df4 = sqlContext.sql(
      s"""
         |select '$statime' as STA_TIME, 'NB话单' as CHECKTYPE, 'PGW2' as PGW,
         |        count(distinct mdn) as USERNUM,
         |        sum(upflow) as UPFLOW, sum(downflow) as DOWNFLOW, sum(upflow+downflow) as TOTALFLOW
         |from $tempTable_yh
         |where p_gwaddress like '115.170.15.%'
       """.stripMargin)


    val result = df1.unionAll(df2).unionAll(df3).unionAll(df4).rdd.collect()

    insertByJDBC()

    def insertByJDBC() = {
//      val deleteSQL = s"delete from IOT_CHECKDPIANDCDR where CHECKTYPE like 'NB话单%' and STA_TIME = to_date('${dataTime}','yyyymmdd')"
//      var conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
//      var psmtdel: PreparedStatement = null
//      psmtdel = conn.prepareStatement(deleteSQL)
//      psmtdel.executeUpdate()
//      conn.commit()
//      psmtdel.close()

      val insertSQL = "insert into IOT_CHECKDPIANDCDR(STA_TIME,CHECKTYPE,PGW,USERNUM,UPFLOW,DOWNFLOW,TOTALFLOW) values (?,?,?,?,?,?,?)"

      val dbConn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      dbConn.setAutoCommit(false)

      val pstmt = dbConn.prepareStatement(insertSQL)
      var i = 0
      try {
        for (r <- result) {
          val STA_TIME = r(0).toString
          val CHECKTYPE = r(1).toString
          val PGW = r(2).toString
          val USERNUM = Integer.parseInt(r(3).toString)
          val UPFLOW = r(4).toString.toDouble
          val DOWNFLOW = r(5).toString.toDouble
          val TOTALFLOW = r(6).toString.toDouble

          pstmt.setDate(1, new java.sql.Date(oraclrTime.getTime()))
          pstmt.setString(2, CHECKTYPE)
          pstmt.setString(3, PGW)
          pstmt.setInt(4, USERNUM)
          pstmt.setDouble(5, UPFLOW)
          pstmt.setDouble(6, DOWNFLOW)
          pstmt.setDouble(7, TOTALFLOW)

          i += 1
          pstmt.addBatch()
          // 每1000条记录commit一次
          if (i % 1000 == 0) {
            pstmt.executeBatch
          }
        }

        pstmt.executeBatch
        dbConn.commit()
        pstmt.close()

      } catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }
    }

  }

}
