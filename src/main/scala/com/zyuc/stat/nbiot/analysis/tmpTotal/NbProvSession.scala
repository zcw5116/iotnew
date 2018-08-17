package com.zyuc.stat.nbiot.analysis.tmpTotal

import java.sql.{DriverManager, PreparedStatement}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-6-26.
  */
object NbProvSession {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20180626")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/cdr/transform/nb/data")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val statime = dataTime.substring(0,4) + "/" + dataTime.substring(4,6) + "/" + dataTime.substring(6,8)

    //val STA_TIME = new Date(statime)
    val oraclrTime =new java.util.Date(statime)

    val partitionPath = s"/d=$d/h=*/m5=*"

    val jdbcDriver = "oracle.jdbc.driver.OracleDriver"
    val jdbcUrl = "jdbc:oracle:thin:@100.66.124.129:1521:dbnms" // "jdbc:oracle:thin:@59.43.49.70:20006:dbnms"
    //plsql这样登          59.43.49.70:20006/dbnms
    val jdbcUser = "epcslview"
    val jdbcPassword = "epc_slview129"

    //sqlContext.read.format("orc").load(inputPath + partitionPath)
    sqlContext.read.format("orc").load(inputPath + partitionPath)
      .selectExpr("mdn","cast(chargingid as string) chargingid","prov","T100").registerTempTable("ProvSessiobTable")
    val sql =
      s"""
         |select 'NB_话单用户会话数量' as DATATYPE, '${statime}' as STA_TIME,
         |        prov as PRODTYPE, count(distinct mdn,chargingid) as USERNUM
         |from ProvSessiobTable
         |where T100 in("开始","全部")
         |group by prov
       """.stripMargin
    val result = sqlContext.sql(sql).rdd.collect()

    insertByJDBC()

    def insertByJDBC() = {
      val deleteSQL = s"delete from iotprodusernum where DATATYPE='NB_话单用户会话数量' and STA_TIME = to_date('${dataTime}','yyyymmdd')"
      var conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      var psmtdel: PreparedStatement = null
      psmtdel = conn.prepareStatement(deleteSQL)
      psmtdel.executeUpdate()
      conn.commit()
      psmtdel.close()

      val insertSQL = "insert into iotprodusernum(DATATYPE,STA_TIME,PRODTYPE,USERNUM) values (?,?,?,?)"

      val dbConn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      dbConn.setAutoCommit(false)

      val pstmt = dbConn.prepareStatement(insertSQL)
      var i = 0
      try {
        for (r <- result) {
          val DATATYPE = r(0).toString
          val STA_TIME = statime//.toLong
          val PRODTYPE = r(2).toString
          val USERNUM = Integer.parseInt(r(3).toString)

          pstmt.setString(1, DATATYPE)
          pstmt.setDate(2, new java.sql.Date(oraclrTime.getTime()))
          pstmt.setString(3, PRODTYPE)
          pstmt.setInt(4, USERNUM)

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
