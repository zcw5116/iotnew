package com.zyuc.stat.iot.analysis.common

import java.sql.{DriverManager, PreparedStatement}
import java.util.Date
import com.zyuc.stat.properties.ConfigProperties
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * task: 2017103101-物联网大屏展示-黄志慧-11253
  * desc: 统计定向企业数/VPDN企业数
  * @author zhoucw
  * @version 1.0
  */
object CompanyScreenShow extends Logging {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name", "OnlineBase_2017100712")
    val userInfoTable = sc.getConf.get("spark.app.table.userInfoTable", "iot_basic_userinfo")
    val userTableDataDayid = sc.getConf.get("spark.app.table.userTableDataDayid", "20170922")
    val statOracleTable = sc.getConf.get("spark.app.table.statOracleTable", "iotprodusernum")
    val statTime = appName.substring(appName.lastIndexOf("_") + 1)


    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    ////////////////////////////////////////////////////////////////
    //   按照业务统计全国的企业数据
    ///////////////////////////////////////////////////////////////

    val statSQL =
      s"""select '全国' prov, count(*) as companynum,
         |sum(case when directSum>0 then 1 else 0 end) dcompanynum,
         |sum(case when vpdnSum>0 then 1 else 0 end) ccompanynum
         |from
         |(
         |    select  companycode, count(*) as totalSum,
         |            sum(case when isdirect='1' then 1 else 0 end ) as directSum,
         |            sum(case when isvpdn='1' then 1 else 0 end ) as vpdnSum
         |    from ${userInfoTable} where d=${userTableDataDayid}
         |    group by companycode
         |) t
       """.stripMargin
    val resultDF = sqlContext.sql(statSQL)
    val companyServSet = new scala.collection.mutable.HashSet[Tuple3[String, String, Long]]
    val result = resultDF.map(x => (x.getString(0), x.getLong(1), x.getLong(2), x.getLong(3))).collect()

    result.foreach(x => {
      // cset+=((x._1, "tnum", x._2))
      companyServSet += ((x._1, "定向企业数", x._3))
      companyServSet += ((x._1, "VPDN企业数", x._4))
    })

    upsert2Oracle()

    def upsert2Oracle(): Unit = {
      val driverUrl: String = "jdbc:oracle:thin:@100.66.124.129:1521/dbnms"
      val dbUser: String = "epcslview"
      val dbPasswd: String = "epc_slview129"

      val statDF = FastDateFormat.getInstance("yyyyMMdd")
      val statDate: Date = statDF.parse(statTime)
      val deleteSQL = s"delete from ${statOracleTable} where DATATYPE in ('定向企业数','VPDN企业数') and PRODTYPE='全国' and sta_time = to_date('${statTime}','yyyy/mm/dd')"
      logInfo("deleteSQL: " + deleteSQL)
      var conn = DriverManager.getConnection(driverUrl, dbUser, dbPasswd)
      var psmt: PreparedStatement = null
      psmt = conn.prepareStatement(deleteSQL)
      psmt.executeUpdate()
      psmt.close()


      val sql = s"INSERT INTO ${statOracleTable} (PRODTYPE, STA_TIME, DATATYPE, USERNUM) VALUES (?,?,?,?)"
      val dbConn = DriverManager.getConnection(driverUrl, dbUser, dbPasswd)
      dbConn.setAutoCommit(false)
      val pstmt = dbConn.prepareStatement(sql)
      var i = 0
      try {
        for (s <- companyServSet) {
          val protype = s._1
          val serv = s._2
          val num = s._3

          pstmt.setString(1, protype)
          pstmt.setDate(2, new java.sql.Date(statDate.getTime))
          pstmt.setString(3, serv)
          pstmt.setLong(4, num)
          i += 1
          pstmt.addBatch()
          if (i % 1000 == 0) {
            pstmt.executeBatch
          }
        }
        pstmt.executeBatch

        dbConn.commit()
        pstmt.close()
        dbConn.close()

      } catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }
    }

  }
}
