package com.zyuc.stat.iot.analysis.common

import com.zyuc.stat.iot.analysis.util.HbaseDataUtil
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.HbaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-10-16.
  */
object UserAnalysis extends Logging{
  val driverUrl: String = "jdbc:oracle:thin:@100.66.124.129:1521/dbnms"
  val dbUser: String = "epcslview"
  val dbPasswd: String = "epc_slview129"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("UserOnlineBaseData").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name","OnlineBase_2017100712") //
    val userInfoTable = sc.getConf.get("spark.app.table.userInfoTable", "iot_basic_userinfo")
    val userTableDataDayid = sc.getConf.get("spark.app.table.userTableDataDayid", "20170922")
    val hCompanyUsernumTable = sc.getConf.get("spark.app.table.hCompanyUsernumTable", "iot_stat_company_usernum")
    val statOracleTable = sc.getConf.get("spark.app.table.statOracleTable", "iot_stat_company_usernum")



    val hFamilies = new Array[String](1)
    hFamilies(0) = "u"
    // 创建表, 如果表存在， 自动忽略
    HbaseUtils.createIfNotExists(hCompanyUsernumTable,hFamilies)


    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val monthid = appName.substring(appName.lastIndexOf("_") + 1).substring(0, 6)

    ////////////////////////////////////////////////////////////////
    //   cache table
    ///////////////////////////////////////////////////////////////

    val statSQL =
      s"""
         |select  companycode, count(*) as totalSum,
         |sum(case when isdirect='1' then 1 else 0 end ) as directSum,
         |sum(case when isvpdn='1' then 1 else 0 end ) as vpdnSum
         |from ${userInfoTable} where d=${userTableDataDayid}
         |group by companycode
       """.stripMargin

    val userInfoTableCached = "userInfoTableCached"
    sqlContext.sql(s"cache table ${userInfoTableCached} as ${statSQL}")

    val resultSQL =
      s"""
         |select "${monthid}" as monthid, "-1" as companycode, sum(totalSum) as totalSum,  sum(directSum) as directSum, sum(vpdnSum) as vpdnSum,
         |       0 directRank, 0  vpdnRank
         |from ${userInfoTableCached}
         |union all
         |select "${monthid}" as monthid, companycode, totalSum, directSum, vpdnSum,
         |       row_number() over(order by directSum desc) directRank,
         |       row_number() over(order by vpdnSum desc) vpdnRank
         |from ${userInfoTableCached}
       """.stripMargin

    val resultDF = sqlContext.sql(resultSQL).coalesce(1)

    val resultRDD = resultDF.rdd.map(x=>{
      val m = x(0).toString
      val c = if(null == x(1)) "" else x(1).toString
      val tsum = x(2).toString
      val dsum = x(3).toString
      val vsum = x(4).toString
      val drank = String.format("%5s", x(5).toString).replaceAll(" ","0")
      val vrank = String.format("%5s", x(6).toString).replaceAll(" ","0")

      val rowkey = m + "_" + c

      val vpdnPut = new Put(Bytes.toBytes(rowkey))

/*      dirctPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("ccode"), Bytes.toBytes(c))
      dirctPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("tsum"), Bytes.toBytes(tsum))
      dirctPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("dsum"), Bytes.toBytes(dsum))
      dirctPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("vsum"), Bytes.toBytes(vsum))
      dirctPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("drank"), Bytes.toBytes(drank))
      dirctPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("vrank"), Bytes.toBytes(vrank))*/

      vpdnPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("ccode"), Bytes.toBytes(c))
      vpdnPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("tsum"), Bytes.toBytes(tsum))
      vpdnPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("dsum"), Bytes.toBytes(dsum))
      vpdnPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("vsum"), Bytes.toBytes(vsum))
      vpdnPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("drank"), Bytes.toBytes(drank))
      vpdnPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("vrank"), Bytes.toBytes(vrank))

      (new ImmutableBytesWritable, vpdnPut)
    })

    HbaseDataUtil.saveRddToHbase(hCompanyUsernumTable, resultRDD)
/*

    val deleteSQL = s"delete from ${statOracleTable} where monthid='${monthid}' "
    var conn = DriverManager.getConnection(driverUrl, dbUser, dbPasswd)
    var psmt: PreparedStatement = null
    psmt = conn.prepareStatement(deleteSQL)
    psmt.executeUpdate()
    psmt.close()

    resultDF.show(100)

    resultDF.foreachPartition(insertData)

    def insertData(iterator:Iterator[Row]) :Unit = {
      Class.forName("oracle.jdbc.driver.OracleDriver")
      var conn: Connection = null
      var psmt: PreparedStatement = null
      val sql = "INSERT INTO iot_stat_company_Usernum (monthid,companycode,totalnum,dnum,vnum,drank,vrank) VALUES (?,?,?,?,?,?,?)"
      var i = 0
      var num = 0
      val batchSize = 100
      try {
        logInfo("1#####################################################")
        conn = DriverManager.getConnection(driverUrl, dbUser, dbPasswd)
        logInfo("2#####################################################")
        conn.setAutoCommit(false);
        psmt = conn.prepareStatement(sql)
        iterator.foreach { row =>
        {
          i += 1
          if (i > batchSize) {
            i = 0
            psmt.executeBatch();
            num += psmt.getUpdateCount();
            psmt.clearBatch();
          }
          psmt.setObject(1, "test")
          psmt.setObject(2, row(1))
          psmt.setObject(3, row(2))
          psmt.setObject(4, row(3))
          psmt.setObject(5, row(4))
          psmt.setObject(6, row(5))
          psmt.setObject(7, row(6))
          psmt.addBatch();
        }
        }
        psmt.executeBatch();
        num += psmt.getUpdateCount();
        conn.commit();
        println(num+"..........................")
        logInfo("#####################################################" + num)
      } catch {
        case e: Exception => {
          e.printStackTrace()
          try {
            conn.rollback();
          } catch {
            case e: Exception => e.printStackTrace();
          }
        }
      } finally {
        if (psmt != null) {
          psmt.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    }*/

 /*   resultDF.foreachPartition(iterator=>{
      Class.forName("oracle.jdbc.driver.OracleDriver")
      var conn1:  Connection = null
      conn1 = DriverManager.getConnection(driverUrl, dbUser, dbPasswd)
      val sql = "INSERT INTO iot_stat_company_Usernum (monthid,companycode) VALUES (?,?)"
      var ps: PreparedStatement = null
      iterator.foreach(row=>{
        ps = conn1.prepareStatement(sql)
        ps.setString(1, row(0).toString)
        ps.setString(2, row(1).toString)
        ps.executeUpdate()
        ps.close()
      })
    })*/


  }



}
