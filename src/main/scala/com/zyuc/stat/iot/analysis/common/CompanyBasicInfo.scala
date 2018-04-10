package com.zyuc.stat.iot.analysis.common

import java.sql.{DriverManager, PreparedStatement}
import java.util.Date

import com.zyuc.stat.iot.analysis.common.CompanyScreenShow.logInfo
import com.zyuc.stat.iot.analysis.util.HbaseDataUtil
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.HbaseUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by zhoucw on 17-10-8.
  */
object CompanyBasicInfo2Oracle extends Logging{
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val userTablePartitionID = sc.getConf.get("spark.app.table.userTablePartitionDayID", "20170922")
    val companyAndDomain = sc.getConf.get("spark.app.table.companyAndDomain", "iot_basic_company_and_domain") //"iot_basic_company_and_domain"
    val companyNameFile = sc.getConf.get("spark.app.companyNameFile", "/hadoop/IOT/ANALY_PLATFORM/BasicData/BaseInfo/map_iot_company.yml")//
    val oraCompanyTable = "iot_analyze_company_basic_info"
    val appName = sc.getConf.get("spark.app.name")


    // 关联企业名称
    import sqlContext.implicits._
    val companyNameDF = sqlContext.read.format("text").load(companyNameFile).
      map(x=>x.getString(0).replaceAll(" ","").split(":", 2)).map(x=>(x(0).trim, x(1).trim)).
      toDF("companyname", "companycode")
    val companNameDimTable = "companNameDimTable"
    companyNameDF.registerTempTable(companNameDimTable)

    val companyTable = "companyTable_" + userTablePartitionID
    // 1 as flag
   val resultDF =  sqlContext.sql(
      s"""
         |select t.provincecode, t.provincename, t.companycode, d.companyname, t.vpdndomain
         |from
         |(
         |select provincecode, provincename, companycode, vpdndomain
         |from
         |(
         |    select provincecode, provincename, companycode, flag,
         |    (case when length(vpdndomain)=0 then null else vpdndomain end) as vpdndomain,
         |           row_number() over(partition by companycode) rn
         |    from ${companyAndDomain} where d='${userTablePartitionID}'
         |) c where c.rn=1
         |) t left join ${companNameDimTable} d
         |on(t.companycode=d.companycode)
       """.stripMargin)
    val results = resultDF.
      map(r=>(r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4))).
      collect()

    def insert2Ora() = {

      try{
        val driverUrl: String = "jdbc:oracle:thin:@100.66.124.129:1521/dbnms"
        val dbUser: String = "epcslview"
        val dbPasswd: String = "epc_slview129"

        val deleteSQL = s"delete from ${oraCompanyTable}')"
        logInfo("deleteSQL: " + deleteSQL)
        var conn = DriverManager.getConnection(driverUrl, dbUser, dbPasswd)
        var psmt: PreparedStatement = null
        psmt = conn.prepareStatement(deleteSQL)
        psmt.executeUpdate()
        psmt.close()

        val sql = s"INSERT INTO ${oraCompanyTable} (COMPANYCODE, COMPANYNAME, DOMAIN, PROVID, PROVFULLNAME) VALUES (?,?,?,?,?)"
        val dbConn = DriverManager.getConnection(driverUrl, dbUser, dbPasswd)
        dbConn.setAutoCommit(false)
        val pstmt = dbConn.prepareStatement(sql)
        var i = 0
        for(r<-results){
          pstmt.setString(1, r._3)
          pstmt.setString(2, r._4)
          pstmt.setString(3, r._5)
          pstmt.setString(4,r._1)
          pstmt.setString(5, r._2)
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
      }catch {
        case e:Exception => {
          e.printStackTrace()
        }
      }

    }


  }




}
