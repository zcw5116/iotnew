package com.zyuc.stat.iot.interphone

import org.apache.spark.{SparkConf, SparkContext}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.downloadFileFromHdfs
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by zhoucw on 17-7-20.
  */
object InterPhoneCDR {

  def parseMDN(log: String) = {
    try {
      val p = log.split("\\|")
      val mdn = "86" + p(1).trim
      Row(mdn)
    } catch {
      case e: Exception => {
        Row('0')
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("UserOnlineBaseData").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)

    val dayid = sc.getConf.get("spark.app.dayid")
    val localDirLocation = sc.getConf.get("spark.app.localDirLocation")
    val hiveTable = "iot_cdr_3gaaa_ticket"

    // 读取mdn数据
    val interPhoneMDNDF = sqlContext.read.format("text").load("/hadoop/IOT/ANALY_PLATFORM/interphone/mdn/mdn.txt").coalesce(1)
    val mdnStruct = StructType(Array(StructField("mdn", StringType)))
    val mdnLogDF = sqlContext.createDataFrame(interPhoneMDNDF.map(x => parseMDN(x.getString(0))), mdnStruct)
    val mdntmptable = "mdntmptable"
    mdnLogDF.registerTempTable(mdntmptable)

    // 读取hive表数据
    val pdsnDF = sqlContext.sql(
      s"""select BSID, CELLID, MDN, Originating, Termination, acct_session_time as AcctSessionTime,
         |acct_input_packets as AcctInputPackets, acct_output_packets as AcctOutputPackets, active_time as ActiveTime
         |from ${hiveTable} where dayid=${dayid}  and acct_status_type = '2' and service_option='33'
       """.stripMargin).coalesce(5)
    val pdsntmptable = "pdsntmptable"
    pdsnDF.registerTempTable(pdsntmptable)

    val resultDF =
      sqlContext.sql(
        s""" select  BSID, CELLID, count(*) as access_cnt, count(distinct t.MDN) as user_cnt,
           |sum(AcctSessionTime) as accesstime_sum, sum(ActiveTime) as activetime_sum,
           |sum(AcctInputPackets) as uppack_sum, sum(AcctOutputPackets) as downpack_sum, sum(AcctInputPackets + AcctOutputPackets) as totalpact_sum,
           |sum(Originating) as upflow_sum, sum(Termination) as downflow_sum, sum(Originating + Termination ) as totalflow_sum
           | from ${mdntmptable} m , ${pdsntmptable} t  where t.mdn = m.mdn
           | group by BSID, CELLID
     """.stripMargin)
    //val resultDF =  sqlContext.sql(" select * from test")
    val hdfsDirLocation= "/hadoop/IOT/interPhone/"
    resultDF.coalesce(2).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).save(hdfsDirLocation)

    val suffix = "_" + dayid + ".csv"
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    downloadFileFromHdfs(fileSystem, hdfsDirLocation, localDirLocation, suffix)
    // resultDF.saveAsCsvFile("file:///tmp/csv/pdsn.csv")
  }
}
