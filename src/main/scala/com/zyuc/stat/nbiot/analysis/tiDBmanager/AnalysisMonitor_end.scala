package com.zyuc.stat.nbiot.analysis.tiDBmanager

import java.util.Date

import com.zyuc.iot.utils.DbUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-7-19.
  */
object AnalysisMonitor_end {
  def main(args: Array[String]): Unit = {

    val endtime = args(0)
    val status = args(1)
    val appType = args(2)
    val appSubType = args(3)
    val appName = args(4)

/*    val beginTime = new Date()
    val year = String.format("%tY", beginTime)
    val month = String.format("%tm", beginTime)
    val day = String.format("%td", beginTime)
    val hour = String.format("%tH", beginTime)
    val minute = String.format("%tM", beginTime)*/

    //val endtime = year + month + day + hour + minute//201807191010
    //val sparkAppName = "NbCdrM5Analysis"
    val time1 = 1
    //val Tidbendtime =new java.util.Date(endtime)

    val sql = "update analysis_platform_monitor set endtime=?, status=? where appType=? and appSubType=? and appName=?"
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val pstm = dbConn.prepareStatement(sql)
    // 执行插入数据操作
    pstm.setString(1, endtime)
    pstm.setString(2, status)
    pstm.setString(3, appType)
    pstm.setString(4, appSubType)
    pstm.setString(5, appName)

    pstm.executeUpdate

    dbConn.commit()
    pstm.close()
    dbConn.close()


  }

}
