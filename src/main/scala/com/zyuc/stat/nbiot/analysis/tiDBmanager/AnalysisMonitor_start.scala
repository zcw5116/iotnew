package com.zyuc.stat.nbiot.analysis.tiDBmanager

import java.sql.{DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

import com.zyuc.iot.utils.DbUtils

/**
  * Created by liuzk on 18-7-19.
  */
object AnalysisMonitor_start {
  def main(args: Array[String]): Unit = {

    val starttime = args(0)//201807191010
    val endtime = args(1)
    val timetype = args(2)//m5
    val appType = args(3)//NbCdrM5Analysis
    val appSubType = args(4)
    val appName = args(5)
    val inputtime = args(6)
    val runtimes = args(7)
    val status = args(8)

/*    val beginTime = new Date()
    val year = String.format("%tY", beginTime)
    val month = String.format("%tm", beginTime)
    val day = String.format("%td", beginTime)
    val hour = String.format("%tH", beginTime)
    val minute = String.format("%tM", beginTime)*/


    //val starttime = year + month + day + hour + minute//201807191010
    //val whatview = "m5"
    //val sparkAppName = "NbCdrM5Analysis"
    //val inputtime = starttime.toLong - 15
    //val time1 = 1

    val sql =
      s"""
         |insert into analysis_platform_monitor(starttime,endtime,timetype,appType,appSubType,appName,
         |inputtime,runtimes,status)
         |values(?,?,?,?,?,?,?,?,?)
         |on duplicate key update endtime=?
       """.stripMargin

    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val pstm = dbConn.prepareStatement(sql)
    // 执行插入数据操作
    pstm.setString(1, starttime)
    pstm.setString(2, endtime)
    pstm.setString(3, timetype)
    pstm.setString(4, appType)
    pstm.setString(5, appSubType)
    pstm.setString(6, appName)
    pstm.setString(7, inputtime)
    pstm.setInt(8, runtimes.toInt)
    pstm.setString(9, status)
    pstm.setString(10, endtime)
    pstm.executeUpdate

    dbConn.commit()
    pstm.close()
    dbConn.close()


  }

}
