package com.zyuc.stat.nbiot.analysis.realtime.utils

import com.zyuc.iot.utils.DbUtils

/**
  * Created by zhoucw on 18-5-14 上午12:12.
  */
object CommonUtils {

  def updateBreakTable(table:String, circle:String) = {
    var dbConn = DbUtils.getDBConnection
    val getSql = s"select curr_cycle, last_cycle from iot_ana_5min_breakpoint where tab_name='$table'"
    val preparedStatement = dbConn.prepareStatement(getSql)
    val rs = preparedStatement.executeQuery()
    var lastCycle = ""
    var curCycle = circle
    if(rs.next()){
      val currCycleRs = rs.getString(1)
      val lastCycleRs = if(null == rs.getString(2)) "" else rs.getString(2)
      if(circle<lastCycleRs){
        // 不更新
        lastCycle = lastCycleRs
        curCycle = currCycleRs
      }else{
        if(circle<currCycleRs){
            lastCycle = circle
            curCycle = currCycleRs
        }else{
          lastCycle = currCycleRs
        }
      }
    }else{
      lastCycle = ""
      curCycle = circle
    }

    val insertSql = "insert into iot_ana_5min_breakpoint(tab_name, curr_cycle, last_cycle ) values(?, ?, ?) on duplicate key update curr_cycle=?, last_cycle=?"
    val pstmt = dbConn.prepareStatement(insertSql)
    pstmt.setString(1, table)
    pstmt.setString(2, curCycle)
    pstmt.setString(3, lastCycle)
    pstmt.setString(4, curCycle)
    pstmt.setString(5, lastCycle)
    pstmt.addBatch()
    pstmt.executeBatch
    pstmt.close()

    dbConn.close()

  }

  def main(args: Array[String]): Unit = {
    updateBreakTable("iot_ana_5min_nb_cdr", "20180410")
  }
}
