package com.zyuc.stat.utils

import java.sql.{Connection, DriverManager}

/**
  * Created by zhoucw on 17-10-16.
  */
object DBUtils {

  val driverUrl: String = "jdbc:oracle:thin:@100.66.124.129:1521/dbnms"
  val dbUser: String = "epcslview"
  val dbPasswd: String = "epc_slview129"

  def getConnection():Connection = {
    var conn: Connection = null
    conn = DriverManager.getConnection(driverUrl, dbUser, dbPasswd)
    conn.setAutoCommit(false)
    conn
  }
}
