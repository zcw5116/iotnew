package com.zyuc.stat.iot.mme

/**
  * Created by zhoucw on 17-8-2.
  */
import java.sql.DriverManager

/**
  *  通过JDBC的方式访问
  */
object SparkSQLThriftServerApp {

  def main(args: Array[String]) {

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://cdh-nn1:10000","spark","")
    val pstmt = conn.prepareStatement("select name, count(*) as cnt from test group by name")
    val rs = pstmt.executeQuery()
    while (rs.next()) {
      println("name:" + rs.getInt("name") +
        " , cnt:" + rs.getString("cnt") )

    }

    rs.close()
    pstmt.close()
    conn.close()


  }


}
