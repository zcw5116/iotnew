package com.zyuc.stat.nbiot.analysis.tiDBmanager

import java.sql.PreparedStatement

import com.zyuc.iot.utils.DbUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-7-9.
  */
object CdrM5Delete {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val nbDeleteTime = sc.getConf.get("spark.app.nbDeleteTime")
    val pgwDeleteTime = sc.getConf.get("spark.app.pgwDeleteTime")

    val deleteNbM5sql = s"delete from iot_ana_5min_nb_cdr where gather_cycle < '${nbDeleteTime}00' limit 10000"
    val delete4gM5sql = s"delete from iot_ana_5min_4g_cdr where gather_cycle < '${pgwDeleteTime}00' limit 10000"
    val countSql_nb = s"select count(*) from iot_ana_5min_nb_cdr where gather_cycle < '${nbDeleteTime}00'"
    val countSql_4g = s"select count(*) from iot_ana_5min_4g_cdr where gather_cycle < '${pgwDeleteTime}00'"

    var dbConn = DbUtils.getDBConnection

    //delete nb
    val preparedStatement_nb = dbConn.prepareStatement(countSql_nb)
    val rs_nb = preparedStatement_nb.executeQuery()
    if(rs_nb.next()){
      val deleteTimes = (rs_nb.getInt(1))/10000 + 1

      var pstmtnb: PreparedStatement = null
      pstmtnb = dbConn.prepareStatement(deleteNbM5sql)
      for(i<-1 to deleteTimes){
        pstmtnb.executeUpdate()
      }
      pstmtnb.close()
    }

    //delete 4g
    val preparedStatement = dbConn.prepareStatement(countSql_4g)
    val rs = preparedStatement.executeQuery()
    if(rs.next()){
      val deletetimes = (rs.getInt(1))/10000 + 1

      var pstmt4g: PreparedStatement = null
      pstmt4g = dbConn.prepareStatement(delete4gM5sql)
      for(i<-1 to deletetimes){
        pstmt4g.executeUpdate()
      }
      pstmt4g.close()
    }


    dbConn.close()
  }
}
