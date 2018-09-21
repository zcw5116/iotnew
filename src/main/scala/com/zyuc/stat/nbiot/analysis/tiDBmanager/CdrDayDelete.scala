package com.zyuc.stat.nbiot.analysis.tiDBmanager

import java.sql.PreparedStatement

import com.zyuc.iot.utils.DbUtils

/**
  * Created by liuzk on 18-7-9.
  */
object CdrDayDelete {
  def main(args: Array[String]): Unit = {
/*    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")*/
    val nbDeleteTime = args(0)
    val pgwDeleteTime = args(1)

    val deleteNbM5sql = s"delete from iot_ana_nb_data_summ_d where summ_cycle = '${nbDeleteTime}' limit 150000"
    val delete4gM5sql = s"delete from iot_ana_4g_data_summ_d where summ_cycle = '${pgwDeleteTime}' limit 150000"
    val countSql_nb = s"select count(*) from iot_ana_nb_data_summ_d where summ_cycle = '${nbDeleteTime}'"
    val countSql_4g = s"select count(*) from iot_ana_4g_data_summ_d where summ_cycle = '${pgwDeleteTime}'"

    var dbConn = DbUtils.getDBConnection

    //delete nb
    val preparedStatement_nb = dbConn.prepareStatement(countSql_nb)
    val rs_nb = preparedStatement_nb.executeQuery()
    if(rs_nb.next()){
      val deleteTimes = (rs_nb.getInt(1))/150000 + 1

      var pstmtnb: PreparedStatement = null
      pstmtnb = dbConn.prepareStatement(deleteNbM5sql)
      for(i<-1 to deleteTimes){
        pstmtnb.executeUpdate()
      }
      pstmtnb.close()
    }
    println("-------------------------nbDay--deleted")

    //delete 4g
    val preparedStatement = dbConn.prepareStatement(countSql_4g)
    val rs = preparedStatement.executeQuery()
    if(rs.next()){
      val deletetimes = (rs.getInt(1))/150000 + 1

      var pstmt4g: PreparedStatement = null
      pstmt4g = dbConn.prepareStatement(delete4gM5sql)
      for(i<-1 to deletetimes){
        pstmt4g.executeUpdate()
      }
      pstmt4g.close()
    }
    println("-------------------------4gDay--deleted")

    dbConn.close()
  }
}
