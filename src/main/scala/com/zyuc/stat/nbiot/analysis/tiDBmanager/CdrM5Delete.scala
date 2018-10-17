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
/*    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")*/
    val nbDeleteTime = args(0)
    val pgwDeleteTime = args(1)
    val online3gApnProvDeleteTime = args(2)
    val online4gApnProvDeleteTime = args(3)

    val deleteNbM5sql = s"delete from iot_ana_5min_nb_cdr where gather_cycle = '${nbDeleteTime}00' limit 5000"
    val delete4gM5sql = s"delete from iot_ana_5min_4g_cdr where gather_cycle = '${pgwDeleteTime}00' limit 5000"
    val delete3gApnProvM5sql = s"delete from iot_ana_5min_prov_3g_stat where gather_cycle = '${online3gApnProvDeleteTime}00' limit 5000"
    val delete4gApnProvM5sql = s"delete from iot_ana_5min_prov_4g_stat where gather_cycle = '${online4gApnProvDeleteTime}00' limit 5000"

    val countSql_nb = s"select count(*) from iot_ana_5min_nb_cdr where gather_cycle = '${nbDeleteTime}00'"
    val countSql_4g = s"select count(*) from iot_ana_5min_4g_cdr where gather_cycle = '${pgwDeleteTime}00'"
    val countSql_3gApnProv = s"select count(*) from iot_ana_5min_prov_3g_stat where gather_cycle = '${online3gApnProvDeleteTime}00'"
    val countSql_4gApnProv = s"select count(*) from iot_ana_5min_prov_4g_stat where gather_cycle = '${online4gApnProvDeleteTime}00'"

    var dbConn = DbUtils.getDBConnection

    //delete nb
    val preparedStatement_nb = dbConn.prepareStatement(countSql_nb)
    val rs_nb = preparedStatement_nb.executeQuery()
    if(rs_nb.next()){
      val deleteTimes = (rs_nb.getInt(1))/5000 + 1

      var pstmtnb: PreparedStatement = null
      pstmtnb = dbConn.prepareStatement(deleteNbM5sql)
      for(i<-1 to deleteTimes){
        pstmtnb.executeUpdate()
      }
      pstmtnb.close()
    }
    println("-------------------------nbM5--deleted")

    //delete 4g
    val preparedStatement = dbConn.prepareStatement(countSql_4g)
    val rs = preparedStatement.executeQuery()
    if(rs.next()){
      val deletetimes = (rs.getInt(1))/5000 + 1

      var pstmt4g: PreparedStatement = null
      pstmt4g = dbConn.prepareStatement(delete4gM5sql)
      for(i<-1 to deletetimes){
        pstmt4g.executeUpdate()
      }
      pstmt4g.close()
    }
    println("-------------------------n4gM5--deleted")

    //delete 3gApnProv
    val preparedStatement1 = dbConn.prepareStatement(countSql_3gApnProv)
    val rs1 = preparedStatement1.executeQuery()
    if(rs1.next()){
      val deletetimes = (rs1.getInt(1))/5000 + 1

      var pstmt4g: PreparedStatement = null
      pstmt4g = dbConn.prepareStatement(delete3gApnProvM5sql)
      for(i<-1 to deletetimes){
        pstmt4g.executeUpdate()
      }
      pstmt4g.close()
    }
    println("-------------------------3gApnProv--deleted")

    //delete 4gApnProv
    val preparedStatement2 = dbConn.prepareStatement(countSql_4gApnProv)
    val rs2 = preparedStatement2.executeQuery()
    if(rs2.next()){
      val deletetimes = (rs2.getInt(1))/5000 + 1

      var pstmt4g: PreparedStatement = null
      pstmt4g = dbConn.prepareStatement(delete4gApnProvM5sql)
      for(i<-1 to deletetimes){
        pstmt4g.executeUpdate()
      }
      pstmt4g.close()
    }
    println("-------------------------4gApnProv--deleted")

    dbConn.close()
  }
}
