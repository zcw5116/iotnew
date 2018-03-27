package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-10-16.
  */
object TestOracle {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:oracle:thin:epcslview/epc_slview129@//100.66.124.129:1521/dbnms",
        "dbtable" -> "epcslview.test", "driver" -> "oracle.jdbc.driver.OracleDriver")).load()

  }

}
