package com.zyuc.stat.iotNBLiuzk.analysis.realtime

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-5-10.
  */
object CDRRealtimeM5Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name","name_CDRRealtimeM5Analysis")
    val datatime = sc.getConf.get("spark.app.datatime","201805101400")
    val inputPath = "hdfs://inmnmbd02:8020/user/epciot/data/cdr/transform/nb/data"
    val outputPath = "hdfs://inmnmbd02:8020/user/epciot/data/cdr/analy_realtime/nb"
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val d = datatime.substring(2, 8)
    val h = datatime.substring(8, 10)
    val m5 = datatime.substring(10, 12)
    val partitionPath = s"/d=$d/h=$h/m5=$m5"
    val dd = datatime.substring(0, 8)
    val hm5 = datatime.substring(8, 12)

    sqlContext.read.format("orc").load("/user/epciot/data/basic/AllUserInfo/").registerTempTable("AllUserTable")
    sqlContext.read.format("orc").load(inputPath + partitionPath).registerTempTable("CDRTempTable")

    val sql =
      s"""
         |select '${datatime}' as gather_cycle, '${dd}' as gather_date, '${hm5}' as gather_time,
         |a.cust_id, c.t801 as city, c.t800 as province,
         |sum(c.l_datavolumefbcuplink) as INFLOW , sum(c.l_datavolumefbcdownlink) as OUTFLOW,
         |(sum(c.l_datavolumefbcuplink) + sum(c.l_datavolumefbcdownlink)) as TOTALFLOW
         |from
         |CDRTempTable c
         |inner join
         |AllUserTable a
         |on c.mdn = a.mdn
         |group by a.cust_id, c.t800, c.t801
         |GROUPING SETS(a.cust_id,(a.cust_id, c.t800),(a.cust_id, c.t800, c.t801 ))
       """.stripMargin

    sqlContext.sql(sql)
      .repartition(20).write.mode(SaveMode.Overwrite).format("orc")
      .save(outputPath + partitionPath)

    //----从表中 根据tablename 取LASTCYCLE + CURRCYCLE  与datatime比较--更新--upset

    //select last curr from breakpoint where tablename=cdr
    //  if ---


  }

}
