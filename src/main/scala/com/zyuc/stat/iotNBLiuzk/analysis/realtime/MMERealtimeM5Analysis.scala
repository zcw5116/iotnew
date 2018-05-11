package com.zyuc.stat.iotNBLiuzk.analysis.realtime

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-5-10.
  */
object MMERealtimeM5Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name","name_MMERealtimeM5Analysis")
    val datatime = sc.getConf.get("spark.app.datatime","201805101400")
    val inputPath = "hdfs://inmnmbd02:8020/user/epciot/data/mme/transform/nb/data"
    val outputPath = "hdfs://inmnmbd02:8020/user/epciot/data/mme/analy_realtime/nb"
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val d = datatime.substring(2, 8)
    val dd = datatime.substring(0, 8)
    val h = datatime.substring(8, 10)
    val m5 = datatime.substring(10, 12)
    val partitionPath = s"/d=$d/h=$h/m5=$m5"


    val df = sqlContext.read.format("orc").load(inputPath + partitionPath).registerTempTable("MMETempTable")
    val sql =
      s"""
         |select '${datatime}' as gather_cycle, '${dd}' as gather_date, '${h}+${m5}' as gather_time,
         |a.cust_id, m.t800 as province, m.t801 as city,
         |count(*) as REQS ,
         |sum(case when m.result="failed" then 1 else 0 end) as FAILREQS ,
         |count(distinct (case when result = 'failed' then MSISDN else '' end)) as FAILUSERS
         |from
         |MMETempTable m
         |inner join
         |AllUserTable a
         |on a.---=m.---//////
         |group by a.cust_id, c.t800, c.t801
         |GROUPING SETS(a.cust_id,(a.cust_id, c.t800),(a.cust_id, c.t800, c.t801 ))
       """.stripMargin

    sqlContext.sql(sql)
      .coalesce(1).write.mode(SaveMode.Overwrite).format("orc")
      .save(outputPath + partitionPath)
  }

}
