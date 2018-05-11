package com.zyuc.stat.iotNBLiuzk.analysis.realtime

import com.zyuc.stat.properties.ConfigProperties
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-5-10.
  */
object CDRDaySummer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name","name_201805101400")
    val datatime = sc.getConf.get("spark.app.datatime","201805101400")
    val inputPath = "hdfs://inmnmbd02:8020/user/epciot/data/cdr/transform/nb/data"
    val outputPath = "hdfs://inmnmbd02:8020/user/epciot/data/cdr/summ_d/nb"
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val d = datatime.substring(2, 8)
    val dd = datatime.substring(0, 8)
    //val h = loadTime.substring(8, 10)
    //val m5 = loadTime.substring(10, 12)
    val partitionPath = s"/d=$d/h=*/m5=*"

    val df = sqlContext.read.format("orc").load(inputPath + partitionPath)
      .selectExpr("t800","t801","t804","l_datavolumefbcuplink","l_datavolumefbcdownlink","substr(servedimeisv,1,8) as tac")
      .registerTempTable("CDRTempTable")

    import sqlContext.implicits._
    sc.textFile("/user/epciot/data/basic/IOTTerminal/iotterminal.csv")
      .filter(!_.contains("This is a Test IMEI")).map(line=>line.split(",\"",5)).map(x=>(x(0),x(1),x(2),x(3),x(4)))
      .toDF("tac","x1","x2","x3", "devtype").registerTempTable("IOTTerminalTable")

    sqlContext.read.format("orc").load("/user/epciot/data/basic/AllUserInfo/").registerTempTable("AllUserTable")

/*    val sql1 =
      s"""
         |select t800,t801,l_datavolumefbcuplink,l_datavolumefbcdownlink,
         |substr(servedimeisv,1,8) tac
         |from CDRTempTable
       """.stripMargin
    sqlContext.sql(sql1).registerTempTable("CDRTempTable2")*/

    val sql =
      s"""
         |select '${dd}' as summ_cycle, a.cust_id,
         |c.t800 as province, c.t801 as city, c.t804, i.devtype
         |sum(c.l_datavolumefbcuplink) as INFLOW , sum(c.l_datavolumefbcdownlink) as OUTFLOW,
         |(sum(c.l_datavolumefbcuplink) + sum(c.l_datavolumefbcdownlink)) as TOTALFLOW,
         |count(distinct mdn) as ACTIVEUSERS ,count(chargingid) as SESSIONS
         |from
         |CDRTempTable c
         |left join
         |IOTTerminalTable i
         |on c.tac=i.tac
         |inner join
         |AllUserTable a
         |on c.mdn = a.mdn
         |group by a.cust_id, c.t800, c.t801, c.t804, i.devtype
         |GROUPING SETS(a.cust_id,(a.cust_id, c.t800),(a.cust_id, c.t800, c.t801 ),(a.cust_id, c.t800, c.t801, c.t804),(a.cust_id, c.t800, c.t801, c.t804, i.devtype))
       """.stripMargin

    sqlContext.sql(sql)
      .repartition(20).write.mode(SaveMode.Overwrite).format("orc")
      .save(outputPath + partitionPath)


  }
}
