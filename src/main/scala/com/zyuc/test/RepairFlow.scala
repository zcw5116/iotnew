package com.zyuc.test

import com.zyuc.stat.iot.analysis.util.{CDRHtableConverter, HbaseDataUtil}
import com.zyuc.stat.properties.ConfigProperties
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 下午5:09.
  */
object RepairFlow {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val targetDay = sc.getConf.get("spark.app.targetDay")
    val sourceDay = sc.getConf.get("spark.app.sourceDay")

    val startTime = sc.getConf.get("spark.app.startTime") // 2355
    val endTime = sc.getConf.get("spark.app.endTime")

    val htablePre = "analyze_summ_rst_flow_"

    val targetHtable = htablePre + targetDay
    val sourceHtable = htablePre + sourceDay

    val sourceDF = CDRHtableConverter.convertToDF(sc, sqlContext, sourceHtable)

    val hisResDF = sourceDF.filter(s"time>=$startTime").filter(s"time<=$endTime").select("rowkey", "f_c_3_u", "f_c_3_d", "f_c_4_u",
      "f_c_4_d", "f_c_t_u", "f_c_t_d", "f_c_3_t", "f_c_4_t", "f_c_t_t")

    val hisResRDD = hisResDF.repartition(11).rdd.map(x=>{
      val rkey =    x(0).toString
      val f_c_3_u = x(1).toString
      val f_c_3_d = x(2).toString
      val f_c_4_u = x(3).toString
      val f_c_4_d = x(4).toString
      val f_c_t_u = x(5).toString
      val f_c_t_d = x(6).toString
      val f_c_3_t = x(7).toString
      val f_c_4_t = x(8).toString
      val f_c_t_t = x(9).toString

      val dayResPut = new Put(Bytes.toBytes(rkey))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_3_u"), Bytes.toBytes(f_c_3_u))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_3_d"), Bytes.toBytes(f_c_3_d))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_4_u"), Bytes.toBytes(f_c_4_u))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_4_d"), Bytes.toBytes(f_c_4_d))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_t_u"), Bytes.toBytes(f_c_t_u))

      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_t_d"), Bytes.toBytes(f_c_t_d))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_3_t"), Bytes.toBytes(f_c_3_t))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_4_t"), Bytes.toBytes(f_c_4_t))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_t_t"), Bytes.toBytes(f_c_t_t))

      (new ImmutableBytesWritable, dayResPut)

    })

    HbaseDataUtil.saveRddToHbase(targetHtable, hisResRDD)
  }

}
