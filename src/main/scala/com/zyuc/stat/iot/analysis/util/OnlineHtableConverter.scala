package com.zyuc.stat.iot.analysis.util

import com.zyuc.stat.properties.ConfigProperties
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{Logging, SparkContext}

/**
  * 将hbase的表转换成DataFrame
  * @author zhoucw
  * @version 1.0
  */
object OnlineHtableConverter extends Logging {

  // struct结构
  val struct = StructType(Array(
    StructField("rowkey", StringType),
    StructField("compnyAndSerAndDomain", StringType),
    StructField("time", StringType),
    StructField("o_c_3_li", StringType),
    StructField("o_c_3_lo", StringType),
    StructField("o_c_3_nlo", StringType),
    StructField("o_c_4_li", StringType),

    StructField("o_c_4_lo", StringType),
    StructField("o_c_4_nlo", StringType),
    StructField("o_c_t_li", StringType),
    StructField("o_c_t_lo", StringType),
    StructField("o_c_t_nlo", StringType),

    StructField("o_c_3_on", StringType),
    StructField("o_c_4_on", StringType),
    StructField("o_c_t_on", StringType)
  ))

  /**
    * desc: 将hbase每条记录转换成Row
    * @param row  hbase每条记录的二元组, Tuple2[ImmutableBytesWritable, Result]
    * @return Row
    */
  def parse(row: Tuple2[ImmutableBytesWritable, Result]): Row = {
    try{
      val rkey = Bytes.toString(row._2.getRow)
      val compnyAndSerAndDomain = rkey.substring(0, rkey.lastIndexOf("_"))
      val time = rkey.substring(rkey.lastIndexOf("_") + 1)

      val o_c_3_li = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_3_li")))
      val o_c_3_lo = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_3_lo")))
      val o_c_3_nlo = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_3_nlo")))
      val o_c_4_li = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_4_li")))

      val o_c_4_lo = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_4_lo")))
      val o_c_4_nlo = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_4_nlo")))
      val o_c_t_li = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_t_li")))
      val o_c_t_lo = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_t_lo")))
      val o_c_t_nlo = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_t_nlo")))

      val o_c_3_on = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_3_on")))
      val o_c_4_on = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_4_on")))
      val o_c_t_on = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_t_on")))

      Row(
        rkey,
        compnyAndSerAndDomain,
        time,

        if(null==o_c_3_li) "0" else o_c_3_li,
        if(null==o_c_3_lo) "0" else o_c_3_lo,
        if(null==o_c_3_nlo) "0" else o_c_3_nlo,
        if(null==o_c_4_li) "0" else o_c_4_li,

        if(null==o_c_4_lo) "0" else o_c_4_lo,
        if(null==o_c_4_nlo) "0" else o_c_4_nlo,
        if(null==o_c_t_li) "0" else o_c_t_li,
        if(null==o_c_t_lo) "0" else o_c_t_lo,
        if(null==o_c_t_nlo) "0" else o_c_t_nlo,

        if(null==o_c_3_on) "0" else o_c_3_on,
        if(null==o_c_4_on) "0" else o_c_4_on,
        if(null==o_c_t_on) "0" else o_c_t_on
      )

    }catch {
      case e:Exception => {
        Row("0")
      }
    }

  }

  /**
    * desc: 将hbase表转换成DataFrame
    * @param sc
    * @param sqlContext
    * @param htable hbase表名
    * @return
    */
  def convertToDF(sc: SparkContext, sqlContext: SQLContext, htable: String, tuple2:Tuple2[String, String]): DataFrame = {
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    //hBaseConf.set("hbase.zookeeper.quorum","EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
    hBaseConf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    //设置zookeeper连接端口，默认2181
    //hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)

    hBaseConf.set(TableInputFormat.INPUT_TABLE, htable)
    if(tuple2 != null){
      hBaseConf.set(TableInputFormat.SCAN_ROW_START, tuple2._1)
      hBaseConf.set(TableInputFormat.SCAN_ROW_STOP, tuple2._2)
    }

    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val resultDF = sqlContext.createDataFrame(hbaseRDD.map(row => parse(row)).filter(_.length!=1), struct)

    resultDF
  }

}
