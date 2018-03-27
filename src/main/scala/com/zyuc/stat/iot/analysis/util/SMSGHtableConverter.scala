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
object SMSGHtableConverter extends Logging {

  // struct结构
  val struct = StructType(Array(
    StructField("rowkey", StringType),
    StructField("compnyAndSerAndDomain", StringType),
    StructField("time", StringType),

    StructField("ms_c_r_n", StringType),
    StructField("ms_c_s_n", StringType),
    StructField("ms_c_t_n", StringType),
    StructField("ms_c_r_cn", StringType),
    StructField("ms_c_s_cn", StringType),

    StructField("ms_c_t_cn", StringType)
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

      val ms_c_r_n = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("ms_c_r_n")))
      val ms_c_s_n = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("ms_c_s_n")))
      val ms_c_t_n = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("ms_c_t_n")))
      val ms_c_r_cn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("ms_c_r_cn")))
      val ms_c_s_cn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("ms_c_s_cn")))

      val ms_c_t_cn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("ms_c_t_cn")))


      Row(
        rkey,
        compnyAndSerAndDomain,
        time,
        if(null==ms_c_r_n) "0" else ms_c_r_n,
        if(null==ms_c_s_n) "0" else ms_c_s_n,
        if(null==ms_c_t_n) "0" else ms_c_t_n,
        if(null==ms_c_r_cn) "0" else ms_c_r_cn,
        if(null==ms_c_s_cn) "0" else ms_c_s_cn,
        if(null==ms_c_t_cn) "0" else ms_c_t_cn
      )

    }catch {
      case e:Exception => {
        logError("ParseError rowkey: [" + Bytes.toString(row._2.getRow) + "] msg[" + e.getMessage + "]")
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
  def convertToDF(sc: SparkContext, sqlContext: SQLContext, htable: String): DataFrame = {
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    //hBaseConf.set("hbase.zookeeper.quorum","EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
    hBaseConf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    //设置zookeeper连接端口，默认2181
    //hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)

    hBaseConf.set(TableInputFormat.INPUT_TABLE, htable)
    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val resultDF = sqlContext.createDataFrame(hbaseRDD.map(row => parse(row)).filter(_.length!=1), struct)

    resultDF
  }

}
