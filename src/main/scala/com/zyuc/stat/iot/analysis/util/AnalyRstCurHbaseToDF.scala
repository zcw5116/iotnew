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
  * Created by dell on 2017/10/10.
  */
object AnalyRstCurHbaseToDF extends Logging{
  // struct结构
  val struct = StructType(Array(
    StructField("rowkey", StringType),

    StructField("companycode", StringType),
    StructField("servtype", StringType),
    StructField("domain", StringType),
    StructField("summtime", StringType),
    StructField("c_d_t_un", StringType),
    StructField("o_c_t_on", StringType),
    StructField("o_d_t_on", StringType),

    StructField("f_c_3_t", StringType),
    StructField("f_c_4_t", StringType),
    StructField("f_c_t_t", StringType),
    StructField("f_d_3_t", StringType),
    StructField("f_d_4_t", StringType),
    StructField("f_d_t_t", StringType),

    StructField("ms_c_s_n", StringType),
    StructField("ms_c_r_n", StringType),
    StructField("ms_c_t_n", StringType),
    StructField("ms_d_s_n", StringType),
    StructField("ms_d_r_n", StringType),
    StructField("ms_d_t_n", StringType)
  ))

  /**
    * desc: 将hbase每条记录转换成Row
    * @param row  hbase每条记录的二元组, Tuple2[ImmutableBytesWritable, Result]
    * @return Row
    */
  def parse(row: Tuple2[ImmutableBytesWritable, Result]): Row = {
    try{
      val rkey = Bytes.toString(row._2.getRow)
      // rowkey {YYMMDD}_{COMPANYCODE}_{SERVICETYPE}_{DOMAIN}
      val companycode = rkey.split("_",4)(1)
      val servtype    = rkey.split("_",4)(2)
      val domain      = rkey.split("_",4)(3)
      val summtime    = rkey.split("_",4)(0)
      val c_d_t_un     = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("c_d_t_un")))
      val o_c_t_on     = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_c_t_on")))
      val o_d_t_on     = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("o_d_t_on")))
      val f_c_3_t      = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("f_c_3_t")))
      val f_c_4_t      = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("f_c_4_t")))
      val f_c_t_t      = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("f_c_t_t")))
      val f_d_3_t      = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("f_d_3_t")))
      val f_d_4_t      = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("f_d_4_t")))
      val f_d_t_t      = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("f_d_t_t")))
      val ms_c_s_n     = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("ms_c_s_n")))
      val ms_c_r_n     = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("ms_c_r_n")))
      val ms_c_t_n     = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("ms_c_t_n")))
      val ms_d_s_n     = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("ms_d_s_n")))
      val ms_d_r_n     = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("ms_d_r_n")))
      val ms_d_t_n     = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("ms_d_t_n")))
      Row(
        rkey,
        companycode,
        servtype,
        domain,
        summtime,
        if(null==c_d_t_un) "0" else c_d_t_un,
        if(null==o_c_t_on) "0" else o_c_t_on,
        if(null==o_d_t_on) "0" else o_d_t_on,

        if(null==f_c_3_t) "0" else f_c_3_t,
        if(null==f_c_4_t) "0" else f_c_4_t,
        if(null==f_c_t_t) "0" else f_c_t_t,
        if(null==f_d_3_t) "0" else f_d_3_t,
        if(null==f_d_4_t) "0" else f_d_4_t,
        if(null==f_d_t_t) "0" else f_d_t_t,

        if(null==ms_c_s_n) "0" else ms_c_s_n,
        if(null==ms_c_r_n) "0" else ms_c_r_n,
        if(null==ms_c_t_n) "0" else ms_c_t_n,
        if(null==ms_d_s_n) "0" else ms_d_s_n,
        if(null==ms_d_r_n) "0" else ms_d_r_n,
        if(null==ms_d_t_n) "0" else ms_d_t_n
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
