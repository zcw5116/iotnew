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
object AuthHtableConverter extends Logging {

  // struct结构
  val struct = StructType(Array(
    StructField("rowkey", StringType),
    StructField("compnyAndSerAndDomain", StringType),
    StructField("time", StringType),
    StructField("a_c_3_rn", StringType),      // req_3g_cnt
    StructField("a_c_3_sn", StringType), // req_3g_succ_cnt
    StructField("a_c_4_rn", StringType),      // req_4g_cnt

    StructField("a_c_4_sn", StringType), // req_4g_succ_cnt
    StructField("a_c_v_rn", StringType),    // req_vpdn_cnt
    StructField("a_c_v_sn", StringType), // req_vpdn_succ_cnt
    StructField("a_c_t_rn", StringType), // req_total_cnt
    StructField("a_c_t_sn", StringType), // req_total_succ_cnt

    StructField("a_c_3_rcn", StringType), // req_3g_card_cnt
    StructField("a_c_3_scn", StringType), // req_3g_card_succ_cnt
    StructField("a_c_3_fcn", StringType), // req_3g_card_fail_cnt
    StructField("a_c_4_rcn", StringType), // req_4g_card_cnt
    StructField("a_c_4_scn", StringType), // req_4g_card_succ_cnt

    StructField("a_c_4_fcn", StringType), // req_4g_card_fail_cnt
    StructField("a_c_v_rcn", StringType),  // req_vpdn_card_cnt
    StructField("a_c_v_scn", StringType), // req_vpdn_card_succ_cnt
    StructField("a_c_v_fcn", StringType), // req_vpdn_card_fail_cnt
    StructField("a_c_t_rcn", StringType), // req_total_card_cnt

    StructField("a_c_t_scn", StringType), // req_total_card_succ_cnt
    StructField("a_c_t_fcn", StringType), // req_total_card_fail_cnt
    StructField("a_c_3_rat", StringType),           // req_3g_ration
    StructField("a_c_4_rat", StringType),           // req_4g_ration
    StructField("a_c_v_rat", StringType),         // req_vpdn_ration

    StructField("a_c_t_rat", StringType)        // req_total_ration
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
      val a_c_3_rn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_3_rn")))
      val a_c_3_sn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_3_sn")))
      val a_c_4_rn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_4_rn")))

      val a_c_4_sn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_4_sn")))
      val a_c_v_rn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_v_rn")))
      val a_c_v_sn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_v_sn")))
      val a_c_t_rn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_t_rn")))
      val a_c_t_sn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_t_sn")))


      val a_c_3_rcn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_3_rcn")))
      val a_c_3_scn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_3_scn")))
      val a_c_3_fcn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_3_fcn")))
      val a_c_4_rcn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_4_rcn")))
      val a_c_4_scn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_4_scn")))

      val a_c_4_fcn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_4_fcn")))
      val a_c_v_rcn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_v_rcn")))
      val a_c_v_scn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_v_scn")))
      val a_c_v_fcn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_v_fcn")))
      val a_c_t_rcn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_t_rcn")))

      val a_c_t_scn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_t_scn")))
      val a_c_t_fcn = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_t_fcn")))
      val a_c_3_rat = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_3_rat")))
      val a_c_4_rat = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_4_rat")))
      val a_c_v_rat = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_v_rat")))

      val a_c_t_rat = Bytes.toString(row._2.getValue(Bytes.toBytes("s"), Bytes.toBytes("a_c_t_rat")))


      Row(
        rkey,
        compnyAndSerAndDomain,
        time,
        if(null==a_c_3_rn) "0" else a_c_3_rn,
        if(null==a_c_3_sn) "0" else a_c_3_sn,
        if(null==a_c_4_rn) "0" else a_c_4_rn,

        if(null==a_c_4_sn) "0" else a_c_4_sn,
        if(null==a_c_v_rn) "0" else a_c_v_rn,
        if(null==a_c_v_sn) "0" else a_c_v_sn,
        if(null==a_c_t_rn) "0" else a_c_t_rn,
        if(null==a_c_t_sn) "0" else a_c_t_sn,

        if(null==a_c_3_rcn) "0" else a_c_3_rcn,
        if(null==a_c_3_scn) "0" else a_c_3_scn,
        if(null==a_c_3_fcn) "0" else a_c_3_fcn,
        if(null==a_c_4_rcn) "0" else a_c_4_rcn,
        if(null==a_c_4_scn) "0" else a_c_4_scn,

        if(null==a_c_4_fcn) "0" else a_c_4_fcn,
        if(null==a_c_v_rcn) "0" else a_c_v_rcn,
        if(null==a_c_v_scn) "0" else a_c_v_scn,
        if(null==a_c_v_fcn) "0" else a_c_v_fcn,
        if(null==a_c_t_rcn) "0" else a_c_t_rcn,

        if(null==a_c_t_scn) "0" else a_c_t_scn,
        if(null==a_c_t_fcn) "0" else a_c_t_fcn,
        if(null==a_c_3_rat) "0" else a_c_3_rat,
        if(null==a_c_4_rat) "0" else a_c_4_rat,
        if(null==a_c_v_rat) "0" else a_c_v_rat,

        if(null==a_c_t_rat) "0" else a_c_t_rat
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
