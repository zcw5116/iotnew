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
object MMEBaseStationHtableConverter extends Logging {

  // struct结构
  val struct = StructType(Array(
    StructField("rowkey", StringType),
    StructField("cmpcodeAndServAndDomainAndBs", StringType),
    StructField("cmpcodeAndServAndDomain", StringType),
    StructField("datatime", StringType),
    StructField("enbid", StringType),

    StructField("ma_sn", StringType),
    StructField("ma_rn", StringType)
  ))

  /**
    * desc: 将hbase每条记录转换成Row
    * @param row  hbase每条记录的二元组, Tuple2[ImmutableBytesWritable, Result]
    * @return Row
    */
  def parse(row: Tuple2[ImmutableBytesWritable, Result]): Row = {
    try{
      val rkey = Bytes.toString(row._2.getRow)
      val cmpcodeAndServAndDomainAndBs = rkey.substring(rkey.indexOf("_") + 1)
      val cmpcodeAndServAndDomain = cmpcodeAndServAndDomainAndBs.substring(0, cmpcodeAndServAndDomainAndBs.lastIndexOf("_"))
      val datatime = rkey.substring(0, rkey.indexOf("_"))
      val enbid = rkey.substring(rkey.lastIndexOf("_") + 1)

      val ma_sn = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ma_sn")))
      val ma_rn = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ma_rn")))
      var ma_rat_tmp = if(null == ma_rn || "0" == ma_rn) 0.000 else (ma_sn.toDouble/ma_rn.toDouble)
      val ma_rat = f"$ma_rat_tmp%1.4f"

      Row(
        rkey,
        cmpcodeAndServAndDomainAndBs,
        cmpcodeAndServAndDomain,
        datatime,
        enbid,
        if(null==ma_sn) "0" else ma_sn,
        if(null==ma_rn) "0" else ma_rn
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
  def convertToDF(sc: SparkContext, sqlContext: SQLContext, htable: String, scanRange:Tuple2[String, String]): DataFrame = {
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    //hBaseConf.set("hbase.zookeeper.quorum","EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
    hBaseConf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    //设置zookeeper连接端口，默认2181
    //hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    hBaseConf.set(TableInputFormat.SCAN_ROW_START, scanRange._1)
    hBaseConf.set(TableInputFormat.SCAN_ROW_STOP, scanRange._2)

    hBaseConf.set(TableInputFormat.INPUT_TABLE, htable)
    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val resultDF = sqlContext.createDataFrame(hbaseRDD.map(row => parse(row)).filter(_.length!=1), struct)

    resultDF
  }

}
