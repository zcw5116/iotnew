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
object AreaBaseHtableConverter extends Logging {

  // struct结构
  val struct1 = StructType(Array(
    StructField("rowkey", StringType),
    StructField("nettype", StringType),
    StructField("cAndSAndD", StringType),
    StructField("cAndSAndDAndB", StringType),
    StructField("datatime", StringType),

    StructField("enbid", StringType),
    StructField("enbname", StringType),
    StructField("cityid", StringType),
    StructField("cityname", StringType),
    StructField("provid", StringType),

    StructField("provname", StringType),
    StructField("ma_sn", StringType),
    StructField("ma_rn", StringType),
    StructField("a_sn", StringType),
    StructField("a_rn", StringType),

    StructField("f_d", StringType),
    StructField("f_u", StringType),
    StructField("o_ln", StringType),
    StructField("o_fln", StringType)
  ))

  /**
    * desc: 将hbase每条记录转换成Row
    * @param row  hbase每条记录的二元组, Tuple2[ImmutableBytesWritable, Result]
    * @return Row
    */
  def parse1(row: Tuple2[ImmutableBytesWritable, Result]): Row = {
    try{
      val rkey = Bytes.toString(row._2.getRow) //  "201710261310_3g_P100002368_C_-1_3608FFFF1C70"
      val arr = rkey.split("_", 6)

      val nettype = arr(1)
      val cAndSAndD = arr(2) + "_" + arr(3) +"_" + arr(4) // companycode And Servtype And Domain
      val cAndSAndDAndB = arr(2) + "_" + arr(3) +"_" + arr(4) + "_" + arr(5) // companycode And Servtype And Domain And enbid
      val datatime = arr(0)
      val enbid = arr(5)

      val enbname = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("enbname")))
      val cityid = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("cityid")))
      val cityname = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("cityname")))
      val provid = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("provid")))
      val provname = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("provname")))

      val ma_sn = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ma_sn")))
      val ma_rn = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ma_rn")))
      val a_sn = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a_sn")))
      val a_rn = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a_rn")))
      val f_u = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("f_u")))
      val f_d = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("f_d")))
      val o_ln = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("o_ln")))
      val o_fln = Bytes.toString(row._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("o_fln")))

/*      var ma_rat_tmp = if(null == ma_rn || "0" == ma_rn) 0.000 else (ma_sn.toDouble/ma_rn.toDouble)
      val ma_rat = f"$ma_rat_tmp%1.4f"

      var a_rat_tmp = if(null == a_rn || "0" == a_rn) 0.000 else (a_sn.toDouble/a_rn.toDouble)
      val a_rat = f"$a_rat_tmp%1.4f"*/


      Row(
        rkey,
        nettype,
        cAndSAndD,
        cAndSAndDAndB,
        datatime,

        enbid,
        enbname,
        cityid,
        cityname,
        provid,
        provname,
        if(null==ma_sn) "0" else ma_sn,
        if(null==ma_rn) "0" else ma_rn,
        if(null==a_sn) "0" else a_sn,

        if(null==a_rn) "0" else a_rn,
        if(null==f_u) "0" else f_u,
        if(null==f_d) "0" else f_d,
        if(null==o_ln) "0" else o_ln,
        if(null==o_fln) "0" else o_fln
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
  def convertToDF1(sc: SparkContext, sqlContext: SQLContext, htable: String, scanRange:Tuple2[String, String]): DataFrame = {
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
    val resultDF = sqlContext.createDataFrame(hbaseRDD.map(row => parse1(row)).filter(_.length!=1), struct1)

    resultDF
  }

}
