package com.zyuc.stat.iot.service

import com.zyuc.stat.iot.dao.CDRDataDAO
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-8-8.
  */
case class HbaseCdrLog(companycode:String, time:String, company_time:String,
                       c_pdsn_upflow:String,c_pdsn_downflow:String,c_pgw_upflow:String,c_pgw_downflow:String,
                       p_pdsn_upflow:String,p_pdsn_downflow:String,p_pgw_upflow:String,p_pgw_downflow:String,
                       b_pdsn_upflow:String,b_pdsn_downflow:String,b_pgw_upflow:String,b_pgw_downflow:String)

object CDRDataService extends Logging{
  def registerCdrRDD(sc:SparkContext, yyyyMMddHHmm:String):RDD[HbaseCdrLog] = {
    val dayid = yyyyMMddHHmm.substring(0,8)
    val htable = "iot_cdr_flow_stat_" + dayid
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    //hBaseConf.set("hbase.zookeeper.quorum","EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
    hBaseConf.set("hbase.zookeeper.quorum",ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    //设置zookeeper连接端口，默认2181
    //hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)

    hBaseConf.set(TableInputFormat.INPUT_TABLE,htable)

    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val cdrRDD = hbaseRDD.map(r=>HbaseCdrLog(
      (Bytes.toString(r._2.getRow)).split("-")(0), (Bytes.toString(r._2.getRow)).split("-")(1),
      Bytes.toString(r._2.getRow),

      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("c_pdsn_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("c_pdsn_downflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("c_pgw_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("c_pgw_downflow"))),

      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("p_pdsn_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("p_pdsn_downflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("p_pgw_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("p_pgw_downflow"))),

      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("b_pdsn_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("b_pdsn_downflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("b_pgw_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("b_pgw_downflow")))
    ))
    cdrRDD
  }

  def saveRddData(cdrDF:DataFrame, yyyyMMddHHmm:String): Unit ={

    // 分钟级别 比如 8点20转换为： 0820
    val startM5 = yyyyMMddHHmm.substring(8, 12)
    val endM5 = DateUtils.timeCalcWithFormatConvertSafe(startM5,"HHmm", 5*60, "HHmm")
    val dayid = yyyyMMddHHmm.substring(0, 8)

    val hbaseTable = "iot_cdr_flow_stat_" + dayid

    val families = new Array[String](1)
    families(0) = "flowinfo"
    HbaseUtils.createIfNotExists(hbaseTable,families)

    val conf = HbaseUtils.getHbaseConf( ConfigProperties.IOT_ZOOKEEPER_QUORUM, ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    val cdrJobConf = new JobConf(conf, this.getClass)
    cdrJobConf.setOutputFormat(classOf[TableOutputFormat])
    cdrJobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable)

    val hbaserdd = cdrDF.rdd.map(x => (x.getString(0), x.getString(1), x.getDouble(2), x.getDouble(3)))
    val cdrrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val currentPut = new Put(Bytes.toBytes(arr._1 + "-" + startM5.toString))
      currentPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("c_"+arr._2+"_upflow"), Bytes.toBytes(arr._3.toString))
      currentPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("c_"+arr._2+"_downflow"), Bytes.toBytes(arr._4.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }
    cdrrdd.saveAsHadoopDataset(cdrJobConf)

    val cdrnextrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val nextPut = new Put(Bytes.toBytes(arr._1 + "-" + endM5.toString))
      nextPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("p_"+arr._2+"_upflow"), Bytes.toBytes(arr._3.toString))
      nextPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("p_"+arr._2+"_downflow"), Bytes.toBytes(arr._4.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, nextPut)
    }
    }
    cdrnextrdd.saveAsHadoopDataset(cdrJobConf)

  }

  def SaveRddToAlarm(sc:SparkContext, sqlContext:SQLContext, df:DataFrame, yyyyMMddHHmm:String) = {
    val startM5 = yyyyMMddHHmm.substring(8, 12)
    val dayid = yyyyMMddHHmm.substring(0, 8)

    val alarmHtable = "analyze_rst_tab"
    val hDFtable = "htable"
    df.registerTempTable(hDFtable)

    val alarmSql = "select  case when length(companycode)=0 then 'N999999999' else companycode end as companycode, " +
      " nvl(c_pdsn_upflow,0), nvl(c_pdsn_downflow,0), nvl(p_pdsn_upflow,0), nvl(p_pdsn_downflow,0), nvl(b_pdsn_upflow,0), nvl(b_pdsn_downflow,0), " +
      " nvl(c_pgw_upflow,0), nvl(c_pgw_downflow,0), nvl(p_pgw_upflow,0), nvl(p_pgw_downflow,0), nvl(b_pgw_upflow,0), nvl(b_pgw_downflow,0) " +
      " from " + hDFtable + "  where time='" + startM5 + "' "

    val conf = HbaseUtils.getHbaseConf( ConfigProperties.IOT_ZOOKEEPER_QUORUM, ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    val alarmJobConf = new JobConf(conf, this.getClass)
    alarmJobConf.setOutputFormat(classOf[TableOutputFormat])
    alarmJobConf.set(TableOutputFormat.OUTPUT_TABLE, alarmHtable)

    // 统计当前时刻的累计流量， 存入预警表和流量的hbase表
    // 流量的hbase表
    val hbaseCdrTable = "iot_cdr_flow_stat_" + dayid
    val families = new Array[String](1)
    families(0) = "flowinfo"
    HbaseUtils.createIfNotExists(hbaseCdrTable,families)

    val cdrJobConf = new JobConf(conf, this.getClass)
    cdrJobConf.setOutputFormat(classOf[TableOutputFormat])
    cdrJobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseCdrTable)


    val alarmdf = sqlContext.sql(alarmSql).coalesce(1)

    // type, vpdncompanycode, authcnt, successcnt, failedcnt, authmdnct, authfaieldcnt
    val alarmrdd = alarmdf.rdd.map(x => (x.getString(0),
      x.getString(1),x.getString(2), x.getString(3) , x.getString(4),
      x.getString(5),x.getString(6),x.getString(7),x.getString(8),
      x.getString(9),x.getString(10),x.getString(11),x.getString(12)))


    val alarmhbaserdd = alarmrdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val currentPut = new Put(Bytes.toBytes(arr._1))
      val cdrPut = new Put(Bytes.toBytes(arr._1 + "-" + startM5.toString))

      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_c_pdsn_time"), Bytes.toBytes(startM5.toString))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_c_pdsn_up"), Bytes.toBytes(arr._2))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_c_pdsn_down"), Bytes.toBytes(arr._3))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_p_pdsn_up"), Bytes.toBytes(arr._4))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_p_pdsn_down"), Bytes.toBytes(arr._5))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_b_pdsn_up"), Bytes.toBytes(arr._6))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_b_pdsn_down"), Bytes.toBytes(arr._7))


      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_c_pgw_time"), Bytes.toBytes(startM5.toString))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_c_pgw_up"), Bytes.toBytes(arr._8))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_c_pgw_down"), Bytes.toBytes(arr._9))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_p_pgw_up"), Bytes.toBytes(arr._10))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_p_pgw_down"), Bytes.toBytes(arr._11))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_b_pgw_up"), Bytes.toBytes(arr._12))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_b_pgw_down"), Bytes.toBytes(arr._13))

      // 当前时刻汇总流量入告警表
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_c_in_sum"), Bytes.toBytes((arr._3.toDouble + arr._9.toDouble).toString ))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_c_out_sum"), Bytes.toBytes((arr._2.toDouble + arr._8.toDouble).toString ))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_c_time"), Bytes.toBytes(yyyyMMddHHmm))
      // 当前时刻汇总流量入话单表
      cdrPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("flow_c_in_sum"), Bytes.toBytes((arr._3.toDouble + arr._9.toDouble).toString ))
      cdrPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("flow_c_out_sum"), Bytes.toBytes((arr._2.toDouble + arr._8.toDouble).toString ))

      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      ((new ImmutableBytesWritable, currentPut), (new ImmutableBytesWritable, cdrPut))
    }
    }

    alarmhbaserdd.map(_._1).saveAsHadoopDataset(alarmJobConf)
    alarmhbaserdd.map(_._2).saveAsHadoopDataset(cdrJobConf)





    val aggSql = "select companycode, " +
      " sum(nvl(c_pdsn_upflow,0)) as c_pdsn_upflow, sum(nvl(c_pdsn_downflow,0)) as c_pdsn_downflow, " +
      " sum(nvl(c_pgw_upflow,0)) as c_pgw_upflow, sum(nvl(c_pgw_downflow,0)) as c_pgw_downflow " +
      " from " + hDFtable + "  where time<='" + startM5  + "' group by companycode"


    logInfo("aggSql: " + aggSql)
    val aggDF = sqlContext.sql(aggSql).coalesce(1)

    // type, vpdncompanycode, authcnt, successcnt, failedcnt, authmdnct, authfaieldcnt
    val aggrdd = aggDF.rdd.map(x => (x.getString(0),
      x.getDouble(1),x.getDouble(2), x.getDouble(3) , x.getDouble(4)))

    val aggHbaseRdd = aggrdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val aggPut = new Put(Bytes.toBytes(arr._1))
      val cdrPut = new Put(Bytes.toBytes(arr._1 + "-" + startM5.toString))

      // 截止当前时刻日汇总流量入告警表
      aggPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_d_in_sum"), Bytes.toBytes((arr._3 + arr._5).toString))
      aggPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("flow_d_out_sum"), Bytes.toBytes((arr._2 + arr._4).toString))
      // 截止当前时刻日汇总流量入话单表
      cdrPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("flow_d_in_sum"), Bytes.toBytes((arr._3 + arr._5).toString))
      cdrPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("flow_d_out_sum"), Bytes.toBytes((arr._2 + arr._4).toString))

      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      ((new ImmutableBytesWritable, aggPut),(new ImmutableBytesWritable, cdrPut))
    }
    }

    aggHbaseRdd.map(_._1).saveAsHadoopDataset(alarmJobConf)
    aggHbaseRdd.map(_._2).saveAsHadoopDataset(cdrJobConf)

  }
}
