package com.zyuc.stat.iot.analysis.baseline

import com.zyuc.stat.iot.analysis.baseline.caseclass.HbaseCDRFlow
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

/**
  * Created by zhoucw on 17-9-7.
  * @deprecated
  */
object CDRBaseLine {
  def registerRDD(sc:SparkContext, hCDRTable:String):RDD[HbaseCDRFlow] = {
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    //hBaseConf.set("hbase.zookeeper.quorum","EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
    hBaseConf.set("hbase.zookeeper.quorum",ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    //设置zookeeper连接端口，默认2181
    //hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)

    hBaseConf.set(TableInputFormat.INPUT_TABLE, hCDRTable)

    val flowCF = "flowinfo"

    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val hTableRDD = hbaseRDD.map(r=>HbaseCDRFlow(
      (Bytes.toString(r._2.getRow)).split("-")(0), (Bytes.toString(r._2.getRow)).split("-")(1),
      Bytes.toString(r._2.getRow),
      Bytes.toString(r._2.getValue(Bytes.toBytes(flowCF),Bytes.toBytes("c_pdsn_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(flowCF),Bytes.toBytes("c_pdsn_downflow"))),

      Bytes.toString(r._2.getValue(Bytes.toBytes(flowCF),Bytes.toBytes("c_pgw_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(flowCF),Bytes.toBytes("c_pgw_downflow")))

    ))
    hTableRDD
  }


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name") // name_2017073111
    val endDayid = sc.getConf.get("spark.app.baseLine.endDayid") // "20170906"
    val intervalDayNums = sc.getConf.get("spark.app.baseLine.intervalDayNums").toInt // “7”
    val hbaseFlowTablePrefix = sc.getConf.get("spark.app.hbaseFlowTablePrefix") // iot_cdr_flow_stat_
    val targetdayid = sc.getConf.get("spark.app.htable.targetdayid")
    // val logType = sc.getConf.get("spark.app.cdr.logtype")

    // 将每天的Hbase数据union all后映射为一个DataFrame
    var hbaseDF:DataFrame = null
    import sqlContext.implicits._
    for(i <- 0 until intervalDayNums){
      val dayid = DateUtils.timeCalcWithFormatConvertSafe(endDayid, "yyyyMMdd", -i*24*60*60, "yyyyMMdd")
      if(i == 0){
        hbaseDF = registerRDD(sc, hbaseFlowTablePrefix + dayid).toDF()
      }
      if(i>0){
        hbaseDF = hbaseDF.unionAll(registerRDD(sc, hbaseFlowTablePrefix + dayid).toDF())
      }
    }

    val tmpTable = "tmpHbaseFlow_" + endDayid
    hbaseDF.registerTempTable(tmpTable)

    //  对每个5分钟点的流量排序, 删除最大值和最小值后取平均值
    val tmpSql =
      s"""
         |select companycode, time, companyAndTime, nvl(upFlowPDSN,0) upFlowPDSN, nvl(downFlowPDSN,0) downFlowPDSN,
         |nvl(upFlowPGW,0) upFlowPGW, nvl(downFlowPGW,0) downFlowPGW,
         |row_number() over(partition by companycode, time order by upFlowPDSN) upPdsnRN,
         |row_number() over(partition by companycode, time order by downFlowPDSN) downPdsnRN,
         |row_number() over(partition by companycode, time order by upFlowPGW) upPgwRN,
         |row_number() over(partition by companycode, time order by downFlowPGW) downPgwRN
         |from ${tmpTable}
       """.stripMargin

    val tmpDF = sqlContext.sql(tmpSql).cache()

    var upPdsnDF = tmpDF.filter("upPdsnRN>1").filter("upPdsnRN<" + intervalDayNums)
    var downPdsnDF = tmpDF.filter("downPdsnRN>1").filter("downPdsnRN<" + intervalDayNums)
    var upPgwDF = tmpDF.filter("upPgwRN>1").filter("upPgwRN<" + intervalDayNums)
    var downPgwDF = tmpDF.filter("downPgwRN>1").filter("downPgwRN<" + intervalDayNums)

    if(intervalDayNums <= 2){
      upPdsnDF = tmpDF
      downPdsnDF = tmpDF
      upPgwDF = tmpDF
      downPgwDF = tmpDF
    }

    val upPdsnRes = upPdsnDF.groupBy("companyAndTime").agg(avg("upFlowPDSN").as("b_pdsn_upflow")).coalesce(1)
    val downPdsnRes =  downPdsnDF.groupBy("companyAndTime").agg(avg("downFlowPDSN").as("b_pdsn_downflow")).coalesce(1)
    val upPgwRes = upPgwDF.groupBy("companyAndTime").agg(avg("upFlowPGW").as("b_pgw_upflow")).coalesce(1)
    val downPgwRes = downPgwDF.groupBy("companyAndTime").agg(avg("downFlowPGW").as("b_pgw_downflow")).coalesce(1)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)

    val targetHtable = hbaseFlowTablePrefix + targetdayid
    val flowCF = "flowinfo"
    val families = new Array[String](1)
    families(0) = flowCF
    HbaseUtils.createIfNotExists(targetHtable,families)


    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, targetHtable)

    val upPdsnResRdd = upPdsnRes.rdd.map(x => (x.getString(0), x.getDouble(1)))
    val downPdsnResRdd = downPdsnRes.rdd.map(x => (x.getString(0), x.getDouble(1)))
    val upPgwResRdd = upPgwRes.rdd.map(x => (x.getString(0), x.getDouble(1)))
    val downPgwResRdd = downPgwRes.rdd.map(x => (x.getString(0), x.getDouble(1)))


    val  upPdsnRdd = upPdsnResRdd.map { arr => {
      val currentPut = new Put(Bytes.toBytes(arr._1))
      currentPut.addColumn(Bytes.toBytes(flowCF), Bytes.toBytes("b_pdsn_upflow"), Bytes.toBytes(arr._2.toString))
      (new ImmutableBytesWritable, currentPut)
    }
    }

    val  downPdsnRdd = downPdsnResRdd.map { arr => {
      val currentPut = new Put(Bytes.toBytes(arr._1))
      currentPut.addColumn(Bytes.toBytes(flowCF), Bytes.toBytes("b_pdsn_downflow"), Bytes.toBytes(arr._2.toString))
      (new ImmutableBytesWritable, currentPut)
    }
    }

    val  upPgwRdd = upPgwResRdd.map { arr => {
      val currentPut = new Put(Bytes.toBytes(arr._1))
      currentPut.addColumn(Bytes.toBytes(flowCF), Bytes.toBytes("b_pgw_upflow"), Bytes.toBytes(arr._2.toString))
      (new ImmutableBytesWritable, currentPut)
    }
    }

    val  downPgwRdd = downPgwResRdd.map { arr => {
      val currentPut = new Put(Bytes.toBytes(arr._1))
      currentPut.addColumn(Bytes.toBytes(flowCF), Bytes.toBytes("b_pgw_downflow"), Bytes.toBytes(arr._2.toString))
      (new ImmutableBytesWritable, currentPut)
    }
    }

    upPdsnRdd.saveAsHadoopDataset(jobConf)
    downPdsnRdd.saveAsHadoopDataset(jobConf)
    upPgwRdd.saveAsHadoopDataset(jobConf)
    downPgwRdd.saveAsHadoopDataset(jobConf)

    sc.stop()
  }

}
