package com.zyuc.stat.iot.baseline

import com.zyuc.stat.iot.analysis.baseline.caseclass.HbaseMME
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
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-9-15.
  */
class MMEBaseLine {

  def registerRDD(sc: SparkContext, hCDRTable: String): RDD[HbaseMME] = {
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    hBaseConf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    hBaseConf.set(TableInputFormat.INPUT_TABLE, hCDRTable)
    val cf = "mmeinfo"
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val hTableRDD = hbaseRDD.map(r => HbaseMME(
      (Bytes.toString(r._2.getRow)).split("-")(0), (Bytes.toString(r._2.getRow)).split("-")(1),
      Bytes.toString(r._2.getRow),
      Bytes.toString(r._2.getValue(Bytes.toBytes(cf), Bytes.toBytes("c_req_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(cf), Bytes.toBytes("c_req_sucesscnt")))
    ))
    hTableRDD
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name") // name_2017073111
    val endDayid = sc.getConf.get("spark.app.baseLine.endDayid") // "20170906"
    val intervalDayNums = sc.getConf.get("spark.app.baseLine.intervalDayNums").toInt // “7”
    val hbaseMMETablePrefix = sc.getConf.get("spark.app.hbaseMMETablePrefix") // iot_mme_day_
    val targetdayid = sc.getConf.get("spark.app.htable.targetdayid")

    // 将每天的Hbase数据union all后映射为一个DataFrame
    var hbaseDF: DataFrame = null
    import sqlContext.implicits._
    for (i <- 0 until intervalDayNums) {
      val dayid = DateUtils.timeCalcWithFormatConvertSafe(endDayid, "yyyyMMdd", -i * 24 * 60 * 60, "yyyyMMdd")
      if (i == 0) {
        //hbaseDF = registerRDD(sc, hbaseMMETablePrefix + dayid).toDF()
      }
      if (i > 0) {
        hbaseDF = hbaseDF.unionAll(registerRDD(sc, hbaseMMETablePrefix + dayid).toDF())
      }
    }

    val tmpTable = "tmpHbaseFlow_" + endDayid
    hbaseDF.registerTempTable(tmpTable)

    //  对每个5分钟点的流量排序, 删除最大值和最小值后取平均值
    val tmpSql =
      s"""select companycode, time, companyAndTime, reqCnt, succCnt, succRadio,
         |row_number() over(partition by companycode, time order by succRadio) succRadioRN
         |from
         |(
         |select companycode, time, companyAndTime,
         |nvl(reqCnt,0) reqCnt, nvl(succCnt,0) succCnt,
         |case when nvl(reqCnt,0) = 0 or reqCnt=0 then 0 else nvl(succCnt,0)/reqCnt end succRadio
         |from ${tmpTable}
         |) t
       """.stripMargin

    val tmpDF = sqlContext.sql(tmpSql).cache()

    var reqDF = tmpDF.filter("succRadioRN>1").filter("succRadioRN<" + intervalDayNums)


    if (intervalDayNums <= 2) {
      reqDF = tmpDF
    }

    val reqAvgDF = reqDF.groupBy("companyAndTime").agg(avg("succRadio").as("succRadio")).coalesce(1)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)

    val targetHtable = hbaseMMETablePrefix + targetdayid
    val cf = "mmeinfo"
    val families = new Array[String](1)
    families(0) = cf
    HbaseUtils.createIfNotExists(targetHtable, families)


    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, targetHtable)

    val reqAvgRdd = reqAvgDF.rdd.map(x => (x.getString(0), x.getDouble(1)))

    val resultRdd = reqAvgRdd.map { arr => {
      val currentPut = new Put(Bytes.toBytes(arr._1))
      currentPut.addColumn(Bytes.toBytes(cf), Bytes.toBytes("b_req_succRadio"), Bytes.toBytes(arr._2.toString))
      (new ImmutableBytesWritable, currentPut)
    }
    }

    resultRdd.saveAsHadoopDataset(jobConf)

    sc.stop()

  }

}
