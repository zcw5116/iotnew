package com.zyuc.stat.iot.baseline

import com.zyuc.stat.iot.baseline.caseclass.HbaseOnline
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
object OnlineBaseLine {

  def registerRDD(sc: SparkContext, hCDRTable: String): RDD[HbaseOnline] = {
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    hBaseConf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    hBaseConf.set(TableInputFormat.INPUT_TABLE, hCDRTable)
    val cf = "info"
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val hTableRDD = hbaseRDD.map(r => HbaseOnline(
      (Bytes.toString(r._2.getRow)).split("_")(0), (Bytes.toString(r._2.getRow)).split("_")(1),
      Bytes.toString(r._2.getRow),
      Bytes.toString(r._2.getValue(Bytes.toBytes(cf), Bytes.toBytes("c_3g_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(cf), Bytes.toBytes("c_4g_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(cf), Bytes.toBytes("c_all_cnt")))
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
    val hbaseOnlineTablePrefix = sc.getConf.get("spark.app.hbaseOnlineTablePrefix") // iot_online_users_
    val targetdayid = sc.getConf.get("spark.app.htable.targetdayid")

    // 将每天的Hbase数据union all后映射为一个DataFrame
    var hbaseDF: DataFrame = null
    import sqlContext.implicits._
    for (i <- 0 until intervalDayNums) {
      val dayid = DateUtils.timeCalcWithFormatConvertSafe(endDayid, "yyyyMMdd", -i * 24 * 60 * 60, "yyyyMMdd")
      if (i == 0) {
        hbaseDF = registerRDD(sc, hbaseOnlineTablePrefix + dayid).toDF()
      }
      if (i > 0) {
        hbaseDF = hbaseDF.unionAll(registerRDD(sc, hbaseOnlineTablePrefix + dayid).toDF())
      }
    }

    val tmpTable = "tmpHbaseOnline_" + endDayid
    hbaseDF.registerTempTable(tmpTable)

    //  对每个5分钟点的流量排序, 删除最大值和最小值后取平均值
    val tmpSql =
      s"""select companycode, time, companyAndTime,
         |nvl(user3gnum,0) user3gnum, nvl(user4gnum,0) user4gnum,
         |nvl(usernum,0) usernum,
         |row_number() over(partition by companycode, time order by usernum) usernumRN
         |from ${tmpTable}
       """.stripMargin

    val tmpDF = sqlContext.sql(tmpSql).cache()

    var reqDF = tmpDF.filter("usernumRN>1").filter("usernumRN<" + intervalDayNums)


    if (intervalDayNums <= 2) {
      reqDF = tmpDF
    }

    val reqAvgDF = reqDF.groupBy("companyAndTime").agg(avg("usernum").as("usernum")).coalesce(1)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)

    val targetHtable = hbaseOnlineTablePrefix + targetdayid
    val cf = "info"
    val families = new Array[String](1)
    families(0) = cf
    HbaseUtils.createIfNotExists(targetHtable, families)


    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, targetHtable)

    val reqAvgRdd = reqAvgDF.rdd.map(x => (x.getString(0), x.getDouble(1)))

    val resultRdd = reqAvgRdd.map { arr => {
      val currentPut = new Put(Bytes.toBytes(arr._1))
      currentPut.addColumn(Bytes.toBytes(cf), Bytes.toBytes("b_all_cnt"), Bytes.toBytes(arr._2.toString))
      (new ImmutableBytesWritable, currentPut)
    }
    }

    resultRdd.saveAsHadoopDataset(jobConf)

    sc.stop()


  }

}
