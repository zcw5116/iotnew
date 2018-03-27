package com.zyuc.stat.iot.operalog

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.HbaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by slview on 17-6-17.
  */
object OperaHbaseAnalysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()// .setAppName("OperalogAnalysis").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val operaDay = sc.getConf.get("spark.app.operaDay") // "20170801"
    val operaTable = sc.getConf.get("spark.app.table.operaTable") // iot_opera_log
    val appName =  sc.getConf.get("spark.app.name")
    // userTable
    val userTable = sc.getConf.get("spark.app.user.tableName")     // "iot_customer_userinfo"
    val userTablePartitionDayid = sc.getConf.get("spark.app.user.userTablePartitionDayid")  //  "20170807"



    val operaPartitionD = operaDay.substring(2,8)
    val cachedOperaTable = s"iot_opera_log_cached_$operaDay"
    sqlContext.sql(
      s"""CACHE TABLE ${cachedOperaTable} as
         |select l.mdn, l.logtype, l.opername, u.custprovince,
         |(case when length(u.vpdncompanycode)=0 or u.vpdncompanycode is null then 'N999999999' else u.vpdncompanycode end) vpdncompanycode
         |from (
         |      select mdn, logtype, opername
         |      from ${operaTable}
         |      where opername in('open','close') and oper_result='成功'
         |      and length(mdn)>0 and d = '${operaPartitionD}'
         |      ) l left join
         |      (select mdn, custprovince,
         |       vpdncompanycode
         |       from  ${userTable}
         |       where d='${userTablePartitionDayid}'
         |      ) u
         |on (l.mdn=u.mdn)
       """.stripMargin)

    val resultDF = sqlContext.sql(
      s"""select vpdncompanycode, logtype,
         |sum(case when opername='open' then 1 else 0 end) as opennum,
         |sum(case when opername='close' then 1 else 0 end) as closenum
         |from $cachedOperaTable
         |group by vpdncompanycode, logtype
       """.stripMargin).coalesce(1)


    val hbaseTable = "iot_operalog_day"
    val year = operaDay.substring(0,4)
    val families = new Array[String](1)
    families(0) = "operainfo_" + year
    HbaseUtils.createIfNotExists(hbaseTable,families)

    val conf = HbaseUtils.getHbaseConf( ConfigProperties.IOT_ZOOKEEPER_QUORUM, ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    val operaJobConf = new JobConf(conf, this.getClass)
    operaJobConf.setOutputFormat(classOf[TableOutputFormat])
    operaJobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable)

    val operaRDD = resultDF.rdd.map(x => (x.getString(0), x.getString(1), x.getLong(2), x.getLong(3)))
    val rowFamily = families(0)
    val operaHbaseRdd = operaRDD.map { arr => {
      val currentPut = new Put(Bytes.toBytes(arr._1))
      currentPut.addColumn(Bytes.toBytes(rowFamily), Bytes.toBytes(arr._2 + "_opennum_" + operaDay.toString), Bytes.toBytes(arr._3.toString))
      currentPut.addColumn(Bytes.toBytes(rowFamily), Bytes.toBytes(arr._2 + "_closenum_" + operaDay.toString), Bytes.toBytes(arr._4.toString))
      //currentPut.addColumn(Bytes.toBytes("operainfo"), Bytes.toBytes(arr._1 + "_"+arr._3+"_cnt"), Bytes.toBytes(arr._4.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }
    operaHbaseRdd.saveAsHadoopDataset(operaJobConf)

    sc.stop()
  }

}
