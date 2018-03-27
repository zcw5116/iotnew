package com.zyuc.stat.iot.service

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{Logging, SparkContext}

object SMSCDataService extends Logging{
  def saveRddData(smscDF:DataFrame, starttimeid:String): Unit = {
    // 分钟级别 比如 8点20转换为： 0820
    val startM5 = starttimeid.substring(8, 12)
    val endM5 = DateUtils.timeCalcWithFormatConvertSafe(startM5, "HHmm", 5 * 60, "HHmm")
    val dayid = starttimeid.substring(0, 8)

    val hbaseTable = "iot_smsc_stat_" + dayid
    var firstfamily =  "smscinfo"
    val families = new Array[String](1)
    families(0) = firstfamily
    HbaseUtils.createIfNotExists(hbaseTable, families)

    val conf = HbaseUtils.getHbaseConf(ConfigProperties.IOT_ZOOKEEPER_QUORUM, ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    val smscJobConf = new JobConf(conf, this.getClass)
    smscJobConf.setOutputFormat(classOf[TableOutputFormat])
    smscJobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable)

    val hbaserdd = smscDF.rdd.map(x => (x.getString(0), x.getString(1), x.getDouble(2)))
    val smscrdd = hbaserdd.map { arr => {
        /*一个Put对象就是一行记录，在构造方法中指定主键
         * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
         * Put.add方法接收三个参数：列族，列名，数据
         */
         val smscPut = new Put(Bytes.toBytes(arr._1+"-" + startM5.toString))
         smscPut.addColumn(Bytes.toBytes(firstfamily), Bytes.toBytes("s_smsc_calledCount"), Bytes.toBytes(arr._2.toString))
         smscPut.addColumn(Bytes.toBytes(firstfamily), Bytes.toBytes("s_smsc_callingCount"), Bytes.toBytes(arr._3.toString))
          //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
         (new ImmutableBytesWritable, smscPut)
      }
    }
    smscrdd.saveAsHadoopDataset(smscJobConf)
  }
}
