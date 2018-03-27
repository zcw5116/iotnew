package test

import com.zyuc.stat.iot.online.OnlineUser.conf
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{CharacterEncodeConversion, HbaseUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-8-31.
  */
object TestHbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("OperalogAnalysis").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val srcDF = CharacterEncodeConversion.transfer(sc, "/tmp/iot_basestation_info.txt", "GBK").map(x=>x.split("\\|\\^\\|",4))
    val rdd = srcDF.map(x=>(x(0), x(1), x(2),x(3)))

    val htable="iot_basic_basestation"
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)

    // 如果h表不存在， 就创建
    val connection = ConnectionFactory.createConnection(conf)
    val families = new Array[String](1)
    families(0) = "basicinfo"



    // 创建表, 如果表存在， 自动忽略
    HbaseUtils.createIfNotExists(htable, families)
    val bsJobConf = new JobConf(conf, this.getClass)
    bsJobConf.setOutputFormat(classOf[TableOutputFormat])
    bsJobConf.set(TableOutputFormat.OUTPUT_TABLE, htable)

    val bsRDD = rdd.map { arr => {
      val bsPut1 = new Put(Bytes.toBytes(arr._1 + "_" + arr._2))
      val bsPut2 = new Put(Bytes.toBytes(arr._1))
      bsPut1.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("bsid"), Bytes.toBytes(arr._1.toString))
      bsPut1.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("province"), Bytes.toBytes(arr._2.toString))
      bsPut1.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("bsname"),Bytes.toBytes((arr._3).toString))
      bsPut1.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("vendor"),Bytes.toBytes((arr._4).toString))
      bsPut2.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("bsid"), Bytes.toBytes(arr._1))
      bsPut2.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("province"), Bytes.toBytes(arr._2))
      bsPut2.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("bsname"),Bytes.toBytes((arr._3).toString))
      bsPut2.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("vendor"),Bytes.toBytes((arr._4).toString))

      ((new ImmutableBytesWritable, bsPut1),(new ImmutableBytesWritable, bsPut2))
    }
    }
    bsRDD.map(_._1).saveAsHadoopDataset(bsJobConf)
    bsRDD.map(_._2).saveAsHadoopDataset(bsJobConf)
  }

}

