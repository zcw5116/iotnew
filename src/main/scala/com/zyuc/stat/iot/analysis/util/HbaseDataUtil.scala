package com.zyuc.stat.iot.analysis.util

import com.zyuc.stat.properties.ConfigProperties
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Desc: 数据与hbase接口
  * @author zhoucw
  * @version 1.0
  */
object HbaseDataUtil {

  /**
    * Desc: 将RDD数据写入hbase表
    * @author zhoucw
    * @param htable     数据写入hbase的表名
    * @param rdd        需要保存到hbase表的RDD
    */
  def saveRddToHbase(htable: String, rdd:RDD[(ImmutableBytesWritable,Put)]):Unit={

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)

    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, htable)


    rdd.saveAsHadoopDataset(jobConf)
  }

  def htableToDataFrame(htable:String) :DataFrame = {

    null
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("name")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val abc = new Array[String](3)
    abc(0)= "a,2"
    abc(1) = "b,3"
    abc(2) = "c,5"

    val rdd = sc.parallelize(abc).repartition(1)

    rdd.foreachPartition(data=>{
      data.foreach(x=>{
        val conf = HBaseConfiguration.create()
        //conf.set("hbase.zookeeper.property.clientPort", "12181")
        // conf.set("hbase.zookeeper.quorum", "spark123")
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.zookeeper.quorum", "inmnmbd03,inmnmbd04")

        conf.set("java.security.krb5.realm", "CHINATELECOM.CN")
        System.setProperty("java.security.krb5.conf", "/slview/jdk/jre/lib/security/krb5.conf")///slview/jdk/jre/lib/security/krb5.conf
        conf.set("hbase.security.authentication", "kerberos")
        conf.set("hadoop.security.authentication", "kerberos")

        conf.set("hbase.master.kerberos.principal", "hbase/_HOST@CHINATELECOM.CN")
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@CHINATELECOM.CN")
        UserGroupInformation.setConfiguration(conf)
        UserGroupInformation.loginUserFromKeytab("epciot/chinatelecom@CHINATELECOM.CN", "/slview/nms/cfg/epciot.keytab")

        val namespace = "epciot"
        val htable = "mytest"
        val conn = ConnectionFactory.createConnection(conf)
        val tableName = TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(htable))
        val table = conn.getTable(tableName)
        val rowkey = x.split(",")(0)
        val value =  x.split(",")(1)
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("myc"), Bytes.toBytes(x))
        table.put(put)
        conn.close()
      })

    })


    def save2Hbase(data:Iterator[String]) = {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", "12181")
      conf.set("hbase.zookeeper.quorum", "spark123")

     /* conf.set("java.security.krb5.realm", "CHINATELECOM.CN")
      System.setProperty("java.security.krb5.conf", "/slview/nms/cfg/krb5.conf")
      conf.set("hbase.security.authentication", "kerberos")
      conf.set("hadoop.security.authentication", "kerberos")

      conf.set("hbase.master.kerberos.principal", "hbase/_HOST@CHINATELECOM.CN")
      conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@CHINATELECOM.CN")
      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab("epciot/chinatelecom@CHINATELECOM.CN", "/slview/nms/cfg/epciot.keytab")*/

      val namespace = "epciot"
      val htable = "mytest"
      val conn = ConnectionFactory.createConnection(conf)
      val tableName = TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(htable))
      val table = conn.getTable(tableName)

      data.foreach(x=>{
        val rowkey = x.split(",")(0)
        val value =  x.split(",")(1)
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("myc"), Bytes.toBytes(x))
        table.put(put)
      })
      conn.close()

    }




  }
}
