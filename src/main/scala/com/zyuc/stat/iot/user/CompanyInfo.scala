package com.zyuc.stat.iot.user

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
  * Created by zhoucw on 17-8-29.
  * 1000 200 200C
  * @deprecated
  */
object CompanyInfo extends Logging{

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val userTablePartitionID = sc.getConf.get("spark.app.table.userTablePartitionDayID")
    val userTable = sc.getConf.get("spark.app.table.userTable") //"iot_customer_userinfo"
    val companyTable = sc.getConf.get("spark.app.table.companyTable") //"iot_basic_company"
    val ifUpdateLatestInfo = sc.getConf.get("spark.app.ifUpdateLatestInfo","1") //  是否更新iot_basic_companyinfo的latestdate和cnt_latest, 0-不更新， 1-更新
    val appName = sc.getConf.get("spark.app.name")

    val userCompanyTmpTable = appName + "_userCompanyTmpTable"
    val userCompanyDF = sqlContext.sql(
      s"""select vpdncompanycode as companycode, count(*) as usernum
         |from $userTable
         |where d='$userTablePartitionID'
         |group by  vpdncompanycode
       """.stripMargin).registerTempTable(userCompanyTmpTable)


    val resultDF = sqlContext.sql(
      s"""select nvl(c.province,'-1') province, nvl(c.provincecode,'-1') provincecode,
         |nvl(c.companycode,'-1') companycode, nvl(c.companyName,'-1') companyName,
         |nvl(c.domain,'-1') domain, nvl(u.usernum,0) usernum,
         |(case when u.usernum>=1000 then 'A' when u.usernum>=200 then 'B' else 'C' end) monilevel
         |from  $companyTable c left join   $userCompanyTmpTable u
         |on(u.companycode=c.companycode)
       """.stripMargin)



    val htable = "iot_basic_companyinfo"
    // 如果h表不存在， 就创建
    // hbase配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    val connection = ConnectionFactory.createConnection(conf)
    val families = new Array[String](2)
    families(0) = "basicinfo"
    families(1) = "cardcnt"
    // 创建表, 如果表存在， 自动忽略
    HbaseUtils.createIfNotExists(htable, families)
    val companyJobConf = new JobConf(conf, this.getClass)
    companyJobConf.setOutputFormat(classOf[TableOutputFormat])
    companyJobConf.set(TableOutputFormat.OUTPUT_TABLE, htable)

    // 预警表
    val alarmChkTable = "analyze_rst_tab"
    val alarmChkJobConf = new JobConf(conf, this.getClass)
    alarmChkJobConf.setOutputFormat(classOf[TableOutputFormat])
    alarmChkJobConf.set(TableOutputFormat.OUTPUT_TABLE, alarmChkTable)

    val companyHbaseRDD = resultDF.coalesce(1).rdd.map(x => (x.getString(0),x.getString(1),x.getString(2),x.getString(3),x.getString(4), x.getLong(5),x.getString(6)))
    val companyRDD = companyHbaseRDD.map { arr => {
      val curPut = new Put(Bytes.toBytes(arr._3))
      val alarmPut = new Put(Bytes.toBytes(arr._3))

      curPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("province"), Bytes.toBytes(arr._1.toString))
      curPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincecode"), Bytes.toBytes(arr._2.toString))
      curPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("companyname"), Bytes.toBytes(arr._4.toString))
      curPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("domain"), Bytes.toBytes(arr._5.toString))
      curPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("monilevel_autocalc"), Bytes.toBytes(arr._7.toString))


      if(ifUpdateLatestInfo == "1"){
        curPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_latest"), Bytes.toBytes(arr._6.toString))
        curPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("date_latest"), Bytes.toBytes(userTablePartitionID))
      }
      curPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_" + userTablePartitionID), Bytes.toBytes(arr._6.toString))

      alarmPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("usernum"), Bytes.toBytes(arr._6.toString))

      ((new ImmutableBytesWritable, curPut), (new ImmutableBytesWritable, alarmPut))
    }
    }
    companyRDD.map(_._1).saveAsHadoopDataset(companyJobConf)
    companyRDD.map(_._2).saveAsHadoopDataset(alarmChkJobConf)

  }

}
