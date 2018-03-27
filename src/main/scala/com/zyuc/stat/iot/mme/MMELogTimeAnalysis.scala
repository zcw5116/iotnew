package com.zyuc.stat.iot.mme

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils.timeCalcWithFormatConvertSafe
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import com.zyuc.stat.utils.MathUtil.divOpera

/**
  * Created by zhoucw on 17-7-24.
  */
object MMELogTimeAnalysis extends Logging{
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    // hbase配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)

    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val m5timeid = sc.getConf.get("spark.app.m5timeid") // 201707211645
    val ifUpdatelarmFlag = sc.getConf.get("spark.app.ifUpdatelarmFlag","1")
    val userTablePartitionID = sc.getConf.get("spark.app.table.userTablePartitionDayID")
    val userTable = sc.getConf.get("spark.app.table.userTable") //"iot_customer_userinfo"


    var ifUpdatealarmChk = true
    if(ifUpdatelarmFlag=="0"){
      ifUpdatealarmChk = false
    } else {
      ifUpdatealarmChk = true
    }

    val partdayid = m5timeid.substring(2,8) // 170721
    val hourid = m5timeid.substring(8,10) // 16
    val m5id = m5timeid.substring(10,12) // 45

    val dayid = m5timeid.substring(0,8)

    // 根据开始时间获取300秒后的时间字符串
    val endm5timeid = timeCalcWithFormatConvertSafe(m5timeid, "yyyyMMddHHmm", 300, "yyyyMMddHHmm")

    // for hbase rowkey
    val curHourM5 = m5timeid.substring(8,12) // 201707241500=> 1500
    val nextHourM5 = endm5timeid.substring(8,12) //  => 1505

    val cachedUserinfoTable = "iot_user_basic_info_cached"
    sqlContext.sql(
      s"""
         |CACHE TABLE ${cachedUserinfoTable} as
         |select u.mdn,case when length(u.vpdncompanycode)=0 then 'N999999999' else u.vpdncompanycode end  as vpdncompanycode
         |from ${userTable} u where d=${userTablePartitionID}
       """.stripMargin).coalesce(1)

    val mmeGroupsql =
      s"""select u.vpdncompanycode,sum(req_cnt) as req_cnt,
         |sum(case when result="success" then req_cnt else 0 end) as reqsucess_cnt,
         |count(distinct t.mdn) as req_mdncnt,
         |sum(case when result="failed" then 1 else 0 end) as req_mdnfailedcnt
         |from
         |    (
         |        select m.msisdn as mdn, m.result,count(*) as req_cnt
         |        from iot_mme_log m
         |        where m.d=${partdayid} and m.h=${hourid} and m.m5 = ${m5id}
         |              and m.isattach=1
         |        group by m.msisdn, m.result
         |    ) t, ${cachedUserinfoTable} u
         |where t.mdn = u.mdn
         |group by u.vpdncompanycode
       """.stripMargin


    logInfo(s"mmeGroupsql :  $mmeGroupsql ")

    val mmeGroupDF = sqlContext.sql(mmeGroupsql).coalesce(1)

    val htable = "iot_mme_day_" + dayid
    // 如果h表不存在， 就创建
    val connection = ConnectionFactory.createConnection(conf)
    val families = new Array[String](2)
    families(0) = "mmeinfo"
    families(1) = "mmefailed"
    // 创建表, 如果表存在， 自动忽略
    HbaseUtils.createIfNotExists(htable,families)
    val authJobConf = new JobConf(conf, this.getClass)
    authJobConf.setOutputFormat(classOf[TableOutputFormat])
    authJobConf.set(TableOutputFormat.OUTPUT_TABLE, htable)


    // 预警表
    val alarmChkTable = "analyze_rst_tab"
    val alarmChkJobConf = new JobConf(conf, this.getClass)
    alarmChkJobConf.setOutputFormat(classOf[TableOutputFormat])
    alarmChkJobConf.set(TableOutputFormat.OUTPUT_TABLE, alarmChkTable)


    // type, vpdncompanycode, authcnt, successcnt, failedcnt, authmdnct, authfaieldcnt
    val hbaserdd = mmeGroupDF.rdd.map(x => (x.getString(0), x.getLong(1), x.getLong(2), x.getLong(3),
      x.getLong(4), divOpera(x.getLong(2).toString,x.getLong(1).toString) ))

    // 当前窗口的mme日志
    val mmecurrentrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val currentPut = new Put(Bytes.toBytes(arr._1 + "-" + curHourM5.toString))
      val alarmPut = new Put(Bytes.toBytes(arr._1))

      currentPut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("c_req_cnt"), Bytes.toBytes(arr._2.toString))
      currentPut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("c_req_sucesscnt"), Bytes.toBytes(arr._3.toString))
      currentPut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("c_req_mdncnt"), Bytes.toBytes(arr._4.toString))
      currentPut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("c_req_mdnfailedcnt"), Bytes.toBytes(arr._5.toString))

      alarmPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("mme_c_time"), Bytes.toBytes(m5timeid))
      alarmPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("mme_c_ratio"), Bytes.toBytes(arr._6))

      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      ((new ImmutableBytesWritable, currentPut), (new ImmutableBytesWritable, alarmPut))
    }
    }
    mmecurrentrdd.map(_._1).saveAsHadoopDataset(authJobConf)
    if(ifUpdatealarmChk){
      mmecurrentrdd.map(_._2).saveAsHadoopDataset(alarmChkJobConf)
    }


    // 下一个时间窗口的mme日志
    val mmenextrdd = hbaserdd.map { arr => {
      val nextPut = new Put(Bytes.toBytes(arr._1 + "-" + nextHourM5.toString))
      nextPut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("p_req_cnt"), Bytes.toBytes(arr._2.toString))
      nextPut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("p_req_sucesscnt"), Bytes.toBytes(arr._3.toString))
      nextPut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("p_req_mdncnt"), Bytes.toBytes(arr._4.toString))
      nextPut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("p_req_mdnfailedcnt"), Bytes.toBytes(arr._5.toString))
      (new ImmutableBytesWritable, nextPut)
    }
    }
    mmenextrdd.saveAsHadoopDataset(authJobConf)


    // 失败原因
    val failedSql =
      s"""select  u.vpdncompanycode, m.pcause, count(*) as req_cnt
         |from iot_mme_log m, ${cachedUserinfoTable} u
         |where m.d=${partdayid} and m.h=${hourid} and m.m5 = ${m5id}
         |      and m.msisdn = u.mdn and m.result<>'success' and m.isattach=1
         |group by u.vpdncompanycode, m.pcause
       """.stripMargin

    // type, vpdncompanycode, auth_result, authcnt, authrank
    val failedrdd = sqlContext.sql(failedSql).coalesce(1).rdd.map(x => (x.getString(0), x.getString(1), x.getLong(2)))

    val curfailedRdd = failedrdd.map { arr => {
      val currentPut = new Put(Bytes.toBytes(arr._1.toString + "-" + curHourM5.toString))
      currentPut.addColumn(Bytes.toBytes("mmefailed"), Bytes.toBytes("f_" + arr._2 + "_cnt"), Bytes.toBytes(arr._3.toString))
      (new ImmutableBytesWritable, currentPut)
    }
    }
    curfailedRdd.saveAsHadoopDataset(authJobConf)



    val groupsql =
      s""" select  u.vpdncompanycode, m.province, concat(m.newgrpid,'|', m.newmmecode) as mmedev, m.enbid, count(*)  as failed_cnt
         |from iot_mme_log m, ${cachedUserinfoTable} u
         |where m.msisdn = u.mdn
         |and m.d=${partdayid} and m.h=${hourid} and m.m5 = ${m5id}
         |and m.result<>'success' and m.isattach=1
         |group by  u.vpdncompanycode, m.province, m.newgrpid, m.newmmecode, m.enbid
         |""".stripMargin

    val topnsql =
      s"""select vpdncompanycode, province, mmedev, enbid, failed_cnt,
         |row_number() over(partition by vpdncompanycode  order by failed_cnt desc) as rn
         |from  (${groupsql}) t
       """.stripMargin

    val failedsiterdd = sqlContext.sql(topnsql).coalesce(1).rdd.map(x => (x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getLong(4), x.getInt(5).toString.formatted("%6s").replaceAll(" ", "0")))

    val hfailedtable = "iot_mme_failedsite_day_" + dayid
    // 如果h表不存在， 就创建
    val failedfamilies = new Array[String](1)
    failedfamilies(0) = "mmeinfo"
    // 创建表, 如果表存在， 自动忽略

    HbaseUtils.createIfNotExists(hfailedtable,failedfamilies)

    val failedsiteJobConf = new JobConf(conf, this.getClass)
    failedsiteJobConf.setOutputFormat(classOf[TableOutputFormat])
    failedsiteJobConf.set(TableOutputFormat.OUTPUT_TABLE, hfailedtable)

    val hbaseFailedsite = failedsiterdd.map { arr => {
      val failedsitePut = new Put(Bytes.toBytes(arr._1.toString + "-" + curHourM5.toString + "-" + arr._6.toString))
      failedsitePut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("province"), Bytes.toBytes(arr._2.toString))
      failedsitePut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("mmedev"), Bytes.toBytes(arr._3.toString))
      failedsitePut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("enbid"), Bytes.toBytes(arr._4.toString))
      failedsitePut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("failed_cnt"), Bytes.toBytes(arr._5.toString))
      (new ImmutableBytesWritable, failedsitePut)
    }
    }
    hbaseFailedsite.saveAsHadoopDataset(failedsiteJobConf)

    sc.stop()

  }

}
