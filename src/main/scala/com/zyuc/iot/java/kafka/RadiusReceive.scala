package com.zyuc.iot.java.kafka

import java.io.InputStream
import com.zyuc.stat.singleton.IotSourceKafkaDealSingleton
import com.zyuc.stat.tools.GetProperties
import com.zyuc.stat.utils.SparkKafkaUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex
import scala.util.parsing.json.JSON

/**
  * Created by wangpf on 2017/6/20.
  * desc:数据匹配case class
  */
case class haradius_out(BSID: String, CDMAIMSI: String, CorrelationID: String, Duration: String,
                        HAServiceAddress: String, IPAddr: String, InputOctets: String, MDN: String,
                        NetType: String, OutputOctets: String, RecvTime: String, ServiceOption: String, ServicePCF: String, SessionID: String,time: String,
                        Status: String,UserName: String, TerminateCause: String,
                        dayid: String)

/**
  * Created by wangpf on 2017/6/20.
  * desc:使用streaming实时清洗上下线信息入hive表
  */
object RadiusReceive extends GetProperties {
  override def inputStreamArray: Array[InputStream] = Array(
    this.getClass.getClassLoader.getResourceAsStream("kafka.proerties")
  )

  // 获取配置文件的内容
  private val prop = props

  def main(args: Array[String]) {
    // 创建StreamingContext
    // 创建上下文
    val sparkConf = new SparkConf()

    val sc = new SparkContext(sparkConf)

    val groupName = sc.getConf.get("spark.app.groupName", "haradiusToHive2")

    val ssc = new StreamingContext(sc, Seconds(300))
    // 创建stream时使用的topic名字集合
    val topics: Set[String] = Set("haradius_out")
    // zookeeper的host和ip,创建一个client
    val zkClient = new ZkClient(prop.getProperty("kafka.zookeeper.list"))
    // 配置信息
    val kafkaParams = Map[String, String]("metadata.broker.list" -> prop.getProperty("kafka.metadata.broker.list"))

    // 获取kafkaStream
    val kafkaStream = SparkKafkaUtils.createDirectKafkaStream(ssc, kafkaParams, zkClient, topics, groupName)

    // 创建hiveContext
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    //设置参数开启 动态分区（dynamic partition）
    hiveContext.sql("set hive.exec.dynamic.partition.mode = nonstrict")
    hiveContext.sql("set hive.exec.dynamic.partition = true")
    kafkaStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.mapPartitions( partition => {
          val data = partition.map(x => {
            val json = JSON.parseFull(x._2)

            json match {
              // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
              case Some(map: Map[String, String]) => {
                val Status = getMapData(map, "Status")
                val Time =
                  if (Status == "Start")
                    parseTime(IotSourceKafkaDealSingleton.getTimeType, getMapData(map, "StartTime"))
                  else if (Status == "Stop")
                    parseTime(IotSourceKafkaDealSingleton.getTimeType, getMapData(map, "StopTime"))
                  else
                    "-1"

                val dayid = if (Time.length >= 8) Time.substring(0, 8) else "-1"

                haradius_out(getMapData(map, "BSID"), getMapData(map, "CDMAIMSI"),
                  getMapData(map, "CorrelationID"), getMapData(map, "Duration"),
                  getMapData(map, "HAServiceAddress"), getMapData(map, "IPAddr"),
                  getMapData(map, "InputOctets"), getMapData(map, "MDN"),
                  getMapData(map, "NetType"), getMapData(map, "OutputOctets"),
                  getMapData(map, "RecvTime"), getMapData(map, "ServiceOption"),
                  getMapData(map, "ServicePCF"), getMapData(map, "SessionID"),
                  Time,getMapData(map, "Status"),
                  getMapData(map, "UserName"), getMapData(map, "TerminateCause"),
                  dayid)
              }
              case other => null
            }
          })
          data
        })
          .filter(_ != null)
          //      .coalesce(1)
          .toDF()
          .registerTempTable("registerTempTable_haradius_out")

        hiveContext.sql("insert into iot.iot_radius_ha partition(dayid) " +
          "select " +
          " BSID, " +
          " CDMAIMSI, " +
          " CorrelationID, " +
          " Duration, " +
          " HAServiceAddress, " +
          " IPAddr, " +
          " InputOctets, " +
          " MDN, " +
          " NetType, " +
          " OutputOctets, " +
          " RecvTime, " +
          " ServiceOption, " +
          " ServicePCF, " +
          " SessionID, " +
          " time, " +
          " Status, " +
          " UserName, " +
          " TerminateCause, " +
          " dayid " +
          "from registerTempTable_haradius_out")

        SparkKafkaUtils.saveOffsets(zkClient, groupName, rdd)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Created by wangpf on 2017/6/20.
    * desc:查看Map中是否存在某一个key，若不存在将其值置为-1
    */
  def getMapData(map: Map[String, String], key: String): String = {
    if (map.contains(key))
      map(key)
    else
      "-1"
  }

  /**
    * Created by wangpf on 2017/6/20.
    * desc:解析kafka中发送的字符串由 YYYY-MM-DD HH24:MI:SS转成YYYYMMDDHH24MISS
    */
  def parseTime(timeType: Regex, timevale: String): String = {
    timevale match {
      case timeType(a, b, c, d, e, f) => return a + b + c + d + e + f
      case other => return "-1"
    }
  }
}
