package test

/**
  * Created by zhoucw on 17-9-25.
  */

import com.google.common.collect.Lists
import com.google.common.collect.Maps
import kafka.api.PartitionOffsetRequestInfo
import kafka.cluster.BrokerEndPoint
import kafka.common.TopicAndPartition
import kafka.javaapi._
import kafka.javaapi.consumer.SimpleConsumer
import java.util.Date
import java.util

import com.zyuc.iot.java.kafka.RadiusReceive.prop
import org.apache.spark.streaming.kafka._


/**
  * KafkaOffsetTool
  *
  */
object KafkaOffsetTool {

  val kafkaParams = Map[String, String]("metadata.broker.list" -> "")



   var instance:KafkaOffsetTool = null
  def getInstance: KafkaOffsetTool = {
    if (instance == null) instance = new KafkaOffsetTool
    instance
  }

  def main(args: Array[String]): Unit = {
    val topics = Lists.newArrayList[String]
    topics.add("haradius_out")
    //    topics.add("bugfix");
    val topicAndPartitionLongMap = KafkaOffsetTool.getInstance.getEarliestOffset("10.37.7.139:9092,10.37.7.140:9092,10.37.7.141:9092", topics, "haradiusToHive")
    val topicAndPartitionLongMap1 = KafkaOffsetTool.getInstance.getLastOffset("10.37.7.139:9092,10.37.7.140:9092,10.37.7.141:9092", topics, "haradiusToHive")
    import scala.collection.JavaConversions._
    for (entry <- topicAndPartitionLongMap.entrySet) {
      System.out.println(entry.getKey.topic + "-" + entry.getKey.partition + ":" + entry.getValue)
    }

    import kafka.common.TopicAndPartition
    val lastestTopicAndPartitionLongMap = KafkaOffsetTool.getInstance.getLastOffset("10.37.7.139:9092,10.37.7.140:9092,10.37.7.141:9092", topics, "haradiusToHive")

    // earliest offsets
    val earliestTopicAndPartitionLongMap = KafkaOffsetTool.getInstance.getEarliestOffset("10.37.7.139:9092,10.37.7.140:9092,10.37.7.141:9092", topics, "haradiusToHive")


    import scala.collection.JavaConversions._


  }
}

class KafkaOffsetTool {
  final val TIMEOUT = 100000
  final val BUFFERSIZE = 64 * 1024

  def getLastOffset(brokerList: String, topics: util.List[String], groupId: String): util.Map[TopicAndPartition, Long] = {
    val topicAndPartitionLongMap = Maps.newHashMap[TopicAndPartition, Long]
    val topicAndPartitionBrokerMap = findLeader(brokerList, topics)
    import scala.collection.JavaConversions._
    for (topicAndPartitionBrokerEntry <- topicAndPartitionBrokerMap.entrySet) { // get leader broker
      val leaderBroker = topicAndPartitionBrokerEntry.getValue
      val simpleConsumer = new SimpleConsumer(leaderBroker.host, leaderBroker.port, TIMEOUT, BUFFERSIZE, groupId)
      val readOffset = getTopicAndPartitionLastOffset(simpleConsumer, topicAndPartitionBrokerEntry.getKey, groupId)
      topicAndPartitionLongMap.put(topicAndPartitionBrokerEntry.getKey, readOffset)
    }
    topicAndPartitionLongMap
  }

  /**
    *
    * @param brokerList
    * @param topics
    * @param groupId
    * @return
    */
  def getEarliestOffset(brokerList: String, topics: util.List[String], groupId: String): util.Map[TopicAndPartition, Long] = {
    val topicAndPartitionLongMap = Maps.newHashMap[TopicAndPartition, Long]
    val topicAndPartitionBrokerMap = findLeader(brokerList, topics)
    import scala.collection.JavaConversions._
    for (topicAndPartitionBrokerEntry <- topicAndPartitionBrokerMap.entrySet) {
      val leaderBroker = topicAndPartitionBrokerEntry.getValue
      val simpleConsumer = new SimpleConsumer(leaderBroker.host, leaderBroker.port, TIMEOUT, BUFFERSIZE, groupId)
      val readOffset = getTopicAndPartitionEarliestOffset(simpleConsumer, topicAndPartitionBrokerEntry.getKey, groupId)
      topicAndPartitionLongMap.put(topicAndPartitionBrokerEntry.getKey, readOffset)
    }
    topicAndPartitionLongMap
  }

  /**
    * 得到所有的 TopicAndPartition
    *
    * @param brokerList
    * @param topics
    * @return topicAndPartitions
    */
   def findLeader(brokerList: String, topics: util.List[String]) = { // get broker's url array
    val brokerUrlArray = getBorkerUrlFromBrokerList(brokerList)
    // get broker's port map
    val brokerPortMap = getPortFromBrokerList(brokerList)
    // create array list of TopicAndPartition
    val topicAndPartitionBrokerMap = Maps.newHashMap[TopicAndPartition, BrokerEndPoint]
    for (broker <- brokerUrlArray) {
      var consumer:SimpleConsumer = null
      try { // new instance of simple Consumer
        consumer = new SimpleConsumer(broker, brokerPortMap.get(broker), TIMEOUT, BUFFERSIZE, "leaderLookup" + new Date().getTime)
        val req = new TopicMetadataRequest(topics)
        val resp = consumer.send(req)
        val metaData = resp.topicsMetadata
        import scala.collection.JavaConversions._
        for (item <- metaData) {
          import scala.collection.JavaConversions._
          for (part <- item.partitionsMetadata) {
            val topicAndPartition = new TopicAndPartition(item.topic, part.partitionId)
            topicAndPartitionBrokerMap.put(topicAndPartition, part.leader)
          }
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally if (consumer != null) consumer.close()
    }
    topicAndPartitionBrokerMap
  }

  /**
    * get last offset
    *
    * @param consumer
    * @param topicAndPartition
    * @param clientName
    * @return
    */
   def getTopicAndPartitionLastOffset(consumer: SimpleConsumer, topicAndPartition: TopicAndPartition, clientName: String):Long = {
    val requestInfo = new util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo]
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime, 1))
    val request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientName)
    val response = consumer.getOffsetsBefore(request)
    if (response.hasError) {
      System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topicAndPartition.topic, topicAndPartition.partition))
      return 0
    }
    val offsets = response.offsets(topicAndPartition.topic, topicAndPartition.partition)
    offsets(0)
  }

  /**
    * get earliest offset
    *
    * @param consumer
    * @param topicAndPartition
    * @param clientName
    * @return
    */
   def getTopicAndPartitionEarliestOffset(consumer: SimpleConsumer, topicAndPartition: TopicAndPartition, clientName: String) :Long = {
    val requestInfo = new util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo]
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime, 1))
    val request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientName)
    val response = consumer.getOffsetsBefore(request)
    if (response.hasError) {
      System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topicAndPartition.topic, topicAndPartition.partition))
      return 0
    }
    val offsets = response.offsets(topicAndPartition.topic, topicAndPartition.partition)
    offsets(0)
  }

  /**
    * 得到所有的broker url
    *
    * @param brokerlist
    * @return
    */
   def getBorkerUrlFromBrokerList(brokerlist: String) = {
    val brokers = brokerlist.split(",")
    var i = 0
    while ( {
      i < brokers.length
    }) {
      brokers(i) = brokers(i).split(":")(0)

      {
        i += 1; i - 1
      }
    }
    brokers
  }

  /**
    * 得到broker url 与 其port 的映射关系
    *
    * @param brokerlist
    * @return
    */
   def getPortFromBrokerList(brokerlist: String) = {
    val map = new util.HashMap[String, Integer]
    val brokers = brokerlist.split(",")
    for (item <- brokers) {
      val itemArr = item.split(":")
      if (itemArr.length > 1) map.put(itemArr(0), itemArr(1).toInt)
    }
    map
  }
}