package com.zyuc.stat.properties

import scala.xml.XML

/**
  * Created by slview on 17-6-7.
  */
object RedisProperties {
  val REDIS_SERVER: String = getRedis._1
  val REDIS_PORT: Int = getRedis._2
  val REDIS_PASSWORD: String = getRedis._3

  def getRedis(): Tuple3[String, Int, String] = {
    var REDIS_SERVER = ""
    var REDIS_PORT = ""
    var REDIS_PASSWORD = ""
    val someXML = XML.load("/ymqoe/nms/cfg/shconfig.xml")
    val headerField = someXML \ "ParaNode"
    headerField.foreach { x =>
      val node = (x \\ "ParaNode").foreach { child =>
        val paraname = (child \\ "ParaName").text
        if (paraname.equals("RedisIP")) {
          REDIS_SERVER = (child \\ "ParaValue").text
        } else if (paraname.equals("RedisPort")) {
          REDIS_PORT = (child \\ "ParaValue").text
        } else if (paraname.equals("RedisPassword")) {
          REDIS_PASSWORD = (child \\ "ParaValue").text
        }
      }
    }
    val tuple: (String, Int, String) = Tuple3(REDIS_SERVER, REDIS_PORT.toInt, REDIS_PASSWORD)
    return tuple
  }
}

