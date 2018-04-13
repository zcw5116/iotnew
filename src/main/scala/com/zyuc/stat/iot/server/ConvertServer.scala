package com.zyuc.stat.iot.server

/**
  * Created by zhoucw on 17-7-17.
  */
import java.io.OutputStream
import java.net.InetSocketAddress
import java.util.concurrent.Executors

import com.alibaba.fastjson.JSON
import com.sun.net.httpserver.{Headers, HttpExchange, HttpHandler, HttpServer}
import com.zyuc.stat.app.{FileManage, HDFSFiles}
import com.zyuc.stat.epc.etl.{EpcGwEtl, EpcMmeEtl}
import com.zyuc.stat.iot.etl.CDRETL.doJob
import com.zyuc.stat.iot.etl.{CDRETL, MMELogETL, MMELogETL_New, SMSCETL}
import com.zyuc.stat.iot.smsc.SMSCAnalysis
import com.zyuc.stat.properties.ConfigProperties
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ConvertServer {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()//.setAppName("ConvertServer").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val port = sc.getConf.get("spark.server.port", "9999")
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    createServer(port.toInt, sqlContext, fileSystem)
  }

  def createServer(port: Int, sqlContext: SQLContext, fileSystem: FileSystem): Unit = {

    val httpServer = HttpServer.create(new InetSocketAddress(port), 30)
    httpServer.setExecutor(Executors.newCachedThreadPool())
    httpServer.createContext("/convert", new HttpHandler() {
      override def handle(httpExchange: HttpExchange): Unit = {
        System.out.println("处理新请求:" + httpExchange.getRequestMethod + " , " + httpExchange.getRequestURI)
        var response = "正常"
        var httpCode = 200
        val requestHeaders = httpExchange.getRequestHeaders
        val contentLength = requestHeaders.getFirst("Content-length").toInt
        System.out.println("" + requestHeaders.getFirst("Content-length"))
        val inputStream = httpExchange.getRequestBody
        val data = new Array[Byte](contentLength)
        val length = inputStream.read(data)
        System.out.println("data:" + new String(data))
        val Params = JSON.parseObject(new String(data))
        val serverLine = Params.getString("serverLine")
        println("serverLine:" + serverLine)
        val responseHeaders: Headers = httpExchange.getResponseHeaders

        responseHeaders.set("Content-Type", "text/html;charset=utf-8")

        var serverInfo = ""
        // 调用SparkSQL的方法进行测试
        try {
          if (serverLine == "test") {
            sqlContext.read.format("json").load("/hadoop/zcw/tmp/zips.json").show
          }
          else if (serverLine == "mmeETL") {
            val appName = Params.getString("appName")
            val loadTime = Params.getString("loadTime")
            val inputPath = Params.getString("inputPath")
            val outputPath = Params.getString("outputPath")
            val hwmmWildcard = Params.getString("hwmmWildcard")
            val hwsmWildcard = Params.getString("hwsmWildcard")
            val ztmmWildcard = Params.getString("ztmmWildcard")
            val ztsmWildcard = Params.getString("ztsmWildcard")
            serverInfo = "未知异常"
            serverInfo = MMELogETL.doJob(sqlContext, fileSystem, appName, loadTime, inputPath, outputPath, hwmmWildcard, hwsmWildcard, ztmmWildcard, ztsmWildcard)
          }
          else if (serverLine == "mmeETL_New") {
            serverInfo = "未知异常"
            serverInfo = MMELogETL_New.doJob(sqlContext, fileSystem, Params)
          } else if (serverLine == "cdrETL") {
            val appName = Params.getString("appName")
            val loadTime = Params.getString("loadTime")
            val inputPath = Params.getString("inputPath")
            val fileWildcard = Params.getString("fileWildcard")
            val outputPath = Params.getString("outputPath")
            val coalesceSize = Params.getString("coalesceSize")
            val logType = Params.getString("logType")
            val logTableName = Params.getString("logTableName")
            serverInfo = "未知异常"
            serverInfo = CDRETL.doJob(sqlContext, fileSystem, appName, loadTime, inputPath, outputPath, fileWildcard, coalesceSize.toInt, logType, logTableName)
          } else if (serverLine == "smssETL") {
            val appName = Params.getString("appName")
            val loadTime = Params.getString("loadTime")
            val inputPath = Params.getString("inputPath")
            val fileWildcard = Params.getString("fileWildcard")
            val outputPath = Params.getString("outputPath")
            val coalesceSize = Params.getString("coalesceSize")
            val logType = Params.getString("logType")
            val logTableName = Params.getString("logTableName")
            serverInfo = "未知异常"
            serverInfo = SMSCETL.doJob(sqlContext, fileSystem, appName, loadTime, inputPath, outputPath, fileWildcard, coalesceSize.toInt, logType, logTableName)
          }else if(serverLine == "epcGwETL"){
            val appName = Params.getString("appName")
            val loadTime = Params.getString("loadTime")
            val inputPath = Params.getString("inputPath")
            val mdnSectionPath = Params.getString("mdnSectionPath")
            val gwTableName = Params.getString("gwTableName")
            val outputPath = Params.getString("outputPath")
            val gwType = Params.getString("gwType")
            serverInfo = "未知异常"
            serverInfo = EpcGwEtl.doEpcGwEtl(sqlContext, loadTime, inputPath, outputPath, mdnSectionPath, gwTableName, gwType )
          } else if(serverLine == "epcMmeETL"){
            val appName = Params.getString("appName")
            val loadTime = Params.getString("loadTime")
            val inputPath = Params.getString("inputPath")
            val mdnSectionPath = Params.getString("mdnSectionPath")
            val mmeTableName = Params.getString("mmeTableName")
            val outputPath = Params.getString("outputPath")
            val ztMMWildcard = Params.getString("ztMMWildcard")
            val ztSMWildcard = Params.getString("ztSMWildcard")
            val ztPagingWildcard = Params.getString("ztPagingWildcard")
            serverInfo = "未知异常"
            serverInfo = EpcMmeEtl.doEpcMmeEtl(sqlContext, loadTime, inputPath, outputPath, mdnSectionPath, mmeTableName, ztMMWildcard, ztSMWildcard, ztPagingWildcard)
          }
          else if(serverLine == "mergeFiles"){
            val appName = Params.getString("appName")
            val batchTime = Params.getString("batchTime")
            val inputPath = Params.getString("inputPath")
            val childPath = Params.getString("childPath")
            val mergePath = Params.getString("mergePath")
            serverInfo = "未知异常"
            serverInfo = appName + "|" +FileManage.mergeFiles(sqlContext, fileSystem, batchTime, inputPath, childPath, mergePath)
          } else if(serverLine == "moveLocalToHDFS"){
            val appName = Params.getString("appName")
            val loadTime = Params.getString("loadTime")
            val localPath = Params.getString("localPath")
            val hdfsParentDir = Params.getString("hdfsParentDir")
            val fileWildCard = Params.getString("fileWildCard")
            serverInfo = "未知异常"
            serverInfo = appName + "|" + HDFSFiles.moveLocalToHDFS(fileSystem,localPath, hdfsParentDir, loadTime, fileWildCard)
          }
          else {
            System.out.println("go")
          }
        }catch {
          case e: Exception =>
            e.printStackTrace()
            response = "失败"
            httpCode = 500
        }

        response = "HttpServerStatus: " + response + ". " + serverInfo

        httpExchange.sendResponseHeaders(httpCode, response.getBytes.length)
        val responseBody: OutputStream = httpExchange.getResponseBody
        responseBody.write(response.getBytes)
        responseBody.flush
        responseBody.close
      }

      httpServer.createContext("/ping", new HttpHandler() {
        override def handle(httpExchange: HttpExchange): Unit = {
          var response = "存活"
          var httpCode = 200

          try {
            if (sqlContext.sparkContext.isStopped) {
              httpCode = 400
              response = "spark终止"
            }
          } catch {
            case e: Exception =>
              httpCode = 500
              response = "服务异常"
          } finally {
            httpExchange.sendResponseHeaders(httpCode, response.getBytes().length)
            val out = httpExchange.getResponseBody //获得输出流
            out.write(response.getBytes())
            out.flush()
            httpExchange.close()
          }
        }
      })


      /**
        * 停止sc测试
        */
      httpServer.createContext("/stop_sc", new HttpHandler() {
        override def handle(httpExchange: HttpExchange): Unit = {
          var response = "成功"
          var httpCode = 200

          try {
            sqlContext.sparkContext.stop()
          } catch {
            case e: Exception =>
              httpCode = 500
              response = "服务异常"
          } finally {
            httpExchange.sendResponseHeaders(httpCode, response.getBytes().length)
            val out = httpExchange.getResponseBody //获得输出流

            out.write(response.getBytes())
            out.flush()
            httpExchange.close()
          }
        }
      })

      httpServer.start()
      println("ConvertServer started " + port + " ......")
    })
  }
}