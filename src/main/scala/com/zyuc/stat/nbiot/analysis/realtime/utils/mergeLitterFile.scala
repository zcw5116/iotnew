package com.zyuc.stat.nbiot.analysis.realtime.utils
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-5-25.
  */
object mergeLitterFile {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setAppName("test_2017062716").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val inputPath = sc.getConf.get("spark.app.inputPath")
    val childPath = sc.getConf.get("spark.app.childPath")
    val mergePath = sc.getConf.get("spark.app.mergePath")
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    //val d = dataTime.substring(0, 8)
    //val h = dataTime.substring(8, 10)
    //val childPath = s"/dayid=$d/hourid=$h"

    mergeFiles(sqlContext, fileSystem, dataTime, inputPath, childPath, mergePath)

  }
  def mergeFiles(parentContext:SQLContext, fileSystem:FileSystem, batchTime:String, inputPath:String, childPath:String, mergePath:String): String ={
    val sqlContext = parentContext.newSession()
    val srcDataPath = inputPath + childPath
    val mergeSrcPath = mergePath + "/" + batchTime + "/src" + childPath
    val mergeDataPath = mergePath + "/" + batchTime + "/data" + childPath

    var mergeInfo = "merge success"

    try{
      val partitionNum = FileUtils.computePartitionNum(fileSystem, srcDataPath, 128)
      // 将需要合并的文件mv到临时目录
      FileUtils.moveFiles(fileSystem, batchTime, srcDataPath, mergeSrcPath, true)
      val srcDF = sqlContext.read.format("orc").load(mergeSrcPath + "/")

      // 将合并目录的src子目录下的文件合并后保存到合并目录的data子目录下
      srcDF.coalesce(partitionNum).write.format("orc").mode(SaveMode.Overwrite).save(mergeDataPath)

      // 将合并目录的data目录下的文件移动到原目录
      FileUtils.moveFiles(fileSystem, batchTime, mergeDataPath, srcDataPath, false)

      // 删除 合并目录src的子目录
      fileSystem.delete(new Path(mergePath + "/" + batchTime), true)

    }catch {
      case e:Exception => {
        e.printStackTrace()
        mergeInfo = "merge failed"
      }
    }

    mergeInfo
  }

}
