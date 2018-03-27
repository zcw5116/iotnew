package com.zyuc.stat.app

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{ExecutorService, Executors}

import com.zyuc.iot.utils.FileFilter
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by zhoucw on 下午1:22.
  */
object HDFSFiles {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("test_201804031140").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    val localPath = sc.getConf.get("spark.app.localpath", "/hadoop/bd/b1")
    val hdfsParentDir = sc.getConf.get("spark.app.hdfsPath", "/tmp")
    val fileWildCard = sc.getConf.get("spark.app.fileWildCard", "^p.*[0-9]$")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val dir = new File(localPath)
    val files = dir.listFiles(new FileFilter(fileWildCard))

    val destDir = hdfsParentDir + "/" + dataTime

    moveLocalToHDFS(fileSystem, localPath, hdfsParentDir, dataTime, fileWildCard)

  }


  def ensureHDFSDirExists(fileSystem: FileSystem, dir:String) = {
    if(!fileSystem.exists(new Path(dir))){
      fileSystem.mkdirs(new Path(dir))
    }
  }

  /**
    *
    * @param fileSystem
    * @param localPath
    * @param hdfsParentDir
    * @param loadTime
    * @param fileWildCard
    */
  def moveLocalToHDFS(fileSystem: FileSystem, localPath:String,  hdfsParentDir:String, loadTime:String, fileWildCard:String) = {


    try{
      val dir = new File(localPath)
      val files = dir.listFiles(new FileFilter(fileWildCard))

      val destDir = hdfsParentDir + "/" + loadTime

     ensureHDFSDirExists(fileSystem, destDir)


      val fileworkings = new mutable.HashSet[Path]()
      for(file <- files){
        val fileWorking = file.getAbsolutePath + ".working"
        file.renameTo(new File(fileWorking))
        fileworkings .+= (new Path(fileWorking))
      }

      val executor = Executors.newFixedThreadPool(10)
      for(fileworking <- fileworkings) {
        executor.execute(new Runnable {
          override def run(): Unit = {
            fileSystem.moveFromLocalFile(fileworking, new Path(destDir + "/"))
            println(destDir + "/" + fileworking.getName)
            fileSystem.rename(new Path(destDir + "/" + fileworking.getName), new Path(destDir + "/" + fileworking.getName.substring(0, fileworking.getName.lastIndexOf("."))))
          }
        })
      }

      executor.shutdown()

    }catch{
      case e:Exception => {
        e.printStackTrace()
      }
    }

  }

  /**
    * 合并文件
    * 合并步骤：
     *1. 将小文件所在的子目录move到合并目录的src目录下
        *../${inputPath}/${childPath}   ==>  ../${mergePath}/src/${childPath}
     *2. 对../${mergePath}/src/${childPath}/目录下文件进行合并，合并后存放到合并目录的data目录../${mergePath}/data/${childPath}/下
         *../${mergePath}/src/${childPath}/ 合并后文件存入：  ../${mergePath}/data/${childPath}/
     *3. 将合并目录的data目录下的文件移动到原目录 ../${inputPath}/${childPath}/
         *../${mergePath}/data/${childPath}/ 移动到 ../${inputPath}/${childPath}/
     *4. 合并目录的src目录下的子目录
        *删除目录 ../${mergePath}/src/${childPath}/
 *
    * @param parentContext
    * @param fileSystem
    * @param batchTime      批次的时间
    * @param inputPath      需要合并文件目录的根目录
    * @param childPath      需要合并文件目录的子目录
    * @param mergePath      合并文件的临时目录
    * @return
    */
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

    batchTime + "|" +mergeInfo
  }

}
