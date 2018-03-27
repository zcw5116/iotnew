package com.zyuc.stat.utils

/**
  * Created by zhoucw on 17-7-19.
  */

import java.io.{File, FileOutputStream, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.Logging

import scala.collection.mutable


object FileUtils extends Logging {

  /**
    *
    * 根据文件大小构建coalesce
    *
    * @param fileSystem   文件系统
    * @param filePath     文件路径
    * @param coalesceSize 收敛大小
    * @return
    *
    */

  def makeCoalesce(fileSystem: FileSystem, filePath: String, coalesceSize: Int): Int = {
    val path = new Path(filePath)
    try {
      val filesize = fileSystem.getContentSummary(path).getLength
      val msize = filesize.asInstanceOf[Double] / 1024 / 1024 / coalesceSize
      Math.ceil(msize).toInt
    } catch {
      case e: IOException => e.printStackTrace()
        1
    }

  }

  /**
    * 根据目录下文件的大小计算partition数
    *
    * @param fileSystem
    * @param filePath
    * @param partitionSize
    * @return
    */
  def computePartitionNum(fileSystem: FileSystem, filePath: String, partitionSize: Int): Int = {
    val path = new Path(filePath)
    try {
      val filesize = fileSystem.getContentSummary(path).getLength
      val msize = filesize.asInstanceOf[Double] / 1024 / 1024 / partitionSize
      Math.ceil(msize).toInt
    } catch {
      case e: IOException => e.printStackTrace()
        1
    }
  }

  /**
    *
    * 将源目录中的文件移动到目标目录中
    *
    * @param fileSystem
    * @param loadTime
    * @param fromDir
    * @param destDir
    * @param ifTruncDestDir
    */
  def moveFiles(fileSystem: FileSystem, loadTime: String, fromDir: String, destDir: String, ifTruncDestDir: Boolean): Unit = {

    val fromDirPath = new Path(fromDir)
    val destDirPath = new Path(destDir)

    if (!fileSystem.exists(new Path(destDir))) {
      fileSystem.mkdirs(destDirPath.getParent)
    }

    // 是否清空目标目录下面的所有文件
    if (ifTruncDestDir) {
      fileSystem.globStatus(new Path(destDir + "/*") ).foreach(x => fileSystem.delete(x.getPath(), true))
    }

    var num = 0
    fileSystem.globStatus(new Path(fromDir + "/*")).foreach(x => {

      val fromLocation = x.getPath().toString
      val fileName = fromLocation.substring(fromLocation.lastIndexOf("/") + 1)
      val fromPath = new Path(fromLocation)

      if (fileName != "_SUCCESS") {
        var destLocation = fromLocation.replace(fromDir, destDir)
        val fileSuffix = if (fileName.contains(".")) fileName.substring(fileName.lastIndexOf(".")) else ""
        val newFileName = loadTime + "_" + num + fileSuffix

        destLocation = destLocation.substring(0, destLocation.lastIndexOf("/") + 1) + newFileName
        num = num + 1
        val destPath = new Path(destLocation)

        if (!fileSystem.exists(destPath.getParent)) {
          fileSystem.mkdirs(destPath.getParent)
        }
        fileSystem.rename(fromPath, destPath)
      }

    })
    logInfo("move files ")
  }

/*
  def computeCoalesceSize(fileSystem: FileSystem, filePath: String, coalesceSize: Int) : Int = {
    val path = new Path(filePath)
    try {
      val filesize = fileSystem.getContentSummary(path).getLength
      val msize = filesize.asInstanceOf[Double] / 1024 / 1024 / coalesceSize
      Math.ceil(msize).toInt
    } catch {
      case e: IOException => e.printStackTrace()
        1
    }
  }
*/


  /**
    *
    * 检查文件是否上传完毕
    *
    * @param filePath    文件路径正则
    * @param fileCount   文件个数
    * @param checkPeriod 检查周期
    * @param checkTimes  检查次数
    * @param tryCount    当前检查到第几次
    * @return
    *
    */

  def checkFileUpload(fileSystem: FileSystem, filePath: String, fileCount: Int, checkPeriod: Long, checkTimes: Int, tryCount: Int): Int = {


    return 1
  }


  /**
    *
    *
    *
    * @param fileSystem 文件系统
    * @param outputPath 输出路径
    * @param loadTime   数据时间
    * @param template   路径模版
    *
    */

  def moveTempFiles(fileSystem: FileSystem, outputPath: String, loadTime: String, template: String, partitions: mutable.HashSet[String]): Unit = {

    // 删除数据目录到文件
    partitions.foreach(partition => {
      println("outputPath:" + outputPath)
      println("partition:" + partition)
      val dataPath = new Path(outputPath + "data/" + partition + "/" + loadTime + "-" + "*.orc")
      fileSystem.globStatus(dataPath).foreach(x => fileSystem.delete(x.getPath(), false)
      )
      // fileSystem.delete(dataPath, false)
      fileSystem
    })

    val tmpPath = new Path(outputPath + "temp/" + loadTime + template + "/*.orc")
    val tmpStatus = fileSystem.globStatus(tmpPath)

    var num = 0
    tmpStatus.map(tmpStat => {
      val tmpLocation = tmpStat.getPath().toString
      var dataLocation = tmpLocation.replace(outputPath + "temp/" + loadTime, outputPath + "data/")
      val index = dataLocation.lastIndexOf("/")
      dataLocation = dataLocation.substring(0, index + 1) + loadTime + "-" + num + ".orc"
      num = num + 1

      val tmpPath = new Path(tmpLocation)
      val dataPath = new Path(dataLocation)

      if (!fileSystem.exists(dataPath.getParent)) {
        fileSystem.mkdirs(dataPath.getParent)
      }
      fileSystem.rename(tmpPath, dataPath)
    })
  }



  /**
    * desc: 将临时目录下到文件移动到正式的数据目录
    * 临时目录： ${outputPath}/temp/
    * 正式目录： ${outputPath}/data/
    * 移动数据到data目录前， 先删除data目录下的所有见
    *
    * @author zhoucw
    * @param fileSystem hdfs文件系统
    * @param outputPath 输出目录
    * @param loadTime   时间
    */
  def moveTempFilesToData(fileSystem: FileSystem, outputPath: String, loadTime: String): Unit = {

    // 删除目录下的文件
    val dataPath = new Path(outputPath + "data/*.orc")
    fileSystem.globStatus(dataPath).foreach(x => fileSystem.delete(x.getPath(), false))

    // 移动临时目录到文件到正式的目录
    var num = 0
    val tmpPath = new Path(outputPath + "temp/*.orc")
    fileSystem.globStatus(tmpPath).foreach(x => {
      val tmpLocation = x.getPath().toString
      var dataLocation = tmpLocation.replace(outputPath + "temp/", outputPath + "data/")
      val index = dataLocation.lastIndexOf("/")
      dataLocation = dataLocation.substring(0, index + 1) + loadTime + "-" + num + ".orc"
      num = num + 1

      val tmpPath = new Path(tmpLocation)
      val dataPath = new Path(dataLocation)

      if (!fileSystem.exists(dataPath.getParent)) {
        fileSystem.mkdirs(dataPath.getParent)
      }

      fileSystem.rename(tmpPath, dataPath)
    })
  }


  def moveTempFilesToESpath(fileSystem: FileSystem, outputPath: String, timeid: String, dayid: String): Unit = {


    //,filesuffix:String
    logInfo("##########--outputPath:" + outputPath)
    logInfo("##########--timeid:" + timeid)
    logInfo("##########--dayid:" + dayid)

    val dataPath = new Path(outputPath + dayid + "/" + timeid + "*")
    logInfo("##########--dataPath:" + dataPath)
    fileSystem.globStatus(dataPath).foreach(x => fileSystem.delete(x.getPath(), false))


    val tmpPath = new Path(outputPath + "tmp/" + timeid + "/part*")
    val tmpStatus = fileSystem.globStatus(tmpPath)
    var num = 0

    tmpStatus.map(tmpStat => {
      val tmpLocation = tmpStat.getPath().toString
      var dataLocation = tmpLocation.replace(outputPath + "tmp/" + timeid, outputPath + dayid + "/")
      logInfo("##########--dataLocation:" + dataLocation)
      val index = dataLocation.lastIndexOf("/")
      dataLocation = dataLocation.substring(0, index + 1) + timeid + "-" + num + ".json"
      num = num + 1

      val tmpPath1 = new Path(tmpLocation)
      val dataPath = new Path(dataLocation)

      if (!fileSystem.exists(dataPath.getParent)) {
        fileSystem.mkdirs(dataPath.getParent)
      }
      fileSystem.rename(tmpPath1, dataPath)
      logInfo("##########--dataPath:" + dataPath)


    })


    // val files = fileSystem.listStatus(path)

  }


  def moveNewlogFiles(outputPath: String, outFiles: Array[FileStatus], loadTime: String): Unit = {
    var num = 1
    outFiles.foreach(filestatus => {
      val srcLocation = filestatus.getPath.toString
      val destLocation = srcLocation.replace(outputPath + "newlog", outputPath + "data/" + loadTime)
    })

  }

  def downloadFileFromHdfs(fileSystem: FileSystem, hdfsDirLocation: String, localDirLocation: String, suffix: String): Unit = {
    val hdfsPath = new Path(hdfsDirLocation + "/*")
    val file = new File(localDirLocation)
    if (!file.exists()) {
      file.mkdirs()
    }
    val hdfsStatus = fileSystem.globStatus(hdfsPath)
    hdfsStatus.map(p => {
      val file = p.getPath
      val name = file.toString.substring(file.toString.lastIndexOf("/") + 1)
      val localPath = localDirLocation + name + suffix
      val len = fileSystem.getContentSummary(p.getPath).getLength
      if (len > 0) {
        val in = fileSystem.open(file)
        val out = new FileOutputStream(localPath)
        IOUtils.copyBytes(in, out, 4096, true)
      }
    })
  }


  def downFilesToLocal(fileSystem: FileSystem, hdfsDirLocation: String, localPath: String, loadTime: String, suffix: String): Unit = {

    // 本地目录, 不存在就创建。 如果存在, 就删除目录下到所有文件
    val localDirLocation = localPath + loadTime
    val localFile = new File(localDirLocation)
    if (!localFile.exists()) {
      localFile.mkdirs()
    }
    val fileList = localFile.listFiles()
    fileList.foreach(f => {
      f.delete()
    })

    val hdfsPath = new Path(hdfsDirLocation + "/*")
    val hdfsStatus = fileSystem.globStatus(hdfsPath)
    var num = 0
    hdfsStatus.foreach(p => {
      val hFile = p.getPath
      val name = hFile.toString.substring(hFile.toString.lastIndexOf("/") + 1)
      val localPath = localDirLocation + "/" + loadTime + "_" + num + suffix
      val len = fileSystem.getContentSummary(p.getPath).getLength
      if (len > 0) {
        val in = fileSystem.open(hFile)
        val out = new FileOutputStream(localPath)
        IOUtils.copyBytes(in, out, 4096, true)
      }
      num = num + 1
    })


  }

  def renameHDFSDir(fileSystem: FileSystem, srcLocation: String, destLocation: String): Boolean = {
    val srcPath = new Path(srcLocation)
    val destPath = new Path(destLocation)
    val isRename = fileSystem.rename(srcPath, destPath)
    isRename
  }


  def main(args: Array[String]): Unit = {
    val config = new Configuration
    var fileSystem: FileSystem = null
    fileSystem = FileSystem.get(config)
    val coalesceSize = 1

    val hdfsDirLocation = "/tmp/mme"
    val localDirLocation = "/home/slview/zcw/abc/"
    val loadTime = "20170813"
    val suffix = ".json"

    downFilesToLocal(fileSystem: FileSystem, hdfsDirLocation: String, localDirLocation: String, loadTime: String, suffix: String)
    /*
        try {

          fileSystem = FileSystem.get(config)
          val filePath = "/hadoop/zcw/tmp/wcout"
          val coalesceSize = 5
          println(makeCoalesce(fileSystem, filePath, coalesceSize))
        } catch {
          case e: Exception =>
            e.printStackTrace()
        } finally if (fileSystem != null) try
          fileSystem.close()
        catch {
          case e: Exception =>
            e.printStackTrace()
        }
    */


  }

}
