package com.zyuc.stat.nbiot.analysis.tmpTotal

import java.io.{File, FileOutputStream, InputStream, OutputStream}

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-7-17.
  */
object PgwM5Output {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[2]").setAppName("NbM5Analysis_201805161510")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/cdr/analy_realtime/pgw")
    ///d=180627/h=10/m5=10
    val localPath = sc.getConf.get("spark.app.localPath", "/home/slview/nms/bigdata/sparkapp/cdr_4g_backup/")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val h = dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)
    val childrenPath = "/d=" + d + "/h=" + h + "/m5=" + m5

    val nbPath = inputPath + childrenPath
    sqlContext.read.format("orc").load(nbPath).coalesce(1)
      .rdd.saveAsTextFile("/user/iot/data/cdr/analy_realtime/backup_pgw" + childrenPath)

    val inputStream: FSDataInputStream = fileSystem
      .open(new Path("/user/iot/data/cdr/analy_realtime/backup_pgw" + childrenPath + "/part-00000"))

    val localFile : File = createLocalFile(localPath + "tmp/" + d+h+m5 +".txt")

    val outputStream: FileOutputStream = new FileOutputStream(localFile)
    transport(inputStream, outputStream)

    fileSystem.globStatus(new Path("/user/iot/data/cdr/analy_realtime/backup_pgw" + childrenPath))
      .foreach(x => fileSystem.delete(x.getPath(), true))
/*    sqlContext.read.format("orc").load(nbPath).coalesce(1)
      .rdd.saveAsTextFile(localPath + "tmp" + childrenPath)

    val oldFile = new File(localPath + "tmp" + childrenPath + "/part-00000")
    val newFile = new File(localPath + "data/" + dataTime + ".txt")
    oldFile.renameTo(newFile)*/

  }

  def transport(inputStream: InputStream, outputStream: OutputStream): Unit = {
    val buffer = new Array[Byte](64 * 10000)
    var len = inputStream.read(buffer)
    while (len != -1) {
      outputStream.write(buffer, 0, len - 1)
      len = inputStream.read(buffer)
    }
    outputStream.flush()
    inputStream.close()
    outputStream.close()
  }
  def createLocalFile(fullName : String) : File = {
    val target : File = new File(fullName)
    if(!target.exists){
      val index = fullName.lastIndexOf(File.separator)
      val parentFullName = fullName.substring(0, index)
      val parent : File = new File(parentFullName)

      if(!parent.exists)
        parent.mkdirs
      else if(!parent.isDirectory)
        parent.mkdir

      target.createNewFile
    }
    target
  }


}