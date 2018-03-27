package com.zyuc.stat.app

import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoucw on 下午1:22.
  */
object FileManage {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("test_201802031135").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    println("dataTime:" + dataTime)
    if(dataTime.matches("^[0-9]{12}")){
      println("right")
    }else{
      System.err.println(s"Usage: FileManage " +
        s"--name = FileManage_{yyyymmddhhMM}" +
        s" loadTime $dataTime formatted error")
      System.exit(1)
    }
    val inputPath = sc.getConf.get("spark.app.input", "/tmp/jiangxi/sgw/mydata")
    val mergePath = sc.getConf.get("spark.app.mergePath", "/tmp/jiangxi/sgw/mymerge")
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val d = dataTime.substring(2, 8)
    val h = dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)
    val childPath = s"/d=$d/h=$h/m5=$m5"

    val mergeInfo = mergeFiles(sqlContext, fileSystem, dataTime, inputPath, childPath, mergePath)

  }


  /**
    * 合并文件
    * 合并步骤：
     *1. 将小文件所在的子目录move到合并目录的src目录下
        *../${inputPath}/${childPath}   ==>  ../${mergePath}/src/${childPath}
    *     xxx/data/d=?/h=?/m5=? ==>  xxx/merge/src/d=?/h=?/m5=?  ==>  xxx/merge/data/d=?/h=?/m5=? ==> xxx/data/d=?/h=?/m5=?/
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
