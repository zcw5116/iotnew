package com.zyuc.stat.iot.analysis.common

import com.zyuc.stat.utils.DBUtils
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
/**
  * Created by hadoop on 18-7-24.
  */
object BsSectionAnalysis extends Logging {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    var nowDate:Date = new Date()
    logInfo("nowDate" +  nowDate.toString())

    var nowDay =   getDaysBefore(nowDate, 0)
    nowDay = sc.getConf.get("spark.app.nowday", "180725")

    val dateSDF = new SimpleDateFormat("yyMMdd")
    nowDate = dateSDF.parse(nowDay)

    var lastDay=   getDaysBefore(nowDate, 1)  //dateSDF.format(nowDate.getTime - 60*60*24*1000)
    lastDay = sc.getConf.get("spark.app.lastday", "180720")
    logInfo("nowDay: " + nowDay + ", lastDay: " + lastDay)

    val firstFlag:String = sc.getConf.get("spark.app.first", "0")
    val tidbTabelName:String =  sc.getConf.get("spark.app.tidbtable", "iot_bs_sector_pgw")
    val inDir = sc.getConf.get("spark.app.indir", "/user/iot_ete/data/cdr/transform/pgw")
    val outDir = sc.getConf.get("spark.app.outdir", "/user/iot_ete/bs_sector/pgw")
    var inputDir: String = inDir + "/data"  // "/user/iot_ete/data/cdr/transform/pgw/data"
    var tempSaveDir: String = outDir + "/dataTemp/d=" + nowDay // "/user/iot_ete/bs_sector/pgw/dataTemp/"
    var saveDir:String = outDir + "/data/d=" + nowDay

    deleteDirFile(outDir + "/dataTemp/" ,sc)

    var relt = tempNodeBSectorInfo(nowDate, inputDir, tempSaveDir, sqlContext)
    if (-1 == relt) {
      logInfo("[EXIT]load file from " + tempSaveDir + " failed")
      sc.stop()
      return

    }

    var nowTable:String = "NodeBSectorInfo_now"
    relt = loadData2Table(nowTable,  outDir + "/dataTemp/d=" + nowDay  + "/now/*orc", sqlContext, sc)
    if (-1 == relt) {
      logInfo("[EXIT]load file from " + outDir + "/dataTemp/d=" + nowDay  + "/now/*orc" + " failed")
      sc.stop()
      return
    }
    else if ( "1" == firstFlag){
      saveNodeBSectorInfo(nowDate, nowTable, "", saveDir, sqlContext)
      data2Tib(saveDir, tidbTabelName, sqlContext)
      sc.stop()
      return
    }

    var lastTable:String = "NodeBSectorInfo_last"
    relt = loadData2Table(lastTable, outDir + "/data/d="     + lastDay + "/*orc"    , sqlContext, sc)
    if (-1 == relt && "1" != firstFlag ) {
      logInfo("[EXIT]load file from " + outDir + "/data/d="     + lastDay + "/*orc" + " failed")
      sc.stop()
      return
    }

    saveNodeBSectorInfo(nowDate, nowTable, lastTable, saveDir, sqlContext)
    data2Tib(saveDir, tidbTabelName, sqlContext)

    sc.stop()
    return
  }

  /**
    *
    * @param fileDir
    * @param sc
    *   删除文件
    */

  def deleteDirFile(fileDir:String, sc:SparkContext): Unit = {
    //delete temp file
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    try{
      fileSystem.globStatus(new Path(fileDir) ).foreach(x => fileSystem.delete(x.getPath(), true))
    }catch{
      case e:Exception => {
        //e.printStackTrace()
      }
    }
  }


  /**
    *
    * @param inputDir
    * @param tempSaveDir
    * @param dayStr
    * @param sqlContext
    * @return
    *      指定日期dayStr的数据读到临时表，提取出扇区信息保存到  tempSaveDir +  "/" +  dayStr下面
    */
  def tempNodeBSectorInfoByDay(inputDir:String, tempSaveDir:String, dayStr:String, sqlContext:SQLContext): Int= {

    // hdfs://sparkhost:8020/user/iot_ete/data/cdr/transform/pgw/data/d=180716/h=02/m5=00/201807160222-1.orc
    val inputpath =  inputDir + "/d=" + dayStr + "/*"


    var df:DataFrame = null
    try{
      df = sqlContext.read.format("orc").load(inputpath)

    }catch{
      case e:Exception => {
        //e.printStackTrace()
        println("inputpath: ", inputpath, "异常")
        return -1
      }
    }
    println("inputpath: ", inputpath)
    df.registerTempTable("NodeBSectorInfoTmp")
    //df.printSchema()
    //df.show()
    df.registerTempTable("NodeBSectorInfoTmp")

    //"/user/iot_ete/bs_sector/pgw/dataTemp/d=180716/180709"
    val outpath =  tempSaveDir + "/last/" + dayStr
    val outDF = sqlContext.sql("select  prov, t802, enbid, t806, max(l_timeoflastusage) as l_timeoflastusage from NodeBSectorInfoTmp group by  prov, t802, enbid, t806").repartition(1)
    println("outpath: ", outpath, outDF.count())
    //outDF.show()
    outDF.write.format("orc").mode(SaveMode.Overwrite).save(outpath)
    sqlContext.dropTempTable("NodeBSectorInfoTmp")

    return 0
  }


  /**
    *
    * @param now
    * @param inputDir
    * @param tempSaveDir
    * @param sqlContext
    * @return
    *         now前面7天的数据中获取的扇区信息保存到  tempSaveDir  下面
    */
  def tempNodeBSectorInfo(now: Date, inputDir:String, tempSaveDir:String, sqlContext: SQLContext): Int = {

    for( i <- 1 to 7){
      println( "Value of i: " + i )
      var dayStr:String = getDaysBefore(now, i)
      var relt:Int = tempNodeBSectorInfoByDay(inputDir, tempSaveDir, dayStr, sqlContext)
    }

    val inputpath =  tempSaveDir + "/last/*"
    var df:DataFrame = null
    try{

      df = sqlContext.read.format("orc").load(inputpath)

    }catch{
      case e:Exception => {
        //e.printStackTrace()
        println("inputpath: ", inputpath, "异常")
        return -1
      }
    }
    println("inputpath: ", inputpath, df.count())
    //df.show()
    df.registerTempTable("NodeBSectorInfoTmp")


    //"/user/iot_ete/bs_sector/pgw/dataTemp/d=180716/180709"
    val outpath:String =  tempSaveDir + "/now"
    val outDF = sqlContext.sql(
      """
         select t.prov, t.tac, t.bsid, t.sectid, t.firtusetime,  t.firtusetime as lastusetime, 0 as status
         from (
                select  prov , t802 as tac , enbid as bsid, t806 as sectid, max(l_timeoflastusage) as firtusetime
                from NodeBSectorInfoTmp
                group by  prov, t802, enbid, t806
           ) t
      """).repartition(1) //.coalesce(1)
    println("outpath: ", outpath, outDF.count())
    //outDF.printSchema()
    //outDF.show()
    outDF.write.format("orc").mode(SaveMode.Overwrite).save(outpath)


    sqlContext.dropTempTable("NodeBSectorInfoTmp")

    return 0
  }

  /**
    *
    * @param nowDay
    * @param now_table
    * @param before_table
    * @param saveDir
    * @param sqlContext
    *    nowDay之前7天中到扇区信息和上次库中信息做比较后生成新的全量扇区信息
    */
  def saveNodeBSectorInfo(nowDay:Date, now_table:String,  before_table:String, saveDir:String, sqlContext: SQLContext) = {

    sqlContext.sql("show tables").show()

    val dateSDF = new SimpleDateFormat("yyyy-MM-dd")
    val today =  dateSDF.format( nowDay.getTime )
    println("today: " + today)

    var sqlStatement:String =  s"""
       select  x.prov, x.tac, x.bsid, x.sectid, x.firtusetime, x.lastusetime, x.status
       from (
          select COALESCE(n.prov, b.prov) as prov, COALESCE(n.tac, b.tac) as tac , COALESCE(n.bsid, b.bsid) as bsid, COALESCE(n.sectid, b.sectid) as sectid,
            case
               when n.firtusetime is null then b.firtusetime
               when b.firtusetime is null or b.status = 6  then n.firtusetime
               else  b.firtusetime
            end as firtusetime,
            COALESCE(n.lastusetime, b.lastusetime) as lastusetime ,
            case
              when datediff(to_date( '${today}' ),to_date(COALESCE(n.lastusetime, b.lastusetime))) < 8  then 0
              when datediff(to_date( '${today}' ),to_date(COALESCE(n.lastusetime, b.lastusetime))) >= 8  and datediff(to_date( '${today}' ),to_date(COALESCE(n.lastusetime, b.lastusetime))) < 16 then 1
              when datediff(to_date( '${today}' ),to_date(COALESCE(n.lastusetime, b.lastusetime))) >= 16 and datediff(to_date( '${today}' ),to_date(COALESCE(n.lastusetime, b.lastusetime))) < 31 then 2
              when datediff(to_date( '${today}' ),to_date(COALESCE(n.lastusetime, b.lastusetime))) >= 31 and datediff(to_date( '${today}' ),to_date(COALESCE(n.lastusetime, b.lastusetime))) < 61 then 3
              when datediff(to_date( '${today}' ),to_date(COALESCE(n.lastusetime, b.lastusetime))) >= 61 and datediff(to_date( '${today}' ),to_date(COALESCE(n.lastusetime, b.lastusetime))) < 91  then 4
              when datediff(to_date( '${today}' ),to_date(COALESCE(n.lastusetime, b.lastusetime))) >= 91 and datediff(to_date( '${today}' ),to_date(COALESCE(n.lastusetime, b.lastusetime))) < 101 then 5
              else   6
             end as status
           from ${now_table}  n full outer join ${before_table}  b  on (n.prov = b.prov and n.tac = b.tac and n.bsid = b.bsid and n.sectid = b.sectid)
       ) x
       where (x.prov is null or x.prov != 'provtest') and (x.tac is null or x.tac != 'tactest') and
             (x.bsid is null or x.bsid != 'bsidtest') and (x.sectid is null or x.sectid != 'sectidtest')
      """

    if ("" == before_table){
      sqlStatement = s"""
                       select n.prov, n.tac, n.bsid, n.sectid, n.firtusetime, n.lastusetime,
                              case
                           		   when datediff(to_date( '${today}' ),to_date(n.lastusetime)) < 8  then 0
                                 when datediff(to_date( '${today}' ),to_date(n.lastusetime)) >= 8  and datediff(to_date( '${today}' ),to_date(n.lastusetime)) < 16 then 1
                                 when datediff(to_date( '${today}' ),to_date(n.lastusetime)) >= 16 and datediff(to_date( '${today}' ),to_date(n.lastusetime)) < 31 then 2
                                 when datediff(to_date( '${today}' ),to_date(n.lastusetime)) >= 31 and datediff(to_date( '${today}' ),to_date(n.lastusetime)) < 61 then 3
                                 when datediff(to_date( '${today}' ),to_date(n.lastusetime)) >= 61 and datediff(to_date( '${today}' ),to_date(n.lastusetime)) < 91  then 4
                                 when datediff(to_date( '${today}' ),to_date(n.lastusetime)) >= 91 and datediff(to_date( '${today}' ),to_date(n.lastusetime)) < 101 then 5
                                 else   6
                             end as status
                   from ${now_table}  n """
    }

    val outDF = sqlContext.sql( sqlStatement ).repartition(1)//.coalesce(1)


    //outDF.printSchema()
    outDF.show()
    //"/user/iot_ete/bs_sector/pgw/data/d=180716/"
    println("outpath: ", saveDir, outDF.count())
    outDF.write.format("orc").mode(SaveMode.Overwrite).save(saveDir)
  }

  /**
    *
    * @param tableName
    * @param inputpath
    * @param sqlContext
    * @param sc
    * @return
    *         将路径inputpath下面orc文件装载到临时表tabelname中
    */
  def loadData2Table(tableName:String, inputpath:String, sqlContext:SQLContext , sc:SparkContext):Int = {

    var df:DataFrame = null
    try{
      df = sqlContext.read.format("orc").load(inputpath)
    }catch{
      case e:Exception => {
        //e.printStackTrace()
        println("loadData2Table(" + tableName + "): " + inputpath, "异常")
        //createEmptyTabe_T(tableName, sqlContext, sc)
        return -1
      }
    }
    println("loadData2Table(" + tableName + "): " + inputpath,  df.count())
    df.registerTempTable(tableName)
    //df.printSchema()
    df.show()
    return 0
  }


  def createEmptyTabe_T(tabelName:String, sqlContext: SQLContext, sc:SparkContext) = {

    val nameRDD = sc.makeRDD(Array(
      "{\"prov\":\"provtest\"," +
        "\"tac\":\"tactest\"," +
        "\"bsid\":\"bsidtest\"," +
        "\"sectid\":\"sectidtest\"," +
        "\"firtusetime\":\"firtusetimetest\"," +
        "\"lastusetime\":\"lastusetimetest\"," +
        "\"status\":\"0\"" +
        "}"
    ))
    val nameDF = sqlContext.read.json(nameRDD)
    nameDF.registerTempTable(tabelName)
    nameDF.printSchema()
    nameDF.show()
  }

  /**
    *
    * @param saveDir
    * @param tabelName
    * @param sqlContext
    * @return
    *         将saveDir下的扇区信息upsert方式入到tib的表tabelName中
    */
  def data2Tib(saveDir:String, tabelName:String, sqlContext: SQLContext) : Int = {

    var df:DataFrame = null
    try{
      df = sqlContext.read.format("orc").load(saveDir + "/*orc")

    }catch{
      case e:Exception => {
        //e.printStackTrace()
        println("inputpath( data2Tib ->" + tabelName  + "): "+  saveDir + "/*orc", "异常")
        return -1
      }
    }
    println("inputpath( data2Tib ->" + tabelName  + "): "+  saveDir + "/*orc")

    df.registerTempTable("data2Tib")

    val result = sqlContext.sql("""
           select case when prov is null then '-1'
                       else prov
                  end as prov,
                  case when tac is null then ''
                       else tac
                  end as tac,
                  case when bsid is null then ''
                       else bsid
                  end as bsid,
                  case when sectid is null then ''
                      else sectid
                  end as sectid,
                  firtusetime, lastusetime, status
           from data2Tib
     """ ).map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getString(4), x.getString(5), x.getInt(6))).collect()

    //val result =df.map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getString(4), x.getString(5), x.getInt(6))).collect()
    var dbConn = DBUtils.getConnection
    dbConn.setAutoCommit(false)
    val sql =   "insert into " + tabelName +
      """ (prov,  tac,  bsid, sectid, firtusetime, lastusetime, status)
                   values (?, ?, ?, ?, ?, ?, ?)
                   on duplicate key update firtusetime=?, lastusetime=?, status=?
              """

    val pstmt = dbConn.prepareStatement(sql)

    var i = 0
    for(r<-result){
      var prov = r._1
      var tac = r._2
      var bsid = r._3
      var sectid = r._4

      val firtusetime = r._5
      val lastusetime = r._6
      val status = r._7

      pstmt.setString(1, prov)
      pstmt.setString(2, tac)
      pstmt.setString(3, bsid)
      pstmt.setString(4, sectid)
      pstmt.setString(5, firtusetime)
      pstmt.setString(6, lastusetime)
      pstmt.setInt(7, status)

      pstmt.setString(8, firtusetime)
      pstmt.setString(9, lastusetime)
      pstmt.setInt(10, status)

      pstmt.addBatch()
      if (i % 1000 == 0 ) {
        i = 0
        pstmt.executeBatch
        dbConn.commit()
      }

      i = i+ 1
    }
    pstmt.executeBatch
    dbConn.commit()
    pstmt.close()
    dbConn.close()
    i
  }


  /**
    *
    * @param now
    * @param interval
    * @return
    *         获取日期now前interval天日期，返回到的是格式为"yyMMdd"的字符串
    */
  def getDaysBefore(now: Date, interval: Int):String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyMMdd")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(now)

    cal.add(Calendar.DATE, - interval)
    val beforeday = dateFormat.format(cal.getTime())
    beforeday
  }
}
