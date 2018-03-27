package test


import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


/**
  * Created by dell on 2017/8/25.
  */
object test1 {
  def main(args: Array[String]): Unit = {
    //val a = 128.8765934
    //val b = a.formatted("%.0f")
    //val beginStr = "20170826000000"
    //val fm = new SimpleDateFormat("yyyyMMddHHmmss")
    //val dt = fm.parse(beginStr);
    //val time = dt.getTime
    //val time = "20170829"
    //println(time.substring(0,8))
    //println(time.substring(2,8))
    //println(time.substring(6,8))

    //获取一个月中的第一天和最后一天
    //var someday = "201709"
    //val format = "yyyyMM"
    //val dayofmonth =  DateTime.parse(someday,DateTimeFormat.forPattern(format)).dayOfMonth().get()
    //println("dayofmonth:  "+dayofmonth)
    //val firstday = DateTime.parse(someday,DateTimeFormat.forPattern(format)).minusDays(dayofmonth-1).toString("yyyyMMdd")
    //println("firstday:  "+firstday)
    //val lastday =  DateTime.parse(firstday,DateTimeFormat.forPattern("yyyyMMdd")).plusMonths(1).minusDays(1).toString("yyyyMMdd")
    //println("lastday:  "+lastday)

    //获取一周中的第一天和最后一天

    val sometime = "20170903"
    val format = "yyyyMMdd"
    val dayofweek =  DateTime.parse(sometime,DateTimeFormat.forPattern(format)).dayOfWeek().get()
    println("weekofday:  "+dayofweek)
    val firstday = DateTime.parse(sometime,DateTimeFormat.forPattern(format)).minusDays(dayofweek-1).toString("yyyyMMdd")
    println("firstday:  "+firstday)
    val lastday =  DateTime.parse(firstday,DateTimeFormat.forPattern("yyyyMMdd")).plusWeeks(1).minusDays(1).toString("yyyyMMdd")
    println("lastday:  "+lastday)
    var a:Map[String,String] = Map()
    //a = DateUtils.getWeeks(sometime,format)
    println("a :  "+a )


    val today1 = DateTime.now().toString("yyyyMMdd")


    // var sumitemtype = "authlog_3g"
    // var ItmeName = sumitemtype.substring(0, sumitemtype.lastIndexOf("_") ) //"authlog"
    // var subItmeName = sumitemtype.substring(sumitemtype.lastIndexOf("_") + 1) //"4g"
    // println("ItmeName:"+ItmeName)
    // println("subItmeName:"+subItmeName)
    // sumitemtype = "mme"
    // ItmeName = sumitemtype.substring(0, sumitemtype.lastIndexOf("_") + 1) //"authlog"
    // subItmeName = sumitemtype.substring(sumitemtype.lastIndexOf("_") + 1) //"4g"
    // println("ItmeName:"+ItmeName)
    // println("subItmeName:"+subItmeName)

    val abd = "a_v_v_d"

    var array1:String = abd.split("_",4)(1)
    val cdf = "dbc"
    array1 = abd.split("_",4)(1)
    //println("array1:"+array1)

    //test map


  }

}
