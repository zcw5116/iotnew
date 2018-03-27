package com.zyuc.stat.epc.etl.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by zhoucw on 下午1:56.
  */
object EpcMmeEtlTransfrom {

  val ZTMM:String = "ZT_MM"
  val ZTSM:String = "ZT_SM"
  val ZTPaging:String =  "ZT_Paging"


  def parse(df:DataFrame, mmeType:String): DataFrame = {
    var newDF:DataFrame = null

      newDF = df.selectExpr("T0 as mmetime", "T12 as eNBID", "T13 as pcause",
         s"case when T13='17' and '${mmeType}'='${ZTMM}' then 'fail' " +
              s" when  T13='38' and '${mmeType}'='${ZTSM}' then 'fail' " +
              s" when '${mmeType}'='${ZTPaging}' then  T131 " +
              s" else 'succ' end as processresult",
        "T5 as imsi", "regexp_replace(T6,'^00','') as MSISDN",
        "substr(regexp_replace(T6,'^00',''), 1, 9) as mdnsection", "T8 as pidenti",
        "T81 as processflag", "T9 as Delaytime", s"'${mmeType}' as mmetype",
        "substr(regexp_replace(T0,'-',''),3,6) as d", "substr(T0,12,2) as h",
        "lpad(floor(substr(T0,15,2)/5)*5,2,'0') as m5")

    newDF
  }
}
