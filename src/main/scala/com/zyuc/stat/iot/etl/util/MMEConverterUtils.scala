package com.zyuc.stat.iot.etl.util

import org.apache.spark.sql.{DataFrame, Row}
import com.alibaba.fastjson.JSON
import com.zyuc.stat.iot.etl.secondary.AuthlogSecondETL.logError
import org.apache.spark.Logging


/**
  * Created by zhoucw on 17-7-23.
  */
object MMEConverterUtils extends Logging{

  val MME_HWMM_TYPE:String = "HuaweiUDN-MM"
  val MME_HWSM_TYPE:String = "HuaweiUDN-SM"
  val MME_ZTMM_TYPE:String = "sgsnmme_mm"
  val MME_ZTSM_TYPE:String = "sgsnmme_sm"
  val MME_ERMM_TYPE:String = "er_mm"
  val MME_ERSM_TYPE:String = "er_sm"
  val NB_MME_HWMM_TYPE:String = "HuaweiUDN-MM"
  val NB_MME_HWSM_TYPE:String = "HuaweiUDN-SM"
  val NB_MME_ZTMM_TYPE:String = "sgsnmme_mm"
  val NB_MME_ZTSM_TYPE:String = "sgsnmme_sm"
  val NB_MME_ERMM_TYPE:String = "er_mm"
  val NB_MME_ERSM_TYPE:String = "er_sm"
  val NB_MME_ZTPG_TYPE:String = "sgsnmme_paging"


  def parseMME(mmeDF:DataFrame, mmetype:String) = {
    var newDF: DataFrame = null
    logInfo("############ mmetype:"+mmetype)
    try {
        if (mmetype == NB_MME_HWSM_TYPE) {
          newDF = mmeDF.selectExpr("T8 as procedureid", "T0 as starttime", "'' as a" +
            "cctype", "T5 as IMSI", "T6 as MSISDN",
            "T14 as sergw", "T13 as pcause", "T7 as imei", "'' as ci",
            "conv(substr(T12,3), 16, 10) as eNBID",
            "T24 as uemme", "'' as newgrpid",
            "'' as newmmecode", "'' as newmtmsi", "'' as oldmcc", "'' as oldgrpid", "'' as oldmmecode", "'' as oldmtmsi",
            "T99 as province", s"'$mmetype' as mmetype",
            s"case when T13='0x0000' then 'success' else 'failed' end as result",
            "case when T8 in('0x00','0x18') then 1 else 0 end as isAttach",
            "T81 as RegProSign", "T131 RegProRst", "T9 as delay",
            "T49 as eci",
            "substr(regexp_replace(T0,'-',''),3,6) as d",
            "substr(T0,12,2) as h",
            "lpad(floor(substr(T0,15,2)/5)*5,2,'0') as m5"
          )
        } else if ( mmetype == NB_MME_ZTMM_TYPE || mmetype == NB_MME_ZTSM_TYPE) {
          newDF = mmeDF.selectExpr("T8 as procedureid", "T0 as starttime", "T10 as acctype", "T5 as IMSI", "T6 as MSISDN",
            "T14 as sergw", "T13 as pcause", "T7 as imei", "T43 as ci",
            s"case when '$mmetype' = '$NB_MME_HWMM_TYPE' then conv(substr(T12,3), 16, 10) else T12 end as eNBID",
            "T24 as uemme",
            "T17 as newgrpid", "T18 as newmmecode", "T19 as newmtmsi", "T28 as oldmcc", "T21 as oldgrpid",
            "T22 as oldmmecode", "T23 as oldmtmsi", "T99 as province", s"'$mmetype' as mmetype",
            s"case when '$mmetype' in ('$NB_MME_HWMM_TYPE', '$NB_MME_HWSM_TYPE') and T13='0x0000' then 'success' when '$mmetype' in ('$NB_MME_ZTMM_TYPE', '$NB_MME_ZTSM_TYPE') and T13='4294967295' then 'success' else 'failed' end as result",
            s"case when '$mmetype' in ('$NB_MME_HWMM_TYPE', '$NB_MME_HWSM_TYPE') and T8 in('0x00','0x18') then 1 when  '$mmetype' in ('$NB_MME_ZTMM_TYPE', '$NB_MME_ZTSM_TYPE') and T8 in('2101','2102') then 1 else 0 end as isAttach",
            "T81 as RegProSign", "T131 RegProRst", "T9 as delay","concat(T12,'-',T43) as eci",
            "substr(regexp_replace(T0,'-',''),3,6) as d",
            "substr(T0,12,2) as h",
            "lpad(floor(substr(T0,15,2)/5)*5,2,'0') as m5"
          )
        } else if(mmetype == NB_MME_HWMM_TYPE){
          newDF = mmeDF.selectExpr("T8 as procedureid", "T0 as starttime", "T10 as acctype", "T5 as IMSI", "T6 as MSISDN",
            "T14 as sergw", "T13 as pcause", "T7 as imei", "T43 as ci",
            s"case when '$mmetype' = '$NB_MME_HWMM_TYPE' then conv(substr(T12,3), 16, 10) else T12 end as eNBID",
            "T24 as uemme",
            "T17 as newgrpid", "T18 as newmmecode", "T19 as newmtmsi", "T28 as oldmcc", "T21 as oldgrpid",
            "T22 as oldmmecode", "T23 as oldmtmsi", "T99 as province", s"'$mmetype' as mmetype",
            s"case when '$mmetype' in ('$NB_MME_HWMM_TYPE', '$NB_MME_HWSM_TYPE') and T13='0x0000' then 'success' when '$mmetype' in ('$NB_MME_ZTMM_TYPE', '$NB_MME_ZTSM_TYPE') and T13='4294967295' then 'success' else 'failed' end as result",
            s"case when '$mmetype' in ('$NB_MME_HWMM_TYPE', '$NB_MME_HWSM_TYPE') and T8 in('0x00','0x18') then 1 when  '$mmetype' in ('$NB_MME_ZTMM_TYPE', '$NB_MME_ZTSM_TYPE') and T8 in('2101','2102') then 1 else 0 end as isAttach",
            "T81 as RegProSign", "T131 RegProRst", "T9 as delay",
            "T49 as eci",
            "substr(regexp_replace(T0,'-',''),3,6) as d",
            "substr(T0,12,2) as h",
            "lpad(floor(substr(T0,15,2)/5)*5,2,'0') as m5"
          )
        }
        else if (mmetype == NB_MME_ERMM_TYPE || mmetype == NB_MME_ERSM_TYPE) {
          newDF = mmeDF.selectExpr("T8 as procedureid", "T0 as starttime", "T10 as acctype", "T5 as IMSI", "T6 as MSISDN",
            "T14 as sergw", "T13 as pcause", "T7 as imei", "T43 as ci", "T12 as eNBID", "'' as uemme",
            "T17 as newgrpid", "T18 as newmmecode", "T19 as newmtmsi", "T28 as oldmcc", "T21 as oldgrpid",
            "T22 as oldmmecode", "T23 as oldmtmsi", "'' as province", s"'$mmetype' as mmetype",
            s"case when '$mmetype' in ('$NB_MME_ERMM_TYPE','$NB_MME_ERSM_TYPE') and T13 ='nocausecode' then 'success' else 'failed' end as result",
            s"case when '$mmetype' in ('$NB_MME_ERMM_TYPE','$NB_MME_ERSM_TYPE') and T8  ='attach' then 1  else 0 end as isAttach",
            "T81 as RegProSign", "T131 RegProRst", "T9 as delay",
            "T49 as eci",
            "substr(regexp_replace(T0,'-',''),3,6) as d",
            "substr(T0,12,2) as h",
            "lpad(floor(substr(T0,15,2)/5)*5,2,'0') as m5"
          )
        }else if(mmetype == NB_MME_ZTPG_TYPE){
          newDF = mmeDF.selectExpr("T8 as procedureid", "T0 as starttime", "T10 as acctype", "T5 as IMSI", "T6 as MSISDN",
            "'' as sergw", "T13 as pcause", "T7 as imei", "T43 as ci", "T12 as eNBID", "T24 as uemme",
            "T17 as newgrpid", "T18 as newmmecode", "T19 as newmtmsi", "T28 as oldmcc", "T21 as oldgrpid",
            "T22 as oldmmecode", "T23 as oldmtmsi", "T99 as province", s"'$mmetype' as mmetype",
            s"case when '$mmetype' in ('$MME_HWMM_TYPE', '$MME_HWSM_TYPE') and T13='0x0000' then 'success' when '$mmetype' in ('$MME_ZTMM_TYPE', '$MME_ZTSM_TYPE') and T13='4294967295' then 'success' else 'failed' end as result",
            s"case when '$mmetype' in ('$MME_HWMM_TYPE', '$MME_HWSM_TYPE') and T8 in('0x00','0x18') then 1 when  '$mmetype' in ('$MME_ZTMM_TYPE', '$MME_ZTSM_TYPE') and T8 in('2101','2102') then 1 else 0 end as isAttach",
            "concat(T12,'-',T43) as eci",
            "substr(regexp_replace(T0,'-',''),3,6) as d",
            "substr(T0,12,2) as h",
            "lpad(floor(substr(T0,15,2)/5)*5,2,'0') as m5"
          )
        }


      newDF.filter(newDF.col("starttime").isNotNull)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError("[" + mmetype + "] 失败 处理异常" + e.getMessage)
        newDF
    }

  }


}
