package com.zyuc.iot.etl.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mengjd
 * @create 2017-10-24 17:18
 **/
public class UserInfoIncrConverterUtils {
    private static Logger logError = LoggerFactory.getLogger(UserInfoIncrConverterUtils.class);

    public static Row parseLine(String line){
        try{
            String[] p = line.split("\\|", 26);
            String apncompanycode = p[7];
        /*D开头是定向业务*/
            String isDirect = "0";
            if(apncompanycode.startsWith("D")){
                isDirect = "1";
            }
            String vpdnDomain = p[9];
            String isVPDN = "0";
            if(p[10] == "1"){
                isVPDN = "1";
            }
            String internetType = p[24];
            String vpdnOnly ="0";
            if(StringUtils.isNoneEmpty(internetType) && internetType.contains("VpdnBlockInternet")){
                vpdnOnly = "1";
            }
        /*非VPN和定向业务，则普通业务*/
            String isCommon = "0";
            if(vpdnOnly!="1" && isDirect!="1"){
                isCommon="1";
            }
            return RowFactory.create(p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], isVPDN, isDirect, p[11], p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19], p[20], p[21], p[22], p[23], internetType, vpdnOnly, isCommon);
        }catch (Exception e){
            logError.error("ParseError log[" + line + "] msg[" + e.getMessage() + "]");
            return RowFactory.create("0");
        }
    }
    public static StructType parseType(){
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("mdn", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("imsicdma", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("imsilte", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("iccid", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("imei", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("companycode", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("vpdncompanycode", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("apncompanycode", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("nettype", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("vpdndomain", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("isvpdn", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("isdirect", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("subscribetimeaaa", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("subscribetimehlr", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("subscribetimehss", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("subscribetimepcrf", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("firstactivetime", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("userstatus", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("atrbprovince", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("userprovince", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("belo_city", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("belo_prov", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("custstatus", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("custtype", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("prodtype", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("internetType", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("vpdnOnly", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("isCommon", DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType( structFields );
        return structType;
    }
    public static Row parseLine(Row line,String vpdndomain,String apn){
        return RowFactory.create(line.getString(0),line.getString(1),line.getString(2),line.getString(3),vpdndomain, apn, line.getString(5),line.getString(6),line.getString(7),line.getString(8),
                line.getString(9),line.getString(10),line.getString(11),line.getString(12),line.getString(13),line.getString(14),line.getString(15),line.getString(16),line.getString(17));
    }
    public static StructType parseUserComparyType(){
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("mdn", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("imsicdma", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("imsilte", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("companycode", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("vpdndomain", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("apn", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("isvpdn", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("isdirect", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("userstatus", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("atrbprovince", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("userprovince", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("belo_city", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("belo_prov", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("custstatus", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("custtype", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("prodtype", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("internetType", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("vpdnOnly", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("isCommon", DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType( structFields );
        return structType;
    }
    public static void main(String[] args) {
        String p = "861064956743088|||||||||0|20170830174733|||||1||";
        String[] s = p.split("\\|",26);
        System.out.println(s.length);
    }
}
