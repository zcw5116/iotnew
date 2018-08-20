package com.zyuc.stat.nbiot.analysis.tiDBmanager;

import java.io.IOException;
import java.sql.*;
import java.util.Date;

/**
 * Created by liuzk on 18-7-18.
 */
public class AnalysisMonitor_java {
    public static void main(String[] arg) {

        String jdbcDriver = "oracle.jdbc.driver.OracleDriver";
        String jdbcUrl = "jdbc:oracle:thin:@59.43.49.70:20006:dbnms";
        String jdbcUser = "epcslview";
        String jdbcPassword = "epc_slview129";

        PreparedStatement pstm = null;
        PreparedStatement pstmUpdate = null;
        // 创建一个结果集对象
        //ResultSet rs = null;
        Connection connection = null;
        try {
            Class.forName(jdbcDriver);
            connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
            System.out.println("成功连接数据库");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("class not find !", e);
        } catch (SQLException e2) {
            throw new RuntimeException("get connection error!", e2);
        }

        //任务发起时间  == 提交Spark任务的时间
        Date beginTime = new Date();
        String year = String.format("%tY", beginTime);
        String month = String.format("%tm", beginTime);
        String day = String.format("%td", beginTime);
        String hour = String.format("%tH", beginTime);
        String minute = String.format("%tM", beginTime);

        String starttime = year + month + day + hour + minute;
        System.out.println(starttime);//201807191010

        String endtime = new String("");
        String view = new String("m5");
        String sparkAppName = new String("NbCdrM5Analysis");
        String inputtime = String.valueOf(Long.parseLong(starttime)-15);
        int time1 = 1;

        String sqlStr = "insert into IOTMonitor values(?,?,?,?,?,?)";

        try {
            // 执行插入数据操作
            pstm = connection.prepareStatement(sqlStr);
            pstm.setString(1, starttime);
            pstm.setString(2, endtime);
            pstm.setString(3, view);
            pstm.setString(4, sparkAppName);
            pstm.setString(5, inputtime);
            pstm.setInt(6, time1);
            pstm.executeUpdate();

            // 调用 shell
            String shpath="/home/hadoop/IdeaProjects/iotnew/aaa.sh";
            Process ps = null;
            try {
                ps = Runtime.getRuntime().exec(shpath);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                ps.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // 获取结束时间 更新 endtime
            Date endTime = new Date();
            System.out.println(endTime);
            String endYear = String.format("%tY", endTime);
            String endMonth = String.format("%tm", endTime);
            String endDay = String.format("%td", endTime);
            String endHour = String.format("%tH", endTime);
            String endMinute = String.format("%tM", endTime);
            String lastTime = endYear + endMonth + endDay + endHour + endMinute;
            String updateEndtimeSQL = "update IOTMonitor set endtime=? where inputtime=? and sparkAppName=? and times=?";

            pstmUpdate = connection.prepareStatement(updateEndtimeSQL);
            pstmUpdate.setString(1,lastTime);
            pstmUpdate.setString(2,inputtime);
            pstmUpdate.setString(3,sparkAppName);
            pstmUpdate.setInt(4,time1);
            pstmUpdate.executeUpdate();




        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (pstmUpdate != null) {
                try {
                    pstmUpdate.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pstm != null) {
                try {
                    pstm.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }




        ////
    }
}
