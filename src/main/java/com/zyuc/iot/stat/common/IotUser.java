package com.zyuc.iot.stat.common;

import com.zyuc.iot.utils.JDBCToHiveUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by zhoucw on 17-11-2.
 */
public class IotUser {

    private static Connection conn = JDBCToHiveUtils.getConnnection();
    private static PreparedStatement ps;
    private static ResultSet rs;

    public static void main(String[] args) {
        if(args.length <1){
            System.out.println("请输入日期：");
            System.exit(-1);
        }
        String dayid = args[0];

        getServUserNum(dayid);

    }

    public static void getServUserNum(String dayid) {
        String sql = "select count(*) as usernum, " +
                "sum(case when isdirect='1' then 1 else 0 end) dnum, " +
                "sum(case when isvpdn='1' then 1 else 0 end) cnum " +
                "from iot_basic_userinfo where d='" + dayid + "'";
        System.out.println(sql);
        try {
            ps = JDBCToHiveUtils.prepare(conn, sql);
            rs = ps.executeQuery();
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columns; i++) {
                    System.out.print(rs.getString(i));
                    System.out.print("\t\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
