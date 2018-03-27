package com.zyuc.iot.utils;

/**
 * Created by zhoucw on 17-11-2.
 */
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class QueryHiveUtils {
    private static Connection conn = JDBCToHiveUtils.getConnnection();
    private static PreparedStatement ps;
    private static ResultSet rs;

    public static void getAll(String tablename) {
        String sql = "select count(*) as cnt from " + tablename;
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

    public static void main(String[] args) {
        String tablename="tmp5";
        QueryHiveUtils.getAll(tablename);
    }

}