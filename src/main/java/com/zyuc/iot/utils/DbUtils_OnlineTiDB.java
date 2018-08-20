package com.zyuc.iot.utils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by zhoucw on 下午4:43.
 */
public  class DbUtils_OnlineTiDB {

    private static Config config;

    public static Connection getDBConnection(){
        config = ConfigFactory.load();
        return getDBConnection(config);
    }

    public static Connection getDBConnection(String configPath) {
        config = ConfigFactory.parseFile(new File(configPath));
        return getDBConnection(config);
    }

    private static Connection getDBConnection(Config config) {
        Connection dbConnection = null;

        String dbDriver = config.getString("onlinetidb.driver");
        String dbUrl = config.getString("onlinetidb.url");
        String dbUser = config.getString("onlinetidb.user");
        String dbPass = config.getString("onlinetidb.password");

        try {
            Class.forName(dbDriver);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }

        try {
            dbConnection = DriverManager.getConnection(
                    dbUrl, dbUser, dbPass);
            return dbConnection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return dbConnection;
    }




    public static void main(String[] args) {
       // System.out.println(DbUtils.getDBConnection("/hadoop/application.conf"));
        String a = null;
        if(a==null){
            System.out.println("a");
        }
    }
}
