package org.apache.hadoop.hive.contrib.auth;

import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;

/**
 * <p>
 * 功能：连接mysql的公共工具类
 * </p>
 *
 */
public class MySqlJdbcUtils {

    /** 数据库url **/
    private static String URL = null;
    /** 数据库用户名 **/
    private static String USER = null;
    /** 密码 **/
    private static String PWD = null;
    /** 数据库的driver **/
    private static String DRIVER_CLASS = null;

    public MySqlJdbcUtils(String jdbcConfigFile) {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream(jdbcConfigFile));

            URL = prop.getProperty("mysql.jdbc.url");
            USER = prop.getProperty("mysql.jdbc.user");
            PWD = prop.getProperty("mysql.jdbc.password");
            DRIVER_CLASS = prop.getProperty("mysql.jdbc.driverClass");

            //注册驱动
            Class.forName(DRIVER_CLASS);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * 获取与指定数据库的连接
     * @return 获取连接
     * @throws SQLException 获取连接异常
     */
    public Connection getConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(URL,USER,PWD);
        return connection;
    }

    /**
     * 释放资源
     * @param rs    :结果集对象
     * @param stmt  :Statement
     * @param conn  :连接
     */
    public void release(ResultSet rs, Statement stmt,Connection conn) {
        //判断结果集是否为空，如果不为空，关闭清空
        if (null != rs) {
            try {
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            rs = null;
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            stmt = null;
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            conn = null;
        }
    }

}