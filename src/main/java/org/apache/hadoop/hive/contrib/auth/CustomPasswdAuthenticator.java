package org.apache.hadoop.hive.contrib.auth;

import javax.security.sasl.AuthenticationException;

import java.sql.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.slf4j.Logger;


/*
public class CustomPasswdAuthenticator implements PasswdAuthenticationProvider{

    private Logger LOG = org.slf4j.LoggerFactory.getLogger(CustomPasswdAuthenticator.class);

    private static final String HIVE_JDBC_PASSWD_AUTH_PREFIX="hive.jdbc_passwd.auth.%s";

    private Configuration conf=null;

    @Override
    public void Authenticate(String userName, String passwd)
            throws AuthenticationException {
        LOG.info("user: "+userName+" try login.");
        String passwdConf = getConf().get(String.format(HIVE_JDBC_PASSWD_AUTH_PREFIX, userName));
        if(passwdConf==null){
            String message = "user's ACL configration is not found. user:"+userName;
            LOG.info(message);
            throw new AuthenticationException(message);
        }
        if(!passwd.equals(passwdConf)){
            String message = "user name and password is mismatch. user:"+userName;
            LOG.info(message);
            throw new AuthenticationException(message);
        }
    }

    public Configuration getConf() {
        if(conf==null){
            this.conf=new Configuration(new HiveConf());
        }
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf=conf;
    }

}
*/

public class CustomPasswdAuthenticator implements PasswdAuthenticationProvider{

    private Logger LOG = org.slf4j.LoggerFactory.getLogger(CustomPasswdAuthenticator.class);

    @Override
    public void Authenticate(String username, String password) throws AuthenticationException {
        HiveConf hiveConf = new HiveConf();
        Configuration conf = new Configuration(hiveConf);
        String filePath = conf.get("hive.server2.custom.authentication.jdbc.config.path");
        //String filePath = "custom.auth.jdbc.properties";
        LOG.info("hive.server2.custom.authentication.jdbc.config.path = " + filePath);

        /*
        通过查找数据库记录，判断用户密码是否正确
         */

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Boolean flag = false;
        MySqlJdbcUtils jdbcUtils = new MySqlJdbcUtils(filePath);
        try {
            conn = jdbcUtils.getConnection();

            String sql = "select " +
                    "hive_user_name, " +
                    "hive_password " +
                    " FROM " +
                    "     hive_info" +
                    " WHERE " +
                    "     hive_user_name = ?  and hive_password = ?";

            pstmt = conn.prepareStatement(sql);
            /*配置绑定变量，username,password替换?*/
            pstmt.setString(1,username);
            pstmt.setString(2,password);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String name = rs.getString("hive_user_name");
                String pwd = rs.getString("hive_password");
                if (name.equals(username) && pwd.equals(password)) {
                    flag = true;
                }
            }

        } catch (Exception e) {
            throw new AuthenticationException("认证hive用户名和密码错误", e);
        } finally {
            jdbcUtils.release(rs,pstmt,conn);

        }

        if (!flag) {
            throw new AuthenticationException("认证hive用户名和密码错误");
        }
    }


}
