package cn.mwee;

import java.sql.*;
import java.util.*;

/**
 * @author tangyu
 * @since 2018-06-15 12:45
 */
public class Db {
    protected Connection getJdbc() throws ClassNotFoundException, SQLException {


        String url = "jdbc:mysql://xx.x.xx.xx:3306/db_name?tinyInt1isBit=false&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
        String user = "db_user";
        String pass = "db_pass";

        Class.forName("com.mysql.jdbc.Driver");

        Connection connection = DriverManager.getConnection(url, user, pass);
        connection.setAutoCommit(false);

        return connection;
    }

    protected int delete(String sql, Object[] args) {
        return this.update(sql, args);
    }

    protected int update(String sql, Object[] args) {
        Connection jdbc = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            jdbc = this.getJdbc();
            preparedStatement = jdbc.prepareStatement(sql);
            if (Objects.nonNull(args)) {
                for (int i = 0; i < args.length; i++) {
                    preparedStatement.setObject(i + 1, args[i]);
                }
            }
            int i = preparedStatement.executeUpdate();
            jdbc.commit();
            return i;

        } catch (Exception e) {
            e.printStackTrace();
            if (Objects.nonNull(jdbc)) {
                try {
                    jdbc.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
        } finally {
            close(preparedStatement, jdbc, rs);
        }
        return 0;
    }

    protected List<Map<String, Object>> getReseult(String sql) {
        return getReseult(sql, null);
    }

    protected List<Map<String, Object>> getReseult(String sql, Object[] args) {


        List<Map<String, Object>> mapList = new ArrayList<>();
        List<String> columsName = new ArrayList<>();
        Connection jdbc = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            jdbc = this.getJdbc();
            preparedStatement = jdbc.prepareStatement(sql);
            if (Objects.nonNull(args)) {
                for (int i = 0; i < args.length; i++) {
                    preparedStatement.setObject(i + 1, args[i]);
                }
            }
            rs = preparedStatement.executeQuery();
            ResultSetMetaData rsm = rs.getMetaData();
            int columeSize = rsm.getColumnCount();
            for (int i = 0; i < columeSize; i++) {
                String columnName = rsm.getColumnName(i + 1);
                columsName.add(columnName);
            }
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (String label : columsName) {
                    Object value = rs.getObject(label);
                    row.put(label, value);
                }
                mapList.add(row);
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            close(preparedStatement, jdbc, rs);
        }
        return mapList;
    }

    protected void close(Statement statement, Connection conn, ResultSet rs) {
        //            preparedStatement.close();
//            connection.close();
        if (Objects.nonNull(rs)) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (Objects.nonNull(statement)) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (Objects.nonNull(conn)) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }

}
