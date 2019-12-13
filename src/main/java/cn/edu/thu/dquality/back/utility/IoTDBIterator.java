package cn.edu.thu.dquality.back.utility;

import java.sql.*;

public class IoTDBIterator implements DQDataIterator {
    private static Connection connection = null;
    private String[] colsName = null;
    private ResultSet resultSet = null;

    private static final String DEFAULT_DRIVER = "org.apache.iotdb.jdbc.IoTDBDriver";
    private static final String DEFAULT_URL = "jdbc:iotdb://127.0.0.1:6667/";
    private static final String DEFAULT_USERNAME = "root";
    private static final String DEFAULT_PASSWORD = "root";


    public IoTDBIterator(String dataset){
        this(DEFAULT_DRIVER, DEFAULT_URL, DEFAULT_USERNAME, DEFAULT_PASSWORD, dataset);
    }

    public IoTDBIterator(String driver, String url, String username, String password, String dataset){
        if(createConnection(driver, url, username, password)){
            String query = "select * from " + dataset;
            try {
                Statement statement = connection.createStatement();
                this.resultSet = statement.executeQuery(query);
                if(this.resultSet != null){
                    ResultSetMetaData metaData = this.resultSet.getMetaData();
                    this.colsName = new String[metaData.getColumnCount()];
                    for (int i = 0; i < this.colsName.length; ++i) {
                        this.colsName[i] = metaData.getColumnLabel(i + 1);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                close();
            }
        }
    }



    public boolean connected(){
        return connection != null;
    }

    public String[] getColsName() {
        return this.colsName;
    }

    public int getTimeColIndex() {
        //TODO
        return 0;
    }

    public int[] getNumericColsIndex() {
        //TODO
        return new int[0];
    }

    public String next() {
        try {
            if (this.resultSet != null && resultSet.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; ; ++i) {
                    line.append(resultSet.getString(i));
                    if (i < this.colsName.length) {
                        line.append(",");
                    } else {
                        return line.toString();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        close();

        return null;
    }

    private boolean createConnection(String driver, String url, String username, String password) {
        if(connection == null){
            try {
                Class.forName(driver);
                connection = DriverManager.getConnection(url, username, password);
            } catch (ClassNotFoundException | SQLException e) {
                e.printStackTrace();
            }
        }
        return this.connected();
    }

    private void close(){
        if (connection != null)
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }
}
