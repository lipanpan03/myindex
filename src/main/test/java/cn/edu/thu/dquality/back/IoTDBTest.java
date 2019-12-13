package cn.edu.thu.dquality.back;

import cn.edu.thu.dquality.back.utility.IoTDBIterator;
import junit.framework.TestCase;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class IoTDBTest extends TestCase{

    @Test
    public void testIoTDB(){
        IoTDBIterator it = new IoTDBIterator("root.test");
        String[] cols = it.getColsName();
        for (String c: cols) {
            System.out.print(c + " ");
        }
        System.out.println();
        String line;
        while((line = it.next()) != null){
            System.out.println(line);
        }
    }

    //Below are methods about uploading a dataset to IoTDB from a local file.
    private static void initGroup(Statement statement) throws SQLException {
        //Create storage group
        try {
            statement.execute("SET STORAGE GROUP TO root.test");
        }catch (IoTDBSQLException e){
            System.out.println(e.getMessage());
        }

        //Show storage group
        statement.execute("SHOW STORAGE GROUP");
        outputResult(statement.getResultSet());

        try {
            statement.execute("CREATE TIMESERIES root.test.value WITH DATATYPE=DOUBLE,ENCODING=PLAIN;");
        }catch (IoTDBSQLException e){
            System.out.println(e.getMessage());
        }
        //Show time series
        statement.execute("SHOW TIMESERIES root.test");
        outputResult(statement.getResultSet());
        //Show devices
        statement.execute("SHOW DEVICES");
        outputResult(statement.getResultSet());
    }

    private static void insertTestData(Statement statement, String filePath) throws SQLException {
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line = br.readLine();
            while((line = br.readLine()) != null){
                String sql = "insert into root.test(timestamp,value) values(" + line + ");";
                System.out.println(sql);
                statement.addBatch(sql);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        statement.executeBatch();
        statement.clearBatch();
    }

    private static Connection getConnection() {
        // JDBC driver name and database URL
        String driver = "org.apache.iotdb.jdbc.IoTDBDriver";
        String url = "jdbc:iotdb://127.0.0.1:6667/";

        // Database credentials
        String username = "root";
        String password = "root";

        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    private static void outputResult(ResultSet resultSet) throws SQLException {
        if (resultSet != null) {
            System.out.println("--------------------------");
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                System.out.print(metaData.getColumnLabel(i + 1) + " ");
            }
            System.out.println();
            while (resultSet.next()) {
                for (int i = 1; ; i++) {
                    System.out.print(resultSet.getString(i));
                    if (i < columnCount) {
                        System.out.print(",");
                    } else {
                        System.out.println();
                        break;
                    }
                }
            }
            System.out.println("--------------------------\n");
        }
    }
}