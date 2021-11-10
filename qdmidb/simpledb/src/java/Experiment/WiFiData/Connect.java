package Experiment.WiFiData;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Connect implements AutoCloseable {
    Connection connection;

    public Connect(String type, String database) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            // use local database only for now
            /*
             * if (type.equals("server")) { System.out.println("server"); connection =
             * DriverManager.getConnection(
             * "jdbc:mysql://sensoria-mysql.ics.uci.edu:3306/tippersdb_restored?useSSL=false&serverTimezone=PST",
             * "tippersUser", "tippers2018"); System.out.println("***"); } if
             * (type.equals("local")) {
             */
            String user = null, pwd = null;

            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("credential.txt")))) {
                user = br.readLine();
                pwd = br.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            connection = DriverManager.getConnection(
                    String.format("jdbc:mysql://sensoria-mysql.ics.uci.edu:3306/%s?useSSL=false&serverTimezone=PST", database), user,
                    pwd);
            // }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() {
        // System.out.println("successful");
        return connection;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}