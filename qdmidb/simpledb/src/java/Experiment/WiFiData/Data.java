package Experiment.WiFiData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Data {
    private static String serverDatabase = "tippersdb_restored";
    private static String localDatabase = "enrichdb";

    public String emailToMac(String email) {
        Connect connectServer = new Connect("server", serverDatabase);// OBSERVATION
        Connection serverConnection = connectServer.getConnection();
        ResultSet rs;
        String mac = "null";
        String temp = "null";
        int c=0;
        //System.out.println(email);
        try {
            Statement stmtserver = serverConnection.createStatement();
            rs = stmtserver.executeQuery(String.format("select distinct SENSOR.id from USER, SENSOR where USER.email='%s'\n"
                    + "and USER.SEMANTIC_ENTITY_ID = SENSOR.USER_ID \n"
                    + "and (SENSOR.sensor_type_id = 3 or SENSOR.sensor_type_id is null)", email));
            while (rs.next()) {
                temp = rs.getString(1);
                c++;
                if(countFrequency(temp)>0){
                    mac = temp;
                }
            }
            if(c==1){
                mac=temp;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connectServer.close();
        return mac;
    }


    public Integer countFrequency(String mac){
        //count frequency of a device
        Connect connectServer = new Connect("server", serverDatabase);// OBSERVATION
        Connection serverConnection = connectServer.getConnection();
        ResultSet rs;
        int count=0;
        try {
            Statement stmtserver = serverConnection.createStatement();
            rs = stmtserver.executeQuery(String.format("select count(*) from OBSERVATION_CLEAN\n" +
                    "WHERE payload='%s' \n" +
                    "and timeStamp>='2018-01-01 00:00:00' \n" +
                    "and timeStamp<='2018-03-01 00:00:00'",mac));
            while (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connectServer.close();
        return count;
    }

    public void readUser(){
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/user.csv"));
            String row;
            int count = 0;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String[] data = row.split(",");
                System.out.println(data[0] + " " + data[1]);
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readSpace(){

    }

    public void readWiFiData(){

    }
}
