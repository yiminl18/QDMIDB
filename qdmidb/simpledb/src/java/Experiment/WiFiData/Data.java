package Experiment.WiFiData;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class Data {
    private static String serverDatabase = "tippersdb_restored";
    private static String localDatabase = "enrichdb";
    public static final int NULL_INTEGER = Integer.MIN_VALUE+1;
    public static final int MISSING_INTEGER = Integer.MIN_VALUE;

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

    public void processUser(){
        //read raw user data and generate the hash codes of dataset
        String fileUser = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/user.csv";
        String fileUserOut = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/userHash.csv";

        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(fileUser));
            String row;
            FileWriter out = new FileWriter(fileUserOut);
            BufferedWriter bw = new BufferedWriter(out);
            int count = 0;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String[] data = row.split(",");
                String name = data[0];
                String email = data[1];
                String mac = emailToMac(data[1]);
                System.out.println(name.hashCode() + " " + email.hashCode() + " " + emailToMac(email).hashCode());
                String output = String.valueOf(name.hashCode()) + "," + String.valueOf(email.hashCode()) + "," + String.valueOf(mac.hashCode());
                bw.write(output);
                bw.newLine();
            }
            bw.flush();
            bw.close();
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void processSpace(){
        //read raw space data and generate the hash codes of dataset
        String fileSpace = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/space.csv";
        String fileSpaceOut = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/spaceHash.csv";
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(fileSpace));
            String row;
            FileWriter out = new FileWriter(fileSpaceOut);
            BufferedWriter bw = new BufferedWriter(out);
            //name, floor, building
            int count = 0;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String[] data = row.split(",");
                String room = data[0];
                String floor = data[1];
                String building = data[2];
                String output = String.valueOf(room.hashCode()) + "," + String.valueOf(floor.hashCode()) + "," + String.valueOf(building.hashCode());
                bw.write(output);
                bw.newLine();
            }
            bw.flush();
            bw.close();
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String payload2Mac(String payload) {
        return payload.substring(14, payload.length()-2);
    }

    public void processWiFiData(){
        //read raw wifi data and generate the hash codes of dataset
        String fileWiFi = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/wifi.csv";
        String fileWiFiOut = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/wifiHash.csv";
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(fileWiFi));
            String row;
            FileWriter out = new FileWriter(fileWiFiOut);
            BufferedWriter bw = new BufferedWriter(out);
            //payload,timeStamp,sensor_id
            int count = 0;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String[] data = row.split(",");
                String payload = data[0];
                String timeStamp = data[1];
                String sensorID = data[2];
                String mac = payload2Mac(payload);
                System.out.println(mac + "," + timeStamp + "," + sensorID);
                if(count>=100){
                    break;
                }
            }
            bw.flush();
            bw.close();
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void testHashCode(){
        String a = "akdjflkajs";
        String b = "123345";
        String c = "12222";
        System.out.println(a.hashCode() + " " + b.hashCode() + " " + c.hashCode());
        System.out.println(a.hashCode() + " " + b.hashCode() + " " + c.hashCode());
    }
}
