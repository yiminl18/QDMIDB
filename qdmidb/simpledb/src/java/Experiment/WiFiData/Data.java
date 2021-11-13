package Experiment.WiFiData;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;

public class Data {
    private static String serverDatabase = "tippersdb_restored";
    private static String localDatabase = "enrichdb";
    public static final int NULL_INTEGER = Integer.MIN_VALUE+1;
    public static final int MISSING_INTEGER = Integer.MIN_VALUE;
    private static final double missingRate = 0.8;
    private List<String> macPool = new ArrayList<>(), buildingPool = new ArrayList<>();
    private int macLength = 42;
    //first key is tid, starting from 0 for each relation
    //second key is field index to its imputed value
    private HashMap<Integer, HashMap<Integer, Integer>> imputedUser = new HashMap<>();
    private HashMap<Integer, HashMap<Integer, Integer>> imputedSpace = new HashMap<>();
    private HashMap<Integer, HashMap<Integer, Integer>> imputedWiFi = new HashMap<>();

    public String emailToMac(String email) {
        Connect connectServer = new Connect("server", serverDatabase);// OBSERVATION
        Connection serverConnection = connectServer.getConnection();
        ResultSet rs;
        String mac = "NULL";
        String temp = "NULL";
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
        String fileUserOut = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/userHash.txt";

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
        String fileSpaceOut = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/spaceHash.txt";
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

    public String ap2Room(String ap){
        Random rand = new Random();
        String room;
        int n = rand.nextInt(100);
        if(n < missingRate*100.0){
            room = "NULL";
        }
        else{
            List<String> rooms = AP2Room.getRooms(ap);
            int m = rand.nextInt(rooms.size());
            room = rooms.get(m);
        }
        return room;
    }

    public String getImputedRoom(String ap){
        Random rand = new Random();
        String room;
        List<String> rooms = AP2Room.getRooms(ap);
        int m = rand.nextInt(rooms.size());
        room = rooms.get(m);
        return room;
    }

    public void processWiFiData(){
        //read raw wifi data and generate the hash codes of dataset
        AP2Room.load();
        String fileWiFi = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/wifi.csv";
        String fileWiFiOut = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/wifiHash.txt";
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
                if(payload.length()-2 < 14){
                    continue;
                }
                String mac = payload2Mac(payload);
                String room = ap2Room(sensorID);
                int roomCode = room.hashCode();
                if(room.equals("NULL")){
                    roomCode = MISSING_INTEGER;
                }
                String output = mac.hashCode() + "," + timeStamp.hashCode() + "," + roomCode;
                bw.write(output);
                bw.newLine();
            }
            System.out.println(count);
            bw.flush();
            bw.close();
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int HOTDECKexist(int range, Random seed){
        return seed.nextInt(range);
    }

    public String HOTDECKnew(int length){
        return RandomStringUtils.randomAlphabetic(length);
    }

    public void loadMapForOneTuple(String relationName, int tid, int fieldIndex, int imputedValue){
        HashMap<Integer, Integer> mp = new HashMap<>();
        mp.put(fieldIndex, imputedValue);
        if(relationName.equals("User")){
            imputedUser.put(tid,mp);
        }else if(relationName.equals("Space")){
            imputedSpace.put(tid,mp);
        }else if(relationName.equals("WiFi")){
            imputedWiFi.put(tid,mp);
        }else{
            System.out.println("relationName not found!");
        }
    }

    public void writeImputedUser(){
        //read raw user data and generate the hash codes of dataset
        //note that hotdeck uses random function inside, so running this function needs to reload dataset to database
        String fileUser = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/user.csv";
        String fileUserOut = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/userImputed.txt";

        try {
            File f = new File(fileUser);
            if(f.exists()){
                System.out.println(fileUserOut + " exists!");
                return;
            }
            BufferedReader csvReader = new BufferedReader(new FileReader(fileUser));
            String row;
            FileWriter out = new FileWriter(fileUserOut);
            BufferedWriter bw = new BufferedWriter(out);
            int count = 0;
            int tid = 0;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String[] data = row.split(",");
                String name = data[0];
                String email = data[1];
                String mac = emailToMac(data[1]);
                if(mac.equals("NULL")){
                    mac = HOTDECKnew(macLength);
                    loadMapForOneTuple("User",tid,2,mac.hashCode());
                }else{
                    macPool.add(mac);
                }
                String output = name + "," + email + "," + '"' + mac + '"';
                bw.write(output);
                bw.newLine();
                tid++;
            }
            bw.flush();
            bw.close();
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeImputedSpace(){
        //read raw space data and generate the hash codes of dataset
        //note that hotdeck uses random function inside, so running this function needs to reload dataset to database
        String fileSpace = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/space.csv";
        String fileSpaceOut = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/spaceImputed.txt";
        Random rand = new Random();
        try {
            File f = new File(fileSpaceOut);
            if(f.exists()){
                System.out.println(fileSpaceOut + " exists!");
                return;
            }
            BufferedReader csvReader = new BufferedReader(new FileReader(fileSpace));
            String row;
            FileWriter out = new FileWriter(fileSpaceOut);
            BufferedWriter bw = new BufferedWriter(out);
            //name, floor, building
            int count = 0;
            int tid = 0;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String[] data = row.split(",");
                String room = data[0];
                String floor = data[1];
                String building = data[2];
                if(building.equals("NULL")){
                    building = buildingPool.get(HOTDECKexist(buildingPool.size(),rand));
                    loadMapForOneTuple("Space",tid,2,building.hashCode());
                }else{
                    buildingPool.add(building);
                }
                String output = room + "," + floor + "," + building;
                bw.write(output);
                bw.newLine();
                tid++;
            }
            bw.flush();
            bw.close();
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeImputedWiFi(){
        //read raw wifi data and generate the hash codes of dataset
        //note that hotdeck uses random function inside, so running this function needs to reload dataset to database
        AP2Room.load();
        String fileWiFi = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/wifi.csv";
        String fileWiFiOut = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/wifiImputed.txt";
        try {
            File f = new File(fileWiFiOut);
            if(f.exists()){
                System.out.println(fileWiFiOut + " exists!");
                return;
            }
            BufferedReader csvReader = new BufferedReader(new FileReader(fileWiFi));
            String row;
            FileWriter out = new FileWriter(fileWiFiOut);
            BufferedWriter bw = new BufferedWriter(out);
            //payload,timeStamp,sensor_id
            int count = 0;
            int tid = 0;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String[] data = row.split(",");
                String payload = data[0];
                String timeStamp = data[1];
                String sensorID = data[2];
                if(payload.length()-2 < 14){
                    continue;
                }
                String mac = payload2Mac(payload);
                String room = ap2Room(sensorID);
                if(room.equals("NULL")){
                    room = getImputedRoom(sensorID);
                    loadMapForOneTuple("WiFi",tid,2,room.hashCode());
                }
                String output = mac + "," + timeStamp + "," + room;
                bw.write(output);
                bw.newLine();
                tid ++;
            }
            //System.out.println(count);
            bw.flush();
            bw.close();
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
