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
    private static final double missingRateRoom = 0.8, missingRateMac = 0.8, missingRateFloor = 0.2, missingRateBuilding = 0.4;
    private List<String> macPool = new ArrayList<>(), buildingPool = new ArrayList<>(), floorPool = new ArrayList<>();
    private int macLength = 42;
    //first key is tid, starting from 0 for each relation
    //second key is field index to its imputed value
    private HashMap<Integer, HashMap<Integer, Integer>> imputedUser = new HashMap<>();
    private HashMap<Integer, HashMap<Integer, Integer>> imputedSpace = new HashMap<>();
    private HashMap<Integer, HashMap<Integer, Integer>> imputedWiFi = new HashMap<>();
    //restore hash codes to original values later ihe: to do

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

    public String payload2Mac(String payload) {
        return payload.substring(15, payload.length()-3);
    }

    public String ap2Room(String ap){
        Random rand = new Random();
        String room;
        int n = rand.nextInt(100);
        if(n < missingRateRoom*100.0){
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

    public int HOTDECKexist(int range, Random seed){
        return seed.nextInt(range);
    }

    public String HOTDECKnew(int length){
        return RandomStringUtils.randomAlphabetic(length);
    }

    public void loadMapForOneTuple(String relationName, int tid, int fieldIndex, int imputedValue){
        //record imputed values

        if(relationName.equals("User")){
            if(imputedUser.containsKey(tid)){
                imputedUser.get(tid).put(fieldIndex, imputedValue);
            }else{
                HashMap<Integer, Integer> mp = new HashMap<>();
                mp.put(fieldIndex, imputedValue);
                imputedUser.put(tid,mp);
            }
        }else if(relationName.equals("Space")){
            if(imputedSpace.containsKey(tid)){
                imputedSpace.get(tid).put(fieldIndex, imputedValue);
            }else{
                HashMap<Integer, Integer> mp = new HashMap<>();
                mp.put(fieldIndex, imputedValue);
                imputedSpace.put(tid,mp);
            }
        }else if(relationName.equals("WiFi")){
            if(imputedWiFi.containsKey(tid)){
                imputedWiFi.get(tid).put(fieldIndex, imputedValue);
            }else{
                HashMap<Integer, Integer> mp = new HashMap<>();
                mp.put(fieldIndex, imputedValue);
                imputedWiFi.put(tid,mp);
            }
        }else{
            System.out.println("relationName not found!");
        }
    }

    public void writeImputedValues(String filePath, HashMap<Integer, HashMap<Integer, Integer>> imputedValues){
        //write imputed values to local disk
        try {
            FileWriter out = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(out);
            for(Map.Entry<Integer, HashMap<Integer, Integer>> entry: imputedValues.entrySet()){
                bw.write(String.valueOf(entry.getKey()));//write tid
                bw.newLine();
                bw.write(String.valueOf(entry.getValue().size()));//write size of imputed values in this tuple
                bw.newLine();
                for(Map.Entry<Integer, Integer> entry1: entry.getValue().entrySet()){
                    //write field index and imputed values
                    String o = String.valueOf(entry1.getKey()) + "," + String.valueOf(entry1.getValue());
                    bw.write(o);
                    bw.newLine();
                }
            }
            bw.flush();
            bw.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeImputedUser(){
        //read raw user data and generate the hash codes of dataset
        //note that hotdeck uses random function inside, so running this function needs to reload dataset to database
        String fileUser = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/user.csv";//raw dirty
        String fileUserOut = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/userClean.txt";//raw clean
        String fileUserHash = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/userHash.txt";//hash dirty
        String fileUserImputed = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/userImputedValues.txt";//hash of imputed values
        Random rand = new Random();

        try {
            File f = new File(fileUserOut);
            if(f.exists()){
                System.out.println(fileUserOut + " exists!");
                return;
            }
            BufferedReader csvReader = new BufferedReader(new FileReader(fileUser));
            String row;

            FileWriter out1 = new FileWriter(fileUserOut);
            BufferedWriter rawClean = new BufferedWriter(out1);

            FileWriter out2 = new FileWriter(fileUserHash);
            BufferedWriter hashDirty = new BufferedWriter(out2);

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
                int macCode = mac.hashCode();

                int n = rand.nextInt(100);
                if(n < missingRateMac*100.0){
                    mac = "NULL";
                }
                if(mac.equals("NULL")){
                    macCode = MISSING_INTEGER;
                    mac = HOTDECKnew(macLength);
                    loadMapForOneTuple("User",tid,2,mac.hashCode());
                }else{
                    macPool.add(mac);
                }
                String rawCleanString = name + "," + email + "," + '"' + mac + '"';
                rawClean.write(rawCleanString);
                rawClean.newLine();

                String hashDirtyString = name.hashCode() + "," + email.hashCode() + "," + macCode;
                hashDirty.write(hashDirtyString);
                hashDirty.newLine();
                tid++;
            }
            rawClean.flush();
            rawClean.close();
            hashDirty.flush();
            hashDirty.close();

            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        writeImputedValues(fileUserImputed, imputedUser);
    }

    public void writeImputedSpace(){
        //read raw space data and generate the hash codes of dataset
        //note that hotdeck uses random function inside, so running this function needs to reload dataset to database
        String fileSpace = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/space.csv";//raw dirty
        String fileSpaceOut = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/spaceClean.txt";//raw clean
        String fileSpaceHash = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/spaceHash.txt";//hash dirty
        String fileSpaceImputed = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/spaceImputedValues.txt";//hash of imputed values
        Random rand = new Random();
        try {
            File f = new File(fileSpaceOut);
            if(f.exists()){
                System.out.println(fileSpaceOut + " exists!");
                return;
            }
            BufferedReader csvReader = new BufferedReader(new FileReader(fileSpace));
            String row;
            FileWriter out1 = new FileWriter(fileSpaceOut);
            BufferedWriter rawClean = new BufferedWriter(out1);

            FileWriter out2 = new FileWriter(fileSpaceHash);
            BufferedWriter hashDirty = new BufferedWriter(out2);

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
                int n = rand.nextInt(100);
                if(n < missingRateBuilding*100.0){
                    building = "NULL";
                }
                int m = rand.nextInt(100);
                if(m < missingRateFloor*100.0){
                    floor = "NULL";
                }

                int buildingCode = building.hashCode();
                int floorCode = floor.hashCode();

                if(building.equals("NULL")){
                    buildingCode = MISSING_INTEGER;
                    if(buildingPool.size() == 0){
                        building = HOTDECKnew(10);
                    }else{
                        building = buildingPool.get(HOTDECKexist(buildingPool.size(),rand));
                    }

                    loadMapForOneTuple("Space",tid,2,building.hashCode());
                }else{
                    buildingPool.add(building);
                }

                if(floor.equals("NULL")){
                    floorCode = MISSING_INTEGER;
                    if(floorPool.size() == 0){
                        floor = HOTDECKnew(5);
                    }else{
                        floor = floorPool.get(HOTDECKexist(floorPool.size(),rand));
                    }
                    loadMapForOneTuple("Space",tid,1,floor.hashCode());
                }else{
                    floorPool.add(floor);
                }
                String rawCleanString = room + "," + floor + "," + building;
                rawClean.write(rawCleanString);
                rawClean.newLine();

                String hashDirtyString = room.hashCode() + "," + floorCode + "," + buildingCode;
                hashDirty.write(hashDirtyString);
                hashDirty.newLine();

                tid++;
            }
            rawClean.flush();
            rawClean.close();

            hashDirty.flush();
            hashDirty.close();

            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        writeImputedValues(fileSpaceImputed, imputedSpace);
    }

    public void writeImputedWiFi(){
        //read raw wifi data and generate the hash codes of dataset
        //note that hotdeck uses random function inside, so running this function needs to reload dataset to database
        AP2Room.load();
        String fileWiFi = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/wifi.csv";//raw dirty
        String fileWiFiOut = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/wifiClean.txt";//raw clean
        String fileWiFiHash = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/wifiHash.txt";//hash dirty
        String fileWiFiImputed = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/wifiImputedValues.txt";//hash of imputed values
        try {
            File f = new File(fileWiFiOut);
            if(f.exists()){
                System.out.println(fileWiFiOut + " exists!");
                return;
            }
            BufferedReader csvReader = new BufferedReader(new FileReader(fileWiFi));
            String row;
            FileWriter out1 = new FileWriter(fileWiFiOut);
            BufferedWriter rawClean = new BufferedWriter(out1);

            FileWriter out2 = new FileWriter(fileWiFiHash);
            BufferedWriter hashDirty = new BufferedWriter(out2);

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
                if(payload.length()-3 < 15){
                    continue;
                }
                String mac = payload2Mac(payload);
                String room = ap2Room(sensorID);
                int roomCode = room.hashCode();
                if(room.equals("NULL")){
                    roomCode = MISSING_INTEGER;
                    room = getImputedRoom(sensorID);
                    loadMapForOneTuple("WiFi",tid,2,room.hashCode());
                }
                String rawCleanString = mac + "," + timeStamp + "," + room;
                String dirtyHashString = mac.hashCode() + "," + timeStamp.hashCode() + "," + roomCode;
                rawClean.write(rawCleanString);
                rawClean.newLine();
                hashDirty.write(dirtyHashString);
                hashDirty.newLine();
                tid ++;
            }
            //System.out.println(count);
            rawClean.flush();
            rawClean.close();
            hashDirty.flush();
            hashDirty.close();
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        writeImputedValues(fileWiFiImputed, imputedWiFi);
    }
}
