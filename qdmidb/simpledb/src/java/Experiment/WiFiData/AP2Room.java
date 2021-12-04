package Experiment.WiFiData;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AP2Room {
    public static List<String> aps;
    public static Map<String, List<String>> apMapRoom;
    static final String path = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/ap2room.dat";
    private static String serverDatabase = "tippersdb_restored";

    public static List<String> getRooms(String ap){
        if(apMapRoom.containsKey(ap)){
            return apMapRoom.get(ap);
        }
        return null;
    }

    public static void init() {
        aps = new ArrayList<>();
        apMapRoom = new HashMap<>();
        try (Connect connect = new Connect("server", serverDatabase)) {
            Connection connection = connect.getConnection();
            Statement st1 = connection.createStatement();
            ResultSet rs1 = st1.executeQuery("select id from SENSOR where sensor_type_id = 1;");
            while (rs1.next()) {
                aps.add(rs1.getString(1));
            }
            PreparedStatement ps1 = connection.prepareStatement(
                    "select INFRASTRUCTURE.name from SENSOR, COVERAGE_INFRASTRUCTURE, INFRASTRUCTURE\n"
                            + "where SENSOR.id = ? and SENSOR.COVERAGE_ID = COVERAGE_INFRASTRUCTURE.id and COVERAGE_INFRASTRUCTURE.SEMANTIC_ENTITY_ID = INFRASTRUCTURE.SEMANTIC_ENTITY_ID;");
            for (String ap : aps) {
                ps1.setString(1, ap);
                ResultSet rs2 = ps1.executeQuery();
                List<String> rooms = new ArrayList<>();
                while (rs2.next()) {
                    rooms.add(rs2.getString(1));
                }
                apMapRoom.put(ap, rooms);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path))) {
            oos.writeObject(apMapRoom);
            System.out.println("Successfully write to disk.");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void load() {
        try (ObjectInputStream ooi = new ObjectInputStream(new FileInputStream(path))) {
            apMapRoom = (Map<String, List<String>>) ooi.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void print() {
        System.out.println("AP number: " + apMapRoom.size());
    }

    public static List<String> find(String ap) {
        return apMapRoom.get(ap);
    }
}
