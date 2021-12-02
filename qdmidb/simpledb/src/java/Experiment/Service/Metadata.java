package Experiment.Service;
/*
    this class provides common services to process different data sets
 */

import java.io.*;

public class Metadata {
    private static final int MISSING_INTEGER = Integer.MIN_VALUE;
    private static final int numOfRelation = 3;

    public void getSchema(){
        //automatically generate schema and statistics from raw data given catalog file

    }

    public void generateSchema(){//this is old
        //read all relations sequentially
        String fileWiFi = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/wifiHash.txt";
        String fileUser = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/userHash.txt";
        String fileSpace = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/spaceHash.txt";
        String fileSchema = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/schema.txt";

        int cardinality = 0;

        try {
            FileWriter out = new FileWriter(fileSchema);
            BufferedWriter bw = new BufferedWriter(out);
            bw.write(String.valueOf(numOfRelation));
            bw.newLine();

            //process space: room,floor,building
            int NULLroom=0, NULLfloor=0, NULLbuilding=0;
            BufferedReader spaceReader = new BufferedReader(new FileReader(fileSpace));
            String row;
            int count = 0;
            bw.write(String.valueOf(3));
            bw.newLine();
            while ((row = spaceReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                cardinality ++;
                String[] data = row.split(",");
                int room = Integer.valueOf(data[0]);
                int floor = Integer.valueOf(data[1]);
                int building = Integer.valueOf(data[2]);
                if(room == MISSING_INTEGER){
                    NULLroom +=1;
                }
                if(floor == MISSING_INTEGER){
                    NULLfloor +=1;
                }
                if(building == MISSING_INTEGER){
                    NULLbuilding +=1;
                }
            }

            bw.write("space.room");
            bw.newLine();

            bw.write(String.valueOf(cardinality) + ',' + String.valueOf(NULLroom));
            bw.newLine();

            bw.write("space.floor");
            bw.newLine();

            bw.write(String.valueOf(cardinality) + ',' + String.valueOf(NULLfloor));
            bw.newLine();

            bw.write("space.building");
            bw.newLine();

            bw.write(String.valueOf(cardinality) + ',' + String.valueOf(NULLbuilding));
            bw.newLine();

            spaceReader.close();

            //process users: name,email,mac
            bw.write(String.valueOf(3));
            bw.newLine();
            cardinality = 0;
            BufferedReader userReader = new BufferedReader(new FileReader(fileUser));
            count = 0;
            int NULLname=0,NULLemail=0,NULLmac=0;
            while ((row = userReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                cardinality ++;
                String[] data = row.split(",");
                int name = Integer.valueOf(data[0]);
                int email = Integer.valueOf(data[1]);
                int mac = Integer.valueOf(data[2]);
                if(name == MISSING_INTEGER){
                    NULLname ++;
                }
                if(email == MISSING_INTEGER){
                    NULLemail ++;
                }
                if(mac == MISSING_INTEGER){
                    NULLmac ++;
                }
            }
            bw.write("users.name");
            bw.newLine();

            bw.write(String.valueOf(cardinality) + ',' + String.valueOf(NULLname));
            bw.newLine();

            bw.write("users.email");
            bw.newLine();

            bw.write(String.valueOf(cardinality) + ',' + String.valueOf(NULLemail));
            bw.newLine();

            bw.write("users.mac");
            bw.newLine();

            bw.write(String.valueOf(cardinality) + ',' + String.valueOf(NULLmac));
            bw.newLine();
            userReader.close();

            //process wifi: mac, time, room
            bw.write(String.valueOf(3));
            bw.newLine();
            cardinality = 0;
            BufferedReader wifiReader = new BufferedReader(new FileReader(fileWiFi));
            count = 0;
            int NULLmacs=0,NULLtime=0,NULLroomWifi=0;
            while ((row = wifiReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                cardinality ++;
                String[] data = row.split(",");
                int macs = Integer.valueOf(data[0]);
                int time = Integer.valueOf(data[1]);
                int roomWifi = Integer.valueOf(data[2]);
                if(macs == MISSING_INTEGER){
                    NULLmacs ++;
                }
                if(time == MISSING_INTEGER){
                    NULLtime ++;
                }
                if(roomWifi == MISSING_INTEGER){
                    NULLroomWifi ++;
                }
            }
            bw.write("wifi.mac");
            bw.newLine();

            bw.write(String.valueOf(cardinality) + ',' + String.valueOf(NULLmacs));
            bw.newLine();

            bw.write("wifi.time");
            bw.newLine();

            bw.write(String.valueOf(cardinality) + ',' + String.valueOf(NULLtime));
            bw.newLine();

            bw.write("wifi.room");
            bw.newLine();

            bw.write(String.valueOf(cardinality) + ',' + String.valueOf(NULLroomWifi));
            bw.newLine();
            wifiReader.close();

            bw.flush();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
