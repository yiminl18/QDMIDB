package Experiment.Simulation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
/*
    generate simulated WiFi dataset
 */
public class WiFi {
    public static final int MISSING_INTEGER = Integer.MIN_VALUE;
    List<Integer> roomPool = new ArrayList<>();
    List<Integer> buildingPool = new ArrayList<>();
    List<Integer> floorPool = new ArrayList<>();
    List<Integer> regionPool = new ArrayList<>();
    List<Integer> capacityPool = new ArrayList<>();

    static class spaceTuple{
        int room, building, floor, region, capacity, size, type;

        public int getRegion() {
            return this.region;
        }

        public spaceTuple(int room, int building, int floor, int region, int capacity, int size, int type){
            this.room = room;
            this.building = building;
            this.floor = floor;
            this.region = region;
            this.capacity = capacity;
            this.size = size;
            this.type = type;
        }

        public int getBuilding() {
            return building;
        }

        public int getFloor() {
            return floor;
        }

        public int getRoom(){
            return this.room;
        }

        public void setRoom(int room){
            this.room = room;
        }

        public void setType(int type){
            this.type = type;
        }

        public void setSize(int size){
            this.size = size;
        }
    }

    List<spaceTuple> spaceTuples = new ArrayList<>();

    public void readRoomPool(){
        //read from "roomPool.txt"
        String fileSpace = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/space.csv";//raw dirty
        try{
            BufferedReader csvReader = new BufferedReader(new FileReader(fileSpace));
            int count = 0;
            String row;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String[] data = row.split(",");
                String room = data[0];
                roomPool.add(room.hashCode());
            }
            csvReader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readSpacePlus(){
        //"region","building","floor","capacity"
        Random rand = new Random();
        String fileSpace = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/space+.csv";//raw dirty
        try{
            BufferedReader csvReader = new BufferedReader(new FileReader(fileSpace));
            int count = 0;
            String row;
            int building, floor, capacity;
            int nullCapacity = 0;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String[] data = row.split(",");
                String region = data[0];
                region = region.substring(1,region.length()-1);
                if(data[1].equals("NULL")){
                    building = MISSING_INTEGER;
                }else{
                    building = Integer.valueOf(data[1]);
                }
                if(data[2].equals("NULL")){
                    floor = MISSING_INTEGER;
                }else{
                    floor = Integer.valueOf(data[2]);
                }
                if(data[3].equals("NULL")){
                    capacity = MISSING_INTEGER;
                    nullCapacity ++;
                }else{
                    capacity = Integer.valueOf(data[3]);
                }
                regionPool.add(region.hashCode());
                buildingPool.add(building);
                floorPool.add(floor);
                capacityPool.add(capacity);
                spaceTuple st = new spaceTuple(0,building,floor, region.hashCode(), capacity,-1,-1);
                spaceTuples.add(st);
                //System.out.println(region + " " + floor + " " + building + " " + capacity);
            }
            System.out.println("percentage of null capacity: " + nullCapacity + " " + spaceTuples.size());
            csvReader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setRoom(int room, int min, int max){
        while(true) {
            int num = ThreadLocalRandom.current().nextInt(min, max);
            if (spaceTuples.get(num).getRoom() != MISSING_INTEGER && spaceTuples.get(num).getRegion() != MISSING_INTEGER) {
                spaceTuples.get(num).setRoom(room);
                break;
            }
        }
    }

    public void setType(){
        Random rand = new Random();
        double missingrate = 0.6;
        for(int i=0;i<spaceTuples.size();i++){
            int n = rand.nextInt(100);
            if(n < missingrate*100.0){
                spaceTuples.get(i).setType(MISSING_INTEGER);
                continue;
            }
            int num = ThreadLocalRandom.current().nextInt(0, 20);
            spaceTuples.get(i).setType(num);
        }
    }

    public void setSize(){
        double missingrate = 0.7;
        Random rand = new Random();
        for(int i=0;i<spaceTuples.size();i++){
            int n = rand.nextInt(100);
            if(n < missingrate*100.0){
                spaceTuples.get(i).setSize(MISSING_INTEGER);
                continue;
            }
            if(spaceTuples.get(i).getRoom() != MISSING_INTEGER){
                //room
                spaceTuples.get(i).setSize(ThreadLocalRandom.current().nextInt(10, 50));
            }else if(spaceTuples.get(i).getRegion() == MISSING_INTEGER && spaceTuples.get(i).getFloor() != MISSING_INTEGER){
                //floor
                spaceTuples.get(i).setSize(ThreadLocalRandom.current().nextInt(300, 600));
            }else if(spaceTuples.get(i).getRegion() != MISSING_INTEGER){
                //region
                spaceTuples.get(i).setSize(ThreadLocalRandom.current().nextInt(40, 100));
            }else if(spaceTuples.get(i).getBuilding()!= MISSING_INTEGER && spaceTuples.get(i).getFloor() == MISSING_INTEGER){
                //building
                spaceTuples.get(i).setSize(ThreadLocalRandom.current().nextInt(300, 600));
            }else{
                spaceTuples.get(i).setSize(ThreadLocalRandom.current().nextInt(30, 100));
            }
        }
    }

    public void writeFile(String path, List<Integer> values){

    }

    public void simulate(){
        //read spaces
        readSpacePlus();
        //add rooms
        for(int i=0;i<roomPool.size();i++){
            for(int j=0;j<3;j++){
                setRoom(roomPool.get(i),0,spaceTuples.size());
            }
        }
        //set type
        setType();
        //set size
        setSize();

    }


}
