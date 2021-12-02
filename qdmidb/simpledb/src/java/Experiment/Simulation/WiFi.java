package Experiment.Simulation;

import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
/*
    generate simulated WiFi dataset
 */
public class WiFi {
    public static final int MISSING_INTEGER = Integer.MIN_VALUE;
    public static final int hashPrime = 98371;
    public static final int locationMIN = 1, locationMAX = 1000;
    List<Integer> roomPool = new ArrayList<>();
    List<Integer> buildingPool = new ArrayList<>();
    List<Integer> floorPool = new ArrayList<>();
    List<Integer> regionPool = new ArrayList<>();
    List<Integer> capacityPool = new ArrayList<>();
    List<Integer> macPool = new ArrayList<>();
    List<Integer> userMacPool = new ArrayList<>();

    /*static class spaceTuple{
        int room, building, floor, region, capacity, size, type;

        public int getRegion() {
            return this.region;
        }

        public List<Integer> getList(){
            List<Integer> values = new ArrayList<>();
            values.add(this.room);
            values.add(this.building);
            values.add(this.floor);
            values.add(this.region);
            values.add(this.capacity);
            values.add(this.size);
            values.add(this.type);
            return values;
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
    }*/

    static class wifiTuple{
        int st, et, mac, lid, duration;
        public wifiTuple(int st, int et, int mac, int lid, int duration){
            this.st = st;
            this.et = et;
            this.mac = mac;
            this.lid = lid;
            this.duration = duration;
        }

        public List<Integer> getList(){
            List<Integer> values = new ArrayList<>();
            values.add(st);
            values.add(et);
            values.add(mac);
            values.add(lid);
            values.add(duration);
            return values;
        }

        public int getSt() {
            return st;
        }

        public void setSt(int st) {
            this.st = st;
        }

        public int getEt() {
            return et;
        }

        public void setEt(int et) {
            this.et = et;
        }

        public int getMac() {
            return mac;
        }

        public void setMac(int mac) {
            this.mac = mac;
        }

        public int getLid() {
            return lid;
        }

        public void setLid(int lid) {
            this.lid = lid;
        }

        public int getDuration() {
            return duration;
        }

        public void setDuration(int duration) {
            this.duration = duration;
        }
    }

    static class userTuple{
        int mac, name, email, group;
        public userTuple(int mac, int name, int email, int group){
            this.mac = mac;
            this.name = name;
            this.email = email;
            this.group = group;
        }

        public List<Integer> getList(){
            List<Integer> values = new ArrayList<>();
            values.add(this.mac);
            values.add(this.name);
            values.add(this.email);
            values.add(this.group);
            return values;
        }

        public int getMac() {
            return mac;
        }

        public void setMac(int mac) {
            this.mac = mac;
        }

        public int getName() {
            return name;
        }

        public void setName(int name) {
            this.name = name;
        }

        public int getEmail() {
            return email;
        }

        public void setEmail(int email) {
            this.email = email;
        }

        public int getGroup() {
            return group;
        }

        public void setGroup(int group) {
            this.group = group;
        }
    }

    //List<spaceTuple> spaceTuples = new ArrayList<>();
    List<wifiTuple> wifiTuples = new ArrayList<>();
    List<userTuple> userTuples = new ArrayList<>();
    private final String occupancyPathQuip = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/spaceQuip.txt";
    private final String occupancyPathIDB = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/spaceIDB.txt";
    private final String wifiPathQuip = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/wifiQuip.txt";
    private final String wifiPathIDB = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/wifiIDB.txt";
    private final String userPathQuip = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/userQuip.txt";
    private final String userPathIDB = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/userIDB.txt";
    BufferedWriter occupancyQuip = null, occupancyIDB= null, wifiQuip = null, wifiIDB = null, userQuip = null, userIDB = null;

    public WiFi(String type){
        File f = null;
        switch (type){
            case "occupancy":
                f = new File(occupancyPathQuip);
                if(f.exists()){
                    System.out.println(occupancyPathQuip + " exists!");
                    return;
                }
                openFile(type);
                processOccupancy();
                closeFile(type);
                break;
            case "wifi":
                f = new File(wifiPathQuip);
                if(f.exists()){
                    System.out.println(wifiPathQuip + " exists!");
                    return;
                }
                openFile(type);
                processWiFi();
                closeFile(type);
                break;
            case "users":
                f = new File(userPathQuip);
                if(f.exists()){
                    System.out.println(userPathQuip + " exists!");
                    return;
                }
                openFile(type);
                processUsers();
                closeFile(type);
                break;
        }

    }

    public int rehash(int val){
//        if(val < 0){
//            val = -1*val;
//        }
        return val % hashPrime;
    }

    public void openFile(String type){
        switch (type){
            case "occupancy":
                try {
                    FileWriter spaceQuipFile = new FileWriter(occupancyPathQuip);
                    occupancyQuip = new BufferedWriter(spaceQuipFile);

                    FileWriter spaceIDBFile = new FileWriter(occupancyPathIDB);
                    occupancyIDB = new BufferedWriter(spaceIDBFile);

                }catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case "wifi":
                try {
                    FileWriter wifiQuipFile = new FileWriter(wifiPathQuip);
                    wifiQuip = new BufferedWriter(wifiQuipFile);

                    FileWriter wifiIDBFile = new FileWriter(wifiPathIDB);
                    wifiIDB = new BufferedWriter(wifiIDBFile);

                }catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case "users":
                try {
                    FileWriter userQuipFile = new FileWriter(userPathQuip);
                    userQuip = new BufferedWriter(userQuipFile);

                    FileWriter userIDBFile = new FileWriter(userPathIDB);
                    userIDB = new BufferedWriter(userIDBFile);

                }catch (IOException e) {
                    e.printStackTrace();
                }
                break;
        }

    }

    public void closeFile(String type){
        switch (type){
            case "space":
                try {
                    occupancyIDB.flush();
                    occupancyIDB.close();

                    occupancyQuip.flush();
                    occupancyQuip.close();
                    System.out.println("Write files successfully!");
                }catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case "wifi":
                try {
                    wifiIDB.flush();
                    wifiIDB.close();

                    wifiQuip.flush();
                    wifiQuip.close();
                    System.out.println("Write files successfully!");
                }catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case "users":
                try {
                    userIDB.flush();
                    userIDB.close();

                    userQuip.flush();
                    userQuip.close();
                    System.out.println("Write files successfully!");
                }catch (IOException e) {
                    e.printStackTrace();
                }
                break;
        }


    }

    public int transformTime(String time){
        //"2019-04-16 16:09:09";
        String t = time.substring(5,time.length());
        String nt = "";
        for(int i=0;i<t.length();i++){
            if(t.charAt(i) >= '0' && t.charAt(i) <= '9'){
                nt += t.charAt(i);
            }
        }
        //remove front zero's
        int pos = 0;
        for(int i=0;i<nt.length();i++){
            if(nt.charAt(i) == '0'){
                pos ++;
            }else{
                break;
            }
        }
        //System.out.println(nt.substring(pos));
        return Integer.valueOf(nt.substring(pos));
    }

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
                if(room.substring(0,1).compareTo("0")>=0 && room.substring(0,1).compareTo("9")<=0){
                    roomPool.add(Integer.valueOf(room.substring(0,4)));
                }
            }
            csvReader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*public void readSpacePlus(){
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
                spaceTuple st = new spaceTuple(MISSING_INTEGER,building,floor, rehash(region.hashCode()), capacity,-1,-1);
                spaceTuples.add(st);
                //System.out.println(region + " " + floor + " " + building + " " + capacity);
            }
            //System.out.println("percentage of null capacity: " + nullCapacity + " " + spaceTuples.size());
            csvReader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }*/

    /*public void setRoom(int room, int min, int max){
        while(true) {
            int num = ThreadLocalRandom.current().nextInt(min, max);
            //System.out.println(spaceTuples.get(num).getRoom() + " " + spaceTuples.get(num).getRegion());
            if (spaceTuples.get(num).getRoom() == MISSING_INTEGER && spaceTuples.get(num).getRegion() != MISSING_INTEGER) {
                spaceTuples.get(num).setRoom(room);
                //System.out.println(num + " " +  room);
                break;
            }
        }
    }*/

    /*public void setType(){
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
    }*/

    /*public void setSize(){
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
    }*/

    public void write2Quip(List<Integer> values, BufferedWriter bw){//write out missing values
        try {
            String out = "";
            for(int i=0;i<values.size();i++){
                out += String.valueOf(values.get(i));
                if(i != values.size()-1){
                    out += ",";
                }
            }
            bw.write(out);
            bw.newLine();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write2IDB(List<Integer> values, BufferedWriter bw){//interpret missing value as space
        try {
            String out = "";
            for(int i=0;i<values.size();i++){
                if(values.get(i) != MISSING_INTEGER){
                    out += String.valueOf(values.get(i));
                }
                if(i != values.size()-1){
                    out += ",";
                }
            }
            bw.write(out);
            bw.newLine();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*public void processSpace(){
        //read spaces
        readRoomPool();
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
        //write to files
        for(int i=0;i<spaceTuples.size();i++){
            //write2IDB(spaceTuples.get(i).getList(), spaceIDB);
            write2Quip(spaceTuples.get(i).getList(), spaceQuip);
        }
    }*/

    public void readPresence(){
        String fileSpace = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/presence.csv";//raw dirty
        //startTimestamp,endTimestamp,room
        try{
            BufferedReader csvReader = new BufferedReader(new FileReader(fileSpace));
            int count = 0;
            String row;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String[] data = row.split(",");
                String st = data[0];
                String et = data[1];
                //String room = data[2];
                int stINT = rehash(transformTime(st));
                int etINT = rehash(transformTime(et));
                int duration = Math.abs(etINT - stINT);
                wifiTuple tuple = new wifiTuple(stINT, etINT, MISSING_INTEGER, MISSING_INTEGER, duration);
                wifiTuples.add(tuple);
            }
            csvReader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readMacs(){
        String fileSpace = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/wifiClean.txt";
        //startTimestamp,endTimestamp,room
        try{
            BufferedReader csvReader = new BufferedReader(new FileReader(fileSpace));
            int count = 0;
            String row;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String[] data = row.split(",");
                String mac = data[0];
                macPool.add(mac.hashCode());
            }
            csvReader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readUserMacs(){
        String fileSpace = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/usermac.csv";
        //startTimestamp,endTimestamp,room
        try{
            BufferedReader csvReader = new BufferedReader(new FileReader(fileSpace));
            int count = 0;
            String row;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String mac = row;
                userMacPool.add(mac.hashCode());
            }
            csvReader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void setMac(){
        //first read from rawpool
        readMacs();
        for(int i=0;i<wifiTuples.size();i++){
            int num = ThreadLocalRandom.current().nextInt(0, macPool.size());
            wifiTuples.get(i).setMac(rehash(macPool.get(num)));
        }
    }

    public void setLocation(){
        double missingRate = 0.7;
        Random rand = new Random();
        for(int i=0;i<wifiTuples.size();i++){
            int n = rand.nextInt(100);
            if(n > missingRate*100.0){//ihe:tt
                int room = ThreadLocalRandom.current().nextInt(locationMIN, locationMAX);
                wifiTuples.get(i).setLid(room);
            }
        }
    }

    /*public void setRegion(){
        //get region pool
        readSpacePlus();
        double missingRate = 0.2;
        Random rand = new Random();

        for(int i=0;i<wifiTuples.size();i++){
            int n = rand.nextInt(100);
            if(n > missingRate*100.0){
                int id = ThreadLocalRandom.current().nextInt(0,regionPool.size());
                int region = regionPool.get(id);
                wifiTuples.get(i).setRegion(rehash(region));
            }
        }
    }*/

    public void processWiFi(){
        //set st,et,duration
        readPresence();
        //set Mac
        setMac();
        setLocation();
        //write to files
        for(int i=0;i<wifiTuples.size();i++){
            write2Quip(wifiTuples.get(i).getList(), wifiQuip);
        }
    }

    public void setUserMac(){
        int userCardinality = 4018;
        for(int i=0;i<userCardinality;i++){
            userTuple tuple = new userTuple(MISSING_INTEGER, MISSING_INTEGER, MISSING_INTEGER, MISSING_INTEGER);
            userTuples.add(tuple);
        }
        for(int i=0;i<userMacPool.size();i++){
            userTuples.get(i).setMac(rehash(userMacPool.get(i)));
        }

        Random rand = new Random();
        double missingrate = 0.2;
        for(int i = userMacPool.size();i<userCardinality;i++) {
            int n = rand.nextInt(100);
            if (n > missingrate * 100.0) {
                int id = ThreadLocalRandom.current().nextInt(0, macPool.size());
                int mac = macPool.get(id);
                userTuples.get(i).setMac(rehash(mac));
                continue;
            }
        }
    }

    public void setUsername(){
        for(int i=0;i<userTuples.size();i++){
            int name = ThreadLocalRandom.current().nextInt(0, 100000);
            userTuples.get(i).setName(name);
        }
    }

    public void setEmail(){
        for(int i=0;i<userTuples.size();i++){
            int email = ThreadLocalRandom.current().nextInt(0, 100000);
            userTuples.get(i).setEmail(email);
        }
    }

    public void setGroup(){
        Random rand = new Random();
        double missingrate = 0.1;
        for(int i=0;i<userTuples.size();i++){
            int n = rand.nextInt(100);
            if (n < missingrate * 100.0) {
                int group = ThreadLocalRandom.current().nextInt(1, 8);
                userTuples.get(i).setGroup(group);
            }
        }
    }

    public void processUsers(){
        //mac, name, email, group;

        //read user macs
        readUserMacs();
        //read macs from wifi connectivity events
        readMacs();

        setUserMac();
        setUsername();
        setEmail();
        setGroup();

        //write to files
        for(int i=0;i<userTuples.size();i++){
            //write2IDB(userTuples.get(i).getList(), userIDB);
            write2Quip(userTuples.get(i).getList(), userQuip);
        }
    }

    public void processOccupancy(){

    }
}
