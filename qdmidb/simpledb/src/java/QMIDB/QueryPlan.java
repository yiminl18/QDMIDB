package QMIDB;

import simpledb.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/*
    This class is used to ingest and store query plan.
    The first version is to manually transform the query plan from outside to to be in the code
 */

public class QueryPlan {
    private static String dataSet;//{R1,R2,S1,S2}
    private static int queryID;
    private static DbIterator iter = null;
    private HeapFile WifISpace, WiFiUsers, WiFiWiFi;
    private HeapFile CDCdemo, CDClabs, CDCexams;

    public void setupWiFiHeapFiles()
    {
        Type types1[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names1[] = new String[]{ "space.room", "space.floor","space.building"};

        TupleDesc tdSpace = new TupleDesc(types1, names1);

        Type types2[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names2[] = new String[]{"users.name", "users.email","users.mac"};

        TupleDesc tdUsers = new TupleDesc(types2, names2);

        Type types3[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names3[] = new String[]{ "wifi.mac", "wifi.time", "wifi.room"};

        TupleDesc tdWiFi = new TupleDesc(types3, names3);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema the tables.
        WifISpace = new HeapFile(new File("simpledb/wifidataset/spaceHash.dat"), tdSpace);
        Database.getCatalog().addTable(WifISpace, "space");

        WiFiUsers = new HeapFile(new File("simpledb/wifidataset/userHash.dat"), tdUsers);
        Database.getCatalog().addTable(WiFiUsers, "users");

        WiFiWiFi = new HeapFile(new File("simpledb/wifidataset/wifiHash.dat"), tdWiFi);
        Database.getCatalog().addTable(WiFiWiFi, "wifi");
    }

    public Operator getQueryPlan(int queryID, TransactionId tid, String dataset)throws Exception{
        if(dataset.equals("WiFi")){
            switch (queryID){
                case 1:
                    return getWiFiQ1(tid);
                default:
                    return null;
            }
        }
        else{
            System.out.println("No such dataset!");
        }
        return null;
    }

    public Operator getWiFiQ1(TransactionId tid)throws Exception{


        SeqScan ssSpace = new SeqScan(tid, WifISpace.getId(), "space");
        SeqScan ssUsers = new SeqScan(tid, WiFiUsers.getId(), "users");
        SeqScan ssWiFi = new SeqScan(tid, WiFiWiFi.getId(), "wifi");

        //users.mac = wifi.mac
        JoinPredicate p1 = new JoinPredicate("wifi.mac", Predicate.Op.EQUALS, "users.mac");
        SmartJoin sj1 = new SmartJoin(p1,ssWiFi,ssUsers);

        //space.building = 'DBH'
        SmartFilter sf1 = new SmartFilter(
                new Predicate("space.building", Predicate.Op.EQUALS, new IntField(67466)), ssSpace);

        //WiFi.room = Space.room
        JoinPredicate p2 = new JoinPredicate("wifi.room", Predicate.Op.EQUALS, "space.room");
        SmartJoin sj2 = new SmartJoin(p2,sj1,sf1);


        //test smartProject
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("users.name"));
        attributes.add(new Attribute("wifi.time"));
        attributes.add(new Attribute("wifi.room"));
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj2);

        return sp;
    }

    public Operator getCDCQ1(TransactionId tid)throws Exception{

        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        //SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //exams.height>=15000
        SmartFilter sf1 = new SmartFilter(
                new Predicate("exams.height", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(15000)), ssexams);

        //demo.id = exams.id
        JoinPredicate p1 = new JoinPredicate("demo.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj2 = new SmartJoin(p1,ssdemo,sf1);

        //test smartProject
        //demo.income, cuff_size
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.income"));
        attributes.add(new Attribute("exams.cuff_size"));
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj2);

        return sp;
    }

    public Operator getCDCQ2(TransactionId tid)throws Exception{

        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //demo.income >= 13 AND demo.income <= 15
        SmartFilter sf1 = new SmartFilter(
                new Predicate("demo.income", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(13)), ssdemo);

        SmartFilter sf2 = new SmartFilter(
                new Predicate("demo.income", Predicate.Op.LESS_THAN_OR_EQ, new IntField(15)), sf1);

        //exams.weight >= 6300
        SmartFilter sf3 = new SmartFilter(
                new Predicate("exams.weight", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(6300)), ssexams);

        //exams.id = demo.id
        JoinPredicate p1 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin sj2 = new SmartJoin(p1,sf3,sf2);

        //labs.id = exams.id
        JoinPredicate p2 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj3 = new SmartJoin(p2,sslabs,sj2);

        //test smartProject
        //demo.income, labs.creatine
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.income"));
        attributes.add(new Attribute("labs.creatine"));
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj3);

        return sp;
    }

    public Operator getCDCQ3(TransactionId tid)throws Exception{
        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //demo.age_yrs <= 6
        SmartFilter sf1 = new SmartFilter(
                new Predicate("demo.age_yrs", Predicate.Op.LESS_THAN_OR_EQ, new IntField(6)), ssdemo);

        //labs.id = demo.id
        JoinPredicate p2 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin sj1 = new SmartJoin(p2,sslabs,sf1);

        //exams.id = labs.id
        JoinPredicate p1 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "labs.id");
        SmartJoin sj2 = new SmartJoin(p1,ssexams,sj1);

        //test smartProject
        //labs.blood_lead
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("labs.blood_lead"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj2);

        return sp;
    }

    public Operator getCDCQ4(TransactionId tid)throws Exception{
        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //exams.body_mass_index >= 3000
        SmartFilter sf1 = new SmartFilter(
                new Predicate("exams.body_mass_index", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(3000)), ssexams);

        //labs.id = exams.id
        JoinPredicate p2 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj1 = new SmartJoin(p2,sslabs,sf1);




        //demo.id = labs.id
        JoinPredicate p1 = new JoinPredicate("demo.id", Predicate.Op.EQUALS, "labs.id");
        SmartJoin sj2 = new SmartJoin(p1,ssdemo,sj1);

        //test smartProject
        //labs.blood_lead
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("labs.blood_lead"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj2);

        return sp;
    }

    public DbIterator test(TransactionId tid){
        SeqScan ssSpace = new SeqScan(tid, WifISpace.getId(), "space");
        return ssSpace;
    }

}
