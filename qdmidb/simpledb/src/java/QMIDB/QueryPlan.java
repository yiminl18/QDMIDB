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

    public DbIterator test(TransactionId tid){
        SeqScan ssSpace = new SeqScan(tid, WifISpace.getId(), "space");
        return ssSpace;
    }

}
