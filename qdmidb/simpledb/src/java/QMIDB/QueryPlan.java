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
    private static String CDC;//{R1,R2,S1,S2}
    private static int queryID;
    private static DbIterator iter = null;
    private HeapFile WifISpace, WiFiUsers, WiFiWiFi;
    private HeapFile CDCdemo, CDClabs, CDCexams;
    private List<String> CDCrelations = new ArrayList<>();
    private List<String> CDCpahts = new ArrayList<>();
    private List<Attribute> attrs = Schema.getSchema();

    public QueryPlan(){
        this.CDCrelations.add("demo");
        this.CDCrelations.add("exams");
        this.CDCrelations.add("labs");
        this.CDCpahts.add("/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/cdcdataset/demo.dat");
        this.CDCpahts.add("/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/cdcdataset/exams.dat");
        this.CDCpahts.add("/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/cdcdataset/labs.dat");
    }

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

    public void setupCDCHeapFiles(){
        for(int i=0;i<CDCrelations.size();i++){
            String relation = CDCrelations.get(i);
            String path = CDCpahts.get(i);
            boolean flag = false;
            int pos = 0;
            Type types[] = null;
            String names[] = null;
            for(int j=0;j<attrs.size();j++){
                if(attrs.get(j).getRelation().equals(relation)){
                    if(!flag){//first time, initialize arrays
                        flag = true;
                        int length = attrs.get(j).getSchemaWidth();
                        types = new Type[length];
                        for(int k=0;k<length;k++){
                            types[k] = Type.INT_TYPE;
                        }
                        names = new String[length];
                        pos = j;
                    }
                    names[j-pos] = attrs.get(j).getAttribute();
                }
            }
            TupleDesc td = new TupleDesc(types, names);
            //System.out.println(relation + " " + path);
            //td.print();
            if(relation.equals("demo")){
                CDCdemo = new HeapFile(new File(path), td);
                Database.getCatalog().addTable(CDCdemo, "demo");
            }
            else if(relation.equals("labs")){
                CDClabs = new HeapFile(new File(path), td);
                Database.getCatalog().addTable(CDClabs, "labs");
            }
            else if(relation.equals("exams")){
                CDCexams = new HeapFile(new File(path), td);
                Database.getCatalog().addTable(CDCexams, "exams");
            }
            else{
                System.out.println("Relation is incorrect!");
            }
        }
    }


    public Operator getQueryPlan(int queryID, TransactionId tid, String dataset, String method)throws Exception{
        if(dataset.equals("WiFi")){
            switch (queryID){
                case 1:
                    return getWiFiQ1(tid);
                default:
                    return null;
            }
        }
        else if(dataset.equals("CDC")){
            switch (queryID){
                case 1:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        //return getQueryTest1(tid);
                        return getCDCQ1IDB(tid);
                    }else{
                        //return getCDCQ1Quip(tid);
                        return getCDCQ1QuipMAXMIN(tid);
                    }
                case 2:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getCDCQ2IDB(tid);
                    }else{
                        return getCDCQ2Quip(tid);
                    }
                case 3:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getCDCQ3IDB(tid);
                    }else{
                        return getCDCQ3Quip(tid);
                    }
                case 4:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getCDCQ4IDB(tid);
                    }else{
                        return getCDCQ4Quip(tid);
                    }
                case 5:
                    //return getCDCQ5IDB(tid);
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getCDCQ5IDB(tid);
                    }else{
                        return getCDCQ5Quip(tid);
                    }
                case 6://MAX/MIN query
                    //return getCDCQ5IDB(tid);
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getCDCQ6IDB(tid);
                    }else{
                        return getCDCQ6Quip(tid);
                    }
                case 7:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getCDCQ7IDB(tid);
                    }else{
                        return getCDCQ7Quip(tid);
                    }
                case 8:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getCDCQ8IDB(tid);
                    }else{
                        return getCDCQ8Quip(tid);
                    }
                default:
                    return null;
            }
        }else{
            System.out.println("No such dataset!");
        }
        return null;
    }

    public Operator getQueryTest1(TransactionId tid) throws Exception{
        SeqScan s1demo= new SeqScan(tid, CDCdemo.getId(), "demo");
        Impute imp1demo = new Impute(new Attribute("demo.income"),s1demo);
        SeqScan s2exams= new SeqScan(tid, CDCexams.getId(), "exams");
        Impute imp2exams = new Impute(new Attribute("exams.cuff_size"),s2exams);
        Impute imp3exams = new Impute(new Attribute("exams.height"),imp2exams);
        SmartFilter sel1exams = new SmartFilter(new Predicate("exams.height", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(15000)), imp3exams);
        JoinPredicate predName1 = new JoinPredicate("demo.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin join1demo = new SmartJoin(predName1, imp1demo, sel1exams);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.income"));
        attributes.add(new Attribute("exams.cuff_size"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join1demo);
        SmartAggregate sp = new SmartAggregate(sp1, "exams.cuff_size", "demo.income", Aggregator.Op.AVG);

        return sp;
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

    //*IDB are plans generated by imputeDB
    public Operator getCDCQ1IDB(TransactionId tid)throws Exception{

        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        //SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //impute demo.income
        Impute ip1 = new Impute(new Attribute("demo.income"),ssdemo);

        //impute exams.cuff_size
        Impute ip2 = new Impute(new Attribute("exams.cuff_size"), ssexams);

        //exams.height>=15000
        SmartFilter sf1 = new SmartFilter(
                new Predicate("exams.height", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(15000)), ip2);

        //demo.id = exams.id
        JoinPredicate p1 = new JoinPredicate("demo.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj2 = new SmartJoin(p1,ip1,sf1);

        //test smartProject
        //demo.income, cuff_size
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.income"));
        attributes.add(new Attribute("exams.cuff_size"));
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj2);

        //aggregate
        //GROUP BY demo.income
        //SELECT demo.income, AVG(exams.cuff_size)
        SmartAggregate sa = new SmartAggregate(sp,"exams.cuff_size","demo.income", Aggregator.Op.AVG);

        return sa;
    }

    public Operator getCDCQ1Quip(TransactionId tid)throws Exception{

        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        //SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //impute demo.income
        //Impute ip1 = new Impute(new Attribute("demo.income"),ssdemo);

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

        //aggregate
        //GROUP BY demo.income
        //SELECT demo.income, AVG(exams.cuff_size)
        SmartAggregate sa = new SmartAggregate(sp,"exams.cuff_size","demo.income", Aggregator.Op.AVG);

        return sa;
    }

    public Operator getCDCQ1QuipMAXMIN(TransactionId tid)throws Exception{

        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        //SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //impute demo.income
        //Impute ip1 = new Impute(new Attribute("exams.arm_circumference"),ssexams);

        //exams.height>=15000
        SmartFilter sf1 = new SmartFilter(
                new Predicate("exams.height", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(15000)), ssexams);

        //demo.id = exams.id
        JoinPredicate p1 = new JoinPredicate("demo.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj2 = new SmartJoin(p1,ssdemo,sf1);

        //test smartProject
        //demo.income, cuff_size
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("exams.arm_circumference"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj2);

        //aggregate
        //GROUP BY demo.income
        //SELECT demo.income, AVG(exams.cuff_size)
        SmartAggregate sa = new SmartAggregate(sp,"exams.arm_circumference","null", Aggregator.Op.MAX);

        return sa;
    }

    public Operator getCDCQ2IDB(TransactionId tid)throws Exception{
        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //demo.income >= 13
        SmartFilter sf1 = new SmartFilter(
                new Predicate("demo.income", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(13)), ssdemo);

        //exams.weight >= 6300
        SmartFilter sf2 = new SmartFilter(
                new Predicate("exams.weight", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(6300)), ssexams);

        //demo.id = exams.id
        JoinPredicate p1 = new JoinPredicate("demo.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj2 = new SmartJoin(p1,sf1,sf2);

        //impute labs.creatine
        Impute ip1 = new Impute(new Attribute("labs.creatine"),sslabs);

        //exams.id = labs.id
        JoinPredicate p2 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "labs.id");
        SmartJoin sj3 = new SmartJoin(p2,sj2,ip1);

        //test smartProject
        //demo.income, labs.creatine
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.income"));
        attributes.add(new Attribute("labs.creatine"));
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj3);

        //aggregate
        //SELECT demo.income, AVG(labs.creatine)
        //GROUP BY demo.income
        SmartAggregate sa = new SmartAggregate(sp,"labs.creatine","demo.income", Aggregator.Op.AVG);

        return sa;
    }

    public Operator getCDCQ2Quip(TransactionId tid)throws Exception{
        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //demo.income >= 13
        SmartFilter sf1 = new SmartFilter(
                new Predicate("demo.income", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(13)), ssdemo);

        //exams.weight >= 6300
        SmartFilter sf2 = new SmartFilter(
                new Predicate("exams.weight", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(6300)), ssexams);

        //demo.id = exams.id
        JoinPredicate p1 = new JoinPredicate("demo.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj2 = new SmartJoin(p1,sf1,sf2);

        //impute labs.creatine
        //Impute ip1 = new Impute(new Attribute("labs.creatine"),sslabs);

        //exams.id = labs.id
        JoinPredicate p2 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "labs.id");
        SmartJoin sj3 = new SmartJoin(p2,sj2,sslabs);

        //test smartProject
        //demo.income, labs.creatine
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.income"));
        attributes.add(new Attribute("labs.creatine"));
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj3);

        //aggregate
        //SELECT demo.income, AVG(labs.creatine)
        //GROUP BY demo.income
        SmartAggregate sa = new SmartAggregate(sp,"labs.creatine","demo.income", Aggregator.Op.AVG);

        return sa;
    }

    public Operator getCDCQ2(TransactionId tid)throws Exception{

        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //demo.income >= 13 AND demo.income <= 15
        SmartFilter sf1 = new SmartFilter(
                new Predicate("demo.income", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(13)), ssdemo);

        //exams.weight >= 6300
        SmartFilter sf3 = new SmartFilter(
                new Predicate("exams.weight", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(6300)), ssexams);

        //exams.id = demo.id
        JoinPredicate p1 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin sj2 = new SmartJoin(p1,sf3,sf1);

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

    public Operator getCDCQ3IDB(TransactionId tid)throws Exception{
        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //impute labs.blood_lead
        Impute ip1 = new Impute(new Attribute("labs.blood_lead"),sslabs);

        //labs.id = exams.id
        JoinPredicate p1 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj1 = new SmartJoin(p1,ip1,ssexams);

        //demo.age_yrs <= 20
        SmartFilter sf1 = new SmartFilter(
                new Predicate("demo.age_yrs", Predicate.Op.LESS_THAN_OR_EQ, new IntField(20)), ssdemo);

        //labs.id = demo.id
        JoinPredicate p2 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin sj2 = new SmartJoin(p2,sj1,sf1);

        //test smartProject
        //labs.blood_lead
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("labs.blood_lead"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj2);

        //aggregate
        //AVG(labs.blood_lead)
        SmartAggregate sa = new SmartAggregate(sp, "labs.blood_lead","null", Aggregator.Op.AVG);

        return sa;
    }

    public Operator getCDCQ3Quip(TransactionId tid)throws Exception{
        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //impute labs.blood_lead
        //Impute ip1 = new Impute(new Attribute("labs.blood_lead"),sslabs);

        //labs.id = exams.id
        JoinPredicate p1 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj1 = new SmartJoin(p1,sslabs,ssexams);

        //demo.age_yrs <= 30
        SmartFilter sf1 = new SmartFilter(
                new Predicate("demo.age_yrs", Predicate.Op.LESS_THAN_OR_EQ, new IntField(20)), ssdemo);

        //labs.id = demo.id
        JoinPredicate p2 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin sj2 = new SmartJoin(p2,sj1,sf1);

        //test smartProject
        //labs.blood_lead
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("labs.blood_lead"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj2);

        //aggregate
        //AVG(labs.blood_lead)
        SmartAggregate sa = new SmartAggregate(sp, "labs.blood_lead","null", Aggregator.Op.AVG);

        return sa;
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

        //aggregate
        //AVG(labs.blood_lead)
        SmartAggregate sa = new SmartAggregate(sp, "labs.blood_lead","null", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getCDCQ4IDB(TransactionId tid)throws Exception{
        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //impute exams.blood_pressure_systolic
        Impute ip1 = new Impute(new Attribute("exams.blood_pressure_systolic"),ssexams);

        //exams.body_mass_index >= 3000
        SmartFilter sf1 = new SmartFilter(
                new Predicate("exams.body_mass_index", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(3000)), ip1);

        //labs.id = exams.id
        JoinPredicate p2 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj1 = new SmartJoin(p2,sslabs,sf1);


        //labs.id = demo.id
        JoinPredicate p1 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin sj2 = new SmartJoin(p1,sj1,ssdemo);

        //test smartProject
        //labs.blood_lead
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.gender"));
        attributes.add(new Attribute("exams.blood_pressure_systolic"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj2);

        //aggregate
        //demo.gender, AVG(exams.blood_pressure_systolic)
        //GROUP BY demo.gender

        SmartAggregate sa = new SmartAggregate(sp,"exams.blood_pressure_systolic","demo.gender", Aggregator.Op.AVG);

        return sa;
    }

    public Operator getCDCQ4Quip(TransactionId tid)throws Exception{
        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //impute exams.blood_pressure_systolic
        //Impute ip1 = new Impute(new Attribute("exams.blood_pressure_systolic"),sslabs);

        //exams.body_mass_index >= 3000
        SmartFilter sf1 = new SmartFilter(
                new Predicate("exams.body_mass_index", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(3000)), ssexams);

        //labs.id = exams.id
        JoinPredicate p2 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj1 = new SmartJoin(p2,sslabs,sf1);


        //labs.id = demo.id
        JoinPredicate p1 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin sj2 = new SmartJoin(p1,sj1,ssdemo);

        //smartProject
        //labs.blood_lead
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.gender"));
        attributes.add(new Attribute("exams.blood_pressure_systolic"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj2);

        //aggregate
        //demo.gender, AVG(exams.blood_pressure_systolic)
        //GROUP BY demo.gender

        SmartAggregate sa = new SmartAggregate(sp,"exams.blood_pressure_systolic","demo.gender", Aggregator.Op.AVG);

        return sa;
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
        attributes.add(new Attribute("demo.gender"));
        attributes.add(new Attribute("exams.blood_pressure_systolic"));
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj2);

        return sp;
    }

    public Operator getCDCQ5IDB(TransactionId tid)throws Exception{
        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //impute exams.waist_circumference
        Impute ip1 = new Impute(new Attribute("exams.waist_circumference"),ssexams);

        //exams.height>=15000
        SmartFilter sf1 = new SmartFilter(
                new Predicate("exams.height", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(15000)), ip1);

        //exams.weight>=10000
        SmartFilter sf2 = new SmartFilter(
                new Predicate("exams.weight", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(10000)), sf1);

        //demo.id = exams.id
        JoinPredicate p1 = new JoinPredicate("demo.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj1 = new SmartJoin(p1,ssdemo,sf2);

        //smartProject
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("exams.waist_circumference"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj1);

        //aggregate: AVG(exams.waist_circumference)
        SmartAggregate sa = new SmartAggregate(sp, "exams.waist_circumference","null", Aggregator.Op.AVG);

        return sa;
    }

    public Operator getCDCQ5Quip(TransactionId tid)throws Exception{
        SeqScan ssdemo = new SeqScan(tid, CDCdemo.getId(), "demo");
        SeqScan ssexams = new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan sslabs = new SeqScan(tid, CDClabs.getId(), "labs");

        //impute exams.waist_circumference
        //Impute ip1 = new Impute(new Attribute("exams.waist_circumference"),ssexams);

        //exams.height>=15000
        SmartFilter sf1 = new SmartFilter(
                new Predicate("exams.height", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(15000)), ssexams);

        //exams.weight>=10000
        SmartFilter sf2 = new SmartFilter(
                new Predicate("exams.weight", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(10000)), sf1);

        //demo.id = exams.id
        JoinPredicate p1 = new JoinPredicate("demo.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin sj1 = new SmartJoin(p1,ssdemo,sf2);

        //test smartProject
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("exams.waist_circumference"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj1);

        //aggregate: AVG(exams.waist_circumference)
        SmartAggregate sa = new SmartAggregate(sp, "exams.waist_circumference","null", Aggregator.Op.AVG);

        return sa;
    }

    public Operator getCDCQ6IDB(TransactionId tid) throws Exception{
        SeqScan s1exams= new SeqScan(tid, CDCexams.getId(), "exams");
        Impute imp1exams = new Impute(new Attribute("exams.weight"),s1exams);
        SmartFilter sel1exams = new SmartFilter(new Predicate("exams.weight", Predicate.Op.LESS_THAN_OR_EQ, new IntField(100)), imp1exams);
        SeqScan s2labs= new SeqScan(tid, CDClabs.getId(), "labs");
        Impute imp2labs = new Impute(new Attribute("labs.blood_lead"),s2labs);
        SmartFilter sel2labs = new SmartFilter(new Predicate("labs.blood_lead", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(40)), imp2labs);
        JoinPredicate predName1 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "labs.id");
        SmartJoin join1exams = new SmartJoin(predName1, sel1exams, sel2labs);
        SeqScan s3demo= new SeqScan(tid, CDCdemo.getId(), "demo");
        Impute imp3demo = new Impute(new Attribute("demo.income"),s3demo);
        JoinPredicate predName2 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin join2exams = new SmartJoin(predName2, join1exams, imp3demo);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.income"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2exams);
        SmartAggregate sp = new SmartAggregate(sp1, "demo.income", "", Aggregator.Op.MAX);
        return sp;
    }

    public Operator getCDCQ6Quip(TransactionId tid) throws Exception{
        SeqScan s1exams= new SeqScan(tid, CDCexams.getId(), "exams");
        SmartFilter sel1exams = new SmartFilter(new Predicate("exams.weight", Predicate.Op.LESS_THAN_OR_EQ, new IntField(100)), s1exams);
        SeqScan s2labs= new SeqScan(tid, CDClabs.getId(), "labs");
        SmartFilter sel2labs = new SmartFilter(new Predicate("labs.blood_lead", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(40)), s2labs);
        JoinPredicate predName1 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "labs.id");
        SmartJoin join1exams = new SmartJoin(predName1, sel1exams, sel2labs);
        SeqScan s3demo= new SeqScan(tid, CDCdemo.getId(), "demo");
        JoinPredicate predName2 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin join2exams = new SmartJoin(predName2, join1exams, s3demo);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.income"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2exams);
        SmartAggregate sp = new SmartAggregate(sp1, "demo.income", "", Aggregator.Op.MAX);
        return sp;
    }
    public Operator getCDCQ7IDB(TransactionId tid) throws Exception{
        SeqScan s1exams= new SeqScan(tid, CDCexams.getId(), "exams");
        Impute imp1exams = new Impute(new Attribute("exams.head_circumference"),s1exams);
        SeqScan s2labs= new SeqScan(tid, CDClabs.getId(), "labs");
        Impute imp2labs = new Impute(new Attribute("labs.triglyceride"),s2labs);
        SmartFilter sel1labs = new SmartFilter(new Predicate("labs.triglyceride", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(300)), imp2labs);
        JoinPredicate predName1 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "labs.id");
        SmartJoin join1exams = new SmartJoin(predName1, imp1exams, sel1labs);
        SeqScan s3demo= new SeqScan(tid, CDCdemo.getId(), "demo");
        Impute imp3demo = new Impute(new Attribute("demo.years_edu"),s3demo);
        SmartFilter sel2demo = new SmartFilter(new Predicate("demo.years_edu", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(5)), imp3demo);
        JoinPredicate predName2 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin join2exams = new SmartJoin(predName2, join1exams, sel2demo);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("exams.head_circumference"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2exams);
        SmartAggregate sp = new SmartAggregate(sp1, "exams.head_circumference", "", Aggregator.Op.MIN);
        return sp;
    }

    public Operator getCDCQ7Quip(TransactionId tid) throws Exception{
        SeqScan s1exams= new SeqScan(tid, CDCexams.getId(), "exams");
        SeqScan s2labs= new SeqScan(tid, CDClabs.getId(), "labs");
        SmartFilter sel1labs = new SmartFilter(new Predicate("labs.triglyceride", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(500)), s2labs);
        JoinPredicate predName1 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "labs.id");
        SmartJoin join1exams = new SmartJoin(predName1, s1exams, sel1labs);
        SeqScan s3demo= new SeqScan(tid, CDCdemo.getId(), "demo");
        SmartFilter sel2demo = new SmartFilter(new Predicate("demo.years_edu", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(10)), s3demo);
        JoinPredicate predName2 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin join2exams = new SmartJoin(predName2, join1exams, sel2demo);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("exams.head_circumference"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2exams);
        SmartAggregate sp = new SmartAggregate(sp1, "exams.head_circumference", "", Aggregator.Op.MIN);
        return sp;
    }

    public Operator getCDCQ8IDB(TransactionId tid) throws Exception{
        SeqScan s1labs= new SeqScan(tid, CDClabs.getId(), "labs");
        Impute imp1labs = new Impute(new Attribute("labs.blood_lead"),s1labs);
        SmartFilter sel1labs = new SmartFilter(new Predicate("labs.blood_lead", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(200)), imp1labs);
        SeqScan s2exams= new SeqScan(tid, CDCexams.getId(), "exams");
        Impute imp2exams = new Impute(new Attribute("exams.blood_pressure_systolic"),s2exams);
        Impute imp3exams = new Impute(new Attribute("exams.body_mass_index"),imp2exams);
        SmartFilter sel2exams = new SmartFilter(new Predicate("exams.blood_pressure_systolic", Predicate.Op.LESS_THAN_OR_EQ, new IntField(100)), imp3exams);
        JoinPredicate predName1 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin join1labs = new SmartJoin(predName1, sel1labs, sel2exams);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("exams.body_mass_index"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join1labs);
        SmartAggregate sp = new SmartAggregate(sp1, "exams.body_mass_index", "", Aggregator.Op.MAX);
        return sp;
    }

    public Operator getCDCQ8Quip(TransactionId tid) throws Exception{
        SeqScan s1labs= new SeqScan(tid, CDClabs.getId(), "labs");
        SmartFilter sel1labs = new SmartFilter(new Predicate("labs.blood_lead", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(200)), s1labs);
        SeqScan s2exams= new SeqScan(tid, CDCexams.getId(), "exams");
        SmartFilter sel2exams = new SmartFilter(new Predicate("exams.blood_pressure_systolic", Predicate.Op.LESS_THAN_OR_EQ, new IntField(100)), s2exams);
        JoinPredicate predName1 = new JoinPredicate("labs.id", Predicate.Op.EQUALS, "exams.id");
        SmartJoin join1labs = new SmartJoin(predName1, sel1labs, sel2exams);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("exams.body_mass_index"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join1labs);
        SmartAggregate sp = new SmartAggregate(sp1, "exams.body_mass_index", "", Aggregator.Op.MAX);
        return sp;
    }


    public DbIterator test(TransactionId tid){
        SeqScan ssSpace = new SeqScan(tid, WifISpace.getId(), "space");
        return ssSpace;
    }

}
