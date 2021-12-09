package QMIDB;

import simpledb.*;

import java.io.File;
import java.lang.reflect.Array;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
    This class is used to ingest and store query plan.
    The first version is to manually transform the query plan from outside to to be in the code
 */

public class QueryPlan {
    private static DbIterator iter = null;
    private HeapFile WiFioccupancy, WiFiusers, WiFiwifi;
    private HeapFile CDCdemo, CDClabs, CDCexams;
    private HeapFile ACSt0, ACSt1, ACSt2, ACSt3, ACSt4;
    private List<String> CDCrelations = new ArrayList<>(), WiFiRelations = new ArrayList<>(), ACSRelations = new ArrayList<>();
    private List<String> CDCpahts = new ArrayList<>(), WiFiPaths = new ArrayList<>(), ACSPaths = new ArrayList<>();
    private List<Attribute> attrs = Schema.getSchema();
    private int ACSRelationSize = 5;

    public QueryPlan(){
        this.CDCrelations.add("demo");
        this.CDCrelations.add("exams");
        this.CDCrelations.add("labs");
        this.CDCpahts.add("/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/cdcdataset/demo.dat");
        this.CDCpahts.add("/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/cdcdataset/exams.dat");
        this.CDCpahts.add("/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/cdcdataset/labs.dat");

        this.WiFiRelations.add("occupancy");
        this.WiFiRelations.add("users");
        this.WiFiRelations.add("wifi");
        this.WiFiPaths.add("/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/occupancy.dat");
        this.WiFiPaths.add("/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/users.dat");
        this.WiFiPaths.add("/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/wifi.dat");

        String path = "../QDMIDB/QDMIDB/qdmidb/simpledb/acsdataset/";
        for(int i=0;i<ACSRelationSize;i++){
            String tableName = "t" + i;
            this.ACSRelations.add(tableName);
            String RPath = Paths.get(path + tableName + ".dat").toAbsolutePath().toString();
            this.ACSPaths.add(RPath);
        }
    }

    public void setupACSHeapFiles(){
        for(int i=0;i<ACSRelations.size();i++){
            String relation = ACSRelations.get(i);
            String path = ACSPaths.get(i);
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
            switch (relation){
                case "t0":
                    ACSt0 = new HeapFile(new File(path),td);;
                    Database.getCatalog().addTable(ACSt0, "t0");
                    break;
                case "t1":
                    ACSt1 = new HeapFile(new File(path),td);;
                    Database.getCatalog().addTable(ACSt1, "t1");
                    break;
                case "t2":
                    ACSt2 = new HeapFile(new File(path),td);;
                    Database.getCatalog().addTable(ACSt2, "t2");
                    break;
                case "t3":
                    ACSt3 = new HeapFile(new File(path),td);;
                    Database.getCatalog().addTable(ACSt3, "t3");
                    break;
                case "t4":
                    ACSt4 = new HeapFile(new File(path),td);;
                    Database.getCatalog().addTable(ACSt4, "t4");
                    break;
                default:
                    System.out.println("Relation is incorrect!");
                    break;
            }
        }
    }

    public void setupWiFiHeapFiles()
    {
        for(int i=0;i<WiFiRelations.size();i++){
            String relation = WiFiRelations.get(i);
            String path = WiFiPaths.get(i);
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
            if(relation.equals("occupancy")){
                WiFioccupancy = new HeapFile(new File(path), td);
                Database.getCatalog().addTable(WiFioccupancy, "occupancy");
            }
            else if(relation.equals("users")){
                WiFiusers = new HeapFile(new File(path), td);
                Database.getCatalog().addTable(WiFiusers, "users");
            }
            else if(relation.equals("wifi")){
                WiFiwifi = new HeapFile(new File(path), td);
                Database.getCatalog().addTable(WiFiwifi, "wifi");
            }
            else{
                System.out.println("Relation is incorrect!");
            }
        }
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
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getWifiQ1IDB(tid);
                    }else{
                        return getWifiQ1Quip(tid);
                    }
                case 2:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getWifiQ2IDB(tid);
                    }else{
                        return getWifiQ2Quip(tid);
                    }
                case 3:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getWifiQ3IDB(tid);
                    }else{
                        return getWifiQ3Quip(tid);
                    }
                case 4:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getWifiQ4IDB(tid);
                    }else{
                        return getWifiQ4Quip(tid);
                    }
                case 5:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getWifiQ5IDB(tid);
                    }else{
                        return getWifiQ5Quip(tid);
                    }
                case 6:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getWifiQ6IDB(tid);
                    }else{
                        return getWifiQ6Quip(tid);
                    }
                case 7:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getWifiQ7IDB(tid);
                    }else{
                        return getWifiQ7Quip(tid);
                    }
                case 8:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getWifiQ8IDB(tid);
                    }else{
                        return getWifiQ8Quip(tid);
                    }
                case 9:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getWifiQ9IDB(tid);
                    }else{
                        return getWifiQ9Quip(tid);
                    }
                case 10:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getWifiQ10IDB(tid);
                    }else{
                        return getWifiQ10Quip(tid);
                    }
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
                        return getCDCQ1IDB(tid);
                    }else{
                        return getCDCQ1Quip(tid);
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
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getCDCQ5IDB(tid);
                    }else{
                        return getCDCQ5Quip(tid);
                    }
                case 6:
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
                case 9:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getCDCQ9IDB(tid);
                    }else{
                        return getCDCQ9Quip(tid);
                    }
                case 10:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getCDCQ10IDB(tid);
                    }else{
                        return getCDCQ10Quip(tid);
                    }
                default:
                    return null;
            }
        }else if(dataset.equals("ACS")){
            switch (queryID){
                case 1:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getACSQ1IDB(tid);
                    }else{
                        return getACSQ1Quip(tid);
                    }
                case 2:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getACSQ2IDB(tid);
                    }else{
                        return getACSQ2Quip(tid);
                    }
                case 3:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getACSQ3IDB(tid);
                    }else{
                        return getACSQ3Quip(tid);
                    }
                case 4:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getACSQ4IDB(tid);
                    }else{
                        return getACSQ4Quip(tid);
                    }
                case 5:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getACSQ5IDB(tid);
                    }else{
                        return getACSQ5Quip(tid);
                    }
                case 6:
                    if(method.equalsIgnoreCase("ImputeDB")){
                        AggregateOptimization.setApplied_flag(false);
                        Decision.flipApplied_bit();
                        return getACSQ6IDB(tid);
                    }else{
                        return getACSQ6Quip(tid);
                    }
                default:
                    return null;
            }
        }else{
            System.out.println("No such dataset!");
        }
        return null;
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

    public Operator getCDCQ9IDB(TransactionId tid) throws Exception{
        SeqScan s1exams= new SeqScan(tid, CDCexams.getId(), "exams");
        Impute imp1exams = new Impute(new Attribute("exams.blood_pressure_systolic"),s1exams);
        Impute imp2exams = new Impute(new Attribute("exams.height"),imp1exams);
        SmartFilter sel1exams = new SmartFilter(new Predicate("exams.height", Predicate.Op.LESS_THAN_OR_EQ, new IntField(10000)), imp2exams);
        SeqScan s2labs= new SeqScan(tid, CDClabs.getId(), "labs");
        Impute imp3labs = new Impute(new Attribute("labs.blood_lead"),s2labs);
        SmartFilter sel2labs = new SmartFilter(new Predicate("labs.blood_lead", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(100)), imp3labs);
        JoinPredicate predName1 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "labs.id");
        SmartJoin join1exams = new SmartJoin(predName1, sel1exams, sel2labs);
        SeqScan s3demo= new SeqScan(tid, CDCdemo.getId(), "demo");
        Impute imp4demo = new Impute(new Attribute("demo.income"),s3demo);
        JoinPredicate predName2 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin join2exams = new SmartJoin(predName2, join1exams, imp4demo);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.income"));
        attributes.add(new Attribute("exams.blood_pressure_systolic"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2exams);
        SmartAggregate sp = new SmartAggregate(sp1, "exams.blood_pressure_systolic", "demo.income", Aggregator.Op.MAX);
        return sp;
    }

    public Operator getCDCQ9Quip(TransactionId tid) throws Exception{
        SeqScan s1exams= new SeqScan(tid, CDCexams.getId(), "exams");
        SmartFilter sel1exams = new SmartFilter(new Predicate("exams.height", Predicate.Op.LESS_THAN_OR_EQ, new IntField(10000)), s1exams);
        SeqScan s2labs= new SeqScan(tid, CDClabs.getId(), "labs");
        SmartFilter sel2labs = new SmartFilter(new Predicate("labs.blood_lead", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(100)), s2labs);
        JoinPredicate predName1 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "labs.id");
        SmartJoin join1exams = new SmartJoin(predName1, sel1exams, sel2labs);
        SeqScan s3demo= new SeqScan(tid, CDCdemo.getId(), "demo");
        JoinPredicate predName2 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin join2exams = new SmartJoin(predName2, join1exams, s3demo);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.income"));
        attributes.add(new Attribute("exams.blood_pressure_systolic"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2exams);
        SmartAggregate sp = new SmartAggregate(sp1, "exams.blood_pressure_systolic", "demo.income", Aggregator.Op.MAX);
        return sp;
    }

    public Operator getCDCQ10IDB(TransactionId tid) throws Exception{
        SeqScan s1exams= new SeqScan(tid, CDCexams.getId(), "exams");
        Impute imp1exams = new Impute(new Attribute("exams.waist_circumference"),s1exams);
        SmartFilter sel1exams = new SmartFilter(new Predicate("exams.waist_circumference", Predicate.Op.LESS_THAN_OR_EQ, new IntField(5000)), imp1exams);
        SeqScan s2labs= new SeqScan(tid, CDClabs.getId(), "labs");
        Impute imp2labs = new Impute(new Attribute("labs.albumin"),s2labs);
        SmartFilter sel2labs = new SmartFilter(new Predicate("labs.albumin", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(2000)), imp2labs);
        Impute imp3labs = new Impute(new Attribute("labs.triglyceride"),sel2labs);
        JoinPredicate predName1 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "labs.id");
        SmartJoin join1exams = new SmartJoin(predName1, sel1exams, imp3labs);
        SeqScan s3demo= new SeqScan(tid, CDCdemo.getId(), "demo");
        JoinPredicate predName2 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin join2exams = new SmartJoin(predName2, join1exams, s3demo);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.gender"));
        attributes.add(new Attribute("labs.triglyceride"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2exams);
        SmartAggregate sp = new SmartAggregate(sp1, "labs.triglyceride", "demo.gender", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getCDCQ10Quip(TransactionId tid) throws Exception{
        SeqScan s1exams= new SeqScan(tid, CDCexams.getId(), "exams");
        SmartFilter sel1exams = new SmartFilter(new Predicate("exams.waist_circumference", Predicate.Op.LESS_THAN_OR_EQ, new IntField(5000)), s1exams);
        SeqScan s2labs= new SeqScan(tid, CDClabs.getId(), "labs");
        SmartFilter sel2labs = new SmartFilter(new Predicate("labs.albumin", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(2000)), s2labs);
        JoinPredicate predName1 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "labs.id");
        SmartJoin join1exams = new SmartJoin(predName1, sel1exams, sel2labs);
        SeqScan s3demo= new SeqScan(tid, CDCdemo.getId(), "demo");
        JoinPredicate predName2 = new JoinPredicate("exams.id", Predicate.Op.EQUALS, "demo.id");
        SmartJoin join2exams = new SmartJoin(predName2, join1exams, s3demo);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("demo.gender"));
        attributes.add(new Attribute("labs.triglyceride"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2exams);
        SmartAggregate sp = new SmartAggregate(sp1, "labs.triglyceride", "demo.gender", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getWifiQ1Quip(TransactionId tid) throws Exception{
        SeqScan s1occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        SmartFilter sel1occupancy = new SmartFilter(new Predicate("occupancy.occupancy", Predicate.Op.GREATER_THAN, new IntField(10)), s1occupancy);
        SeqScan s2wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(5500)), s2wifi);
        SmartFilter sel3wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(3500)), sel2wifi);
        JoinPredicate predName1 = new JoinPredicate("occupancy.lid", Predicate.Op.EQUALS, "wifi.lid");
        SmartJoin join1occupancy = new SmartJoin(predName1, sel1occupancy, sel3wifi);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("occupancy.type"));
        attributes.add(new Attribute("wifi.duration"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join1occupancy);
        SmartAggregate sp = new SmartAggregate(sp1, "wifi.duration", "occupancy.type", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getWifiQ1IDB(TransactionId tid) throws Exception{
        SeqScan s1occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        Impute imp1occupancy = new Impute(new Attribute("occupancy.occupancy"),s1occupancy);
        SmartFilter sel1occupancy = new SmartFilter(new Predicate("occupancy.occupancy", Predicate.Op.GREATER_THAN, new IntField(10)), imp1occupancy);
        SeqScan s2wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        Impute imp2wifi = new Impute(new Attribute("wifi.lid"),s2wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(5500)), imp2wifi);
        SmartFilter sel3wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(3500)), sel2wifi);
        JoinPredicate predName1 = new JoinPredicate("occupancy.lid", Predicate.Op.EQUALS, "wifi.lid");
        SmartJoin join1occupancy = new SmartJoin(predName1, sel1occupancy, sel3wifi);
        Impute imp3occupancy = new Impute(new Attribute("occupancy.type"),join1occupancy);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("occupancy.type"));
        attributes.add(new Attribute("wifi.duration"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, imp3occupancy);
        SmartAggregate sp = new SmartAggregate(sp1, "wifi.duration", "occupancy.type", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getWifiQ2Quip(TransactionId tid) throws Exception{
        SeqScan s1occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        SeqScan s2wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.duration", Predicate.Op.LESS_THAN, new IntField(500)), s2wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(15000)), sel1wifi);
        SmartFilter sel3wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(10000)), sel2wifi);
        JoinPredicate predName1 = new JoinPredicate("occupancy.lid", Predicate.Op.EQUALS, "wifi.lid");
        SmartJoin join1occupancy = new SmartJoin(predName1, s1occupancy, sel3wifi);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("occupancy.type"));
        attributes.add(new Attribute("occupancy.occupancy"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join1occupancy);
        SmartAggregate sp = new SmartAggregate(sp1, "occupancy.occupancy", "occupancy.type", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getWifiQ2IDB(TransactionId tid) throws Exception{
        SeqScan s1occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        SeqScan s2wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        Impute imp1wifi = new Impute(new Attribute("wifi.lid"),s2wifi);
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.duration", Predicate.Op.LESS_THAN, new IntField(500)), imp1wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(15000)), sel1wifi);
        SmartFilter sel3wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(10000)), sel2wifi);
        JoinPredicate predName1 = new JoinPredicate("occupancy.lid", Predicate.Op.EQUALS, "wifi.lid");
        SmartJoin join1occupancy = new SmartJoin(predName1, s1occupancy, sel3wifi);
        Impute imp2occupancy = new Impute(new Attribute("occupancy.type"),join1occupancy);
        Impute imp3occupancy = new Impute(new Attribute("occupancy.occupancy"),imp2occupancy);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("occupancy.type"));
        attributes.add(new Attribute("occupancy.occupancy"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, imp3occupancy);
        SmartAggregate sp = new SmartAggregate(sp1, "occupancy.occupancy", "occupancy.type", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getWifiQ3Quip(TransactionId tid) throws Exception{
        SeqScan s1users= new SeqScan(tid, WiFiusers.getId(), "users");
        SeqScan s2wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(2000)), s2wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(500)), sel1wifi);
        JoinPredicate predName1 = new JoinPredicate("users.mac", Predicate.Op.EQUALS, "wifi.mac");
        SmartJoin join1users = new SmartJoin(predName1, s1users, sel2wifi);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("users.ugroup"));
        attributes.add(new Attribute("wifi.duration"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join1users);
        SmartAggregate sp = new SmartAggregate(sp1, "wifi.duration", "users.ugroup", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getWifiQ3IDB(TransactionId tid) throws Exception{
        SeqScan s1users= new SeqScan(tid, WiFiusers.getId(), "users");
        Impute imp1users = new Impute(new Attribute("users.mac"),s1users);
        SeqScan s2wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(2000)), s2wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(500)), sel1wifi);
        JoinPredicate predName1 = new JoinPredicate("users.mac", Predicate.Op.EQUALS, "wifi.mac");
        SmartJoin join1users = new SmartJoin(predName1, imp1users, sel2wifi);
        Impute imp2users = new Impute(new Attribute("users.ugroup"),join1users);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("users.ugroup"));
        attributes.add(new Attribute("wifi.duration"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, imp2users);
        SmartAggregate sp = new SmartAggregate(sp1, "wifi.duration", "users.ugroup", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getWifiQ4Quip(TransactionId tid) throws Exception{
        SeqScan s1occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        SmartFilter sel1occupancy = new SmartFilter(new Predicate("occupancy.type", Predicate.Op.EQUALS, new IntField(1)), s1occupancy);
        SeqScan s2wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(10000)), s2wifi);
        SmartFilter sel3wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(5000)), sel2wifi);
        JoinPredicate predName1 = new JoinPredicate("occupancy.lid", Predicate.Op.EQUALS, "wifi.lid");
        SmartJoin join1occupancy = new SmartJoin(predName1, sel1occupancy, sel3wifi);
        SeqScan s3users= new SeqScan(tid, WiFiusers.getId(), "users");
        JoinPredicate predName2 = new JoinPredicate("wifi.mac", Predicate.Op.EQUALS, "users.mac");
        SmartJoin join2wifi = new SmartJoin(predName2, join1occupancy, s3users);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("users.ugroup"));
        attributes.add(new Attribute("occupancy.occupancy"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2wifi);
        SmartAggregate sp = new SmartAggregate(sp1, "occupancy.occupancy", "users.ugroup", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getWifiQ4IDB(TransactionId tid) throws Exception{
        SeqScan s1occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        Impute imp1occupancy = new Impute(new Attribute("occupancy.type"),s1occupancy);
        SmartFilter sel1occupancy = new SmartFilter(new Predicate("occupancy.type", Predicate.Op.EQUALS, new IntField(1)), imp1occupancy);
        SeqScan s2wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        Impute imp2wifi = new Impute(new Attribute("wifi.lid"),s2wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(10000)), imp2wifi);
        SmartFilter sel3wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(5000)), sel2wifi);
        JoinPredicate predName1 = new JoinPredicate("occupancy.lid", Predicate.Op.EQUALS, "wifi.lid");
        SmartJoin join1occupancy = new SmartJoin(predName1, sel1occupancy, sel3wifi);
        Impute imp3occupancy = new Impute(new Attribute("occupancy.occupancy"),join1occupancy);
        SeqScan s3users= new SeqScan(tid, WiFiusers.getId(), "users");
        Impute imp4users = new Impute(new Attribute("users.mac"),s3users);
        JoinPredicate predName2 = new JoinPredicate("wifi.mac", Predicate.Op.EQUALS, "users.mac");
        SmartJoin join2wifi = new SmartJoin(predName2, imp3occupancy, imp4users);
        Impute imp5users = new Impute(new Attribute("users.ugroup"),join2wifi);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("users.ugroup"));
        attributes.add(new Attribute("occupancy.occupancy"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, imp5users);
        SmartAggregate sp = new SmartAggregate(sp1, "occupancy.occupancy", "users.ugroup", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getWifiQ5Quip(TransactionId tid) throws Exception{
        SeqScan s1wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(10000)), s1wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(500)), sel1wifi);
        SeqScan s2occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        SmartFilter sel3occupancy = new SmartFilter(new Predicate("occupancy.type", Predicate.Op.EQUALS, new IntField(2)), s2occupancy);
        JoinPredicate predName1 = new JoinPredicate("wifi.lid", Predicate.Op.EQUALS, "occupancy.lid");
        SmartJoin join1wifi = new SmartJoin(predName1, sel2wifi, sel3occupancy);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("occupancy.occupancy"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join1wifi);
        SmartAggregate sp = new SmartAggregate(sp1, "occupancy.occupancy", "", Aggregator.Op.MAX);
        return sp;
    }

    public Operator getWifiQ5IDB(TransactionId tid) throws Exception{
        SeqScan s1wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        Impute imp1wifi = new Impute(new Attribute("wifi.lid"),s1wifi);
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(10000)), imp1wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(500)), sel1wifi);
        SeqScan s2occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        Impute imp2occupancy = new Impute(new Attribute("occupancy.type"),s2occupancy);
        Impute imp3occupancy = new Impute(new Attribute("occupancy.occupancy"),imp2occupancy);
        SmartFilter sel3occupancy = new SmartFilter(new Predicate("occupancy.type", Predicate.Op.EQUALS, new IntField(2)), imp3occupancy);
        JoinPredicate predName1 = new JoinPredicate("wifi.lid", Predicate.Op.EQUALS, "occupancy.lid");
        SmartJoin join1wifi = new SmartJoin(predName1, sel2wifi, sel3occupancy);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("occupancy.occupancy"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join1wifi);
        SmartAggregate sp = new SmartAggregate(sp1, "occupancy.occupancy", "", Aggregator.Op.MAX);
        return sp;
    }

    public Operator getWifiQ6Quip(TransactionId tid) throws Exception{
        SeqScan s1users= new SeqScan(tid, WiFiusers.getId(), "users");
        SmartFilter sel1users = new SmartFilter(new Predicate("users.ugroup", Predicate.Op.EQUALS, new IntField(2)), s1users);
        SeqScan s2wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(800)), s2wifi);
        SmartFilter sel3wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(2000)), sel2wifi);
        JoinPredicate predName1 = new JoinPredicate("users.mac", Predicate.Op.EQUALS, "wifi.mac");
        SmartJoin join1users = new SmartJoin(predName1, sel1users, sel3wifi);
        SeqScan s3occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        JoinPredicate predName2 = new JoinPredicate("wifi.lid", Predicate.Op.EQUALS, "occupancy.lid");
        SmartJoin join2wifi = new SmartJoin(predName2, join1users, s3occupancy);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("occupancy.type"));
        attributes.add(new Attribute("occupancy.occupancy"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2wifi);
        SmartAggregate sp = new SmartAggregate(sp1, "occupancy.occupancy", "occupancy.type", Aggregator.Op.MAX);
        return sp;
    }

    public Operator getWifiQ6IDB(TransactionId tid) throws Exception{
        SeqScan s1users= new SeqScan(tid, WiFiusers.getId(), "users");
        Impute imp1users = new Impute(new Attribute("users.ugroup"),s1users);
        Impute imp2users = new Impute(new Attribute("users.mac"),imp1users);
        SmartFilter sel1users = new SmartFilter(new Predicate("users.ugroup", Predicate.Op.EQUALS, new IntField(2)), imp2users);
        SeqScan s2wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        Impute imp3wifi = new Impute(new Attribute("wifi.lid"),s2wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(800)), imp3wifi);
        SmartFilter sel3wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(2000)), sel2wifi);
        JoinPredicate predName1 = new JoinPredicate("users.mac", Predicate.Op.EQUALS, "wifi.mac");
        SmartJoin join1users = new SmartJoin(predName1, sel1users, sel3wifi);
        SeqScan s3occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        JoinPredicate predName2 = new JoinPredicate("wifi.lid", Predicate.Op.EQUALS, "occupancy.lid");
        SmartJoin join2wifi = new SmartJoin(predName2, join1users, s3occupancy);
        Impute imp4occupancy = new Impute(new Attribute("occupancy.type"),join2wifi);
        Impute imp5occupancy = new Impute(new Attribute("occupancy.occupancy"),imp4occupancy);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("occupancy.type"));
        attributes.add(new Attribute("occupancy.occupancy"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, imp5occupancy);
        SmartAggregate sp = new SmartAggregate(sp1, "occupancy.occupancy", "occupancy.type", Aggregator.Op.MAX);
        return sp;
    }

    public Operator getWifiQ7Quip(TransactionId tid) throws Exception{
        SeqScan s1wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(8000)), s1wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(1000)), sel1wifi);
        SeqScan s2occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        SmartFilter sel3occupancy = new SmartFilter(new Predicate("occupancy.type", Predicate.Op.EQUALS, new IntField(2)), s2occupancy);
        JoinPredicate predName1 = new JoinPredicate("wifi.lid", Predicate.Op.EQUALS, "occupancy.lid");
        SmartJoin join1wifi = new SmartJoin(predName1, sel2wifi, sel3occupancy);
        SeqScan s3users= new SeqScan(tid, WiFiusers.getId(), "users");
        SmartFilter sel4users = new SmartFilter(new Predicate("users.ugroup", Predicate.Op.EQUALS, new IntField(1)), s3users);
        JoinPredicate predName2 = new JoinPredicate("wifi.mac", Predicate.Op.EQUALS, "users.mac");
        SmartJoin join2wifi = new SmartJoin(predName2, join1wifi, sel4users);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("users.name"));
        attributes.add(new Attribute("wifi.lid"));
        attributes.add(new Attribute("wifi.st"));
        attributes.add(new Attribute("wifi.et"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE,Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2wifi);
        //SmartAggregate sp = new SmartAggregate(sp1, "", "", Aggregator.Op.);
        return sp1;
    }

    public Operator getWifiQ7IDB(TransactionId tid) throws Exception{
        SeqScan s1wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        Impute imp1wifi = new Impute(new Attribute("wifi.lid"),s1wifi);
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(8000)), imp1wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(1000)), sel1wifi);
        SeqScan s2occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        Impute imp2occupancy = new Impute(new Attribute("occupancy.type"),s2occupancy);
        SmartFilter sel3occupancy = new SmartFilter(new Predicate("occupancy.type", Predicate.Op.EQUALS, new IntField(2)), imp2occupancy);
        JoinPredicate predName1 = new JoinPredicate("wifi.lid", Predicate.Op.EQUALS, "occupancy.lid");
        SmartJoin join1wifi = new SmartJoin(predName1, sel2wifi, sel3occupancy);
        SeqScan s3users= new SeqScan(tid, WiFiusers.getId(), "users");
        Impute imp3users = new Impute(new Attribute("users.ugroup"),s3users);
        Impute imp4users = new Impute(new Attribute("users.mac"),imp3users);
        SmartFilter sel4users = new SmartFilter(new Predicate("users.ugroup", Predicate.Op.EQUALS, new IntField(1)), imp4users);
        JoinPredicate predName2 = new JoinPredicate("wifi.mac", Predicate.Op.EQUALS, "users.mac");
        SmartJoin join2wifi = new SmartJoin(predName2, join1wifi, sel4users);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("users.name"));
        attributes.add(new Attribute("wifi.lid"));
        attributes.add(new Attribute("wifi.st"));
        attributes.add(new Attribute("wifi.et"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE,Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2wifi);
        //SmartAggregate sp = new SmartAggregate(sp1, "", "", Aggregator.Op.);
        return sp1;
    }

    public Operator getWifiQ8Quip(TransactionId tid) throws Exception{
        SeqScan s1wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(200)), s1wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(1000)), sel1wifi);
        SeqScan s2occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        JoinPredicate predName1 = new JoinPredicate("wifi.lid", Predicate.Op.EQUALS, "occupancy.lid");
        SmartJoin join1wifi = new SmartJoin(predName1, sel2wifi, s2occupancy);
        SeqScan s3users= new SeqScan(tid, WiFiusers.getId(), "users");
        SmartFilter sel3users = new SmartFilter(new Predicate("users.ugroup", Predicate.Op.EQUALS, new IntField(2)), s3users);
        JoinPredicate predName2 = new JoinPredicate("wifi.mac", Predicate.Op.EQUALS, "users.mac");
        SmartJoin join2wifi = new SmartJoin(predName2, join1wifi, sel3users);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("occupancy.type"));
        attributes.add(new Attribute("wifi.duration"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2wifi);
        SmartAggregate sp = new SmartAggregate(sp1, "wifi.duration", "occupancy.type", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getWifiQ8IDB(TransactionId tid) throws Exception{
        SeqScan s1wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        Impute imp1wifi = new Impute(new Attribute("wifi.lid"),s1wifi);
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(200)), imp1wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(1000)), sel1wifi);
        SeqScan s2occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        JoinPredicate predName1 = new JoinPredicate("wifi.lid", Predicate.Op.EQUALS, "occupancy.lid");
        SmartJoin join1wifi = new SmartJoin(predName1, sel2wifi, s2occupancy);
        Impute imp2occupancy = new Impute(new Attribute("occupancy.type"),join1wifi);
        SeqScan s3users= new SeqScan(tid, WiFiusers.getId(), "users");
        Impute imp3users = new Impute(new Attribute("users.mac"),s3users);
        SmartFilter sel3users = new SmartFilter(new Predicate("users.ugroup", Predicate.Op.EQUALS, new IntField(2)), imp3users);
        JoinPredicate predName2 = new JoinPredicate("wifi.mac", Predicate.Op.EQUALS, "users.mac");
        SmartJoin join2wifi = new SmartJoin(predName2, imp2occupancy, sel3users);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("occupancy.type"));
        attributes.add(new Attribute("wifi.duration"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2wifi);
        SmartAggregate sp = new SmartAggregate(sp1, "wifi.duration", "occupancy.type", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getWifiQ9Quip(TransactionId tid) throws Exception{
        SeqScan s1wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(6000)), s1wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(2500)), sel1wifi);
        SeqScan s2occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        SmartFilter sel3occupancy = new SmartFilter(new Predicate("occupancy.type", Predicate.Op.EQUALS, new IntField(1)), s2occupancy);
        JoinPredicate predName1 = new JoinPredicate("wifi.lid", Predicate.Op.EQUALS, "occupancy.lid");
        SmartJoin join1wifi = new SmartJoin(predName1, sel2wifi, sel3occupancy);
        SeqScan s3users= new SeqScan(tid, WiFiusers.getId(), "users");
        JoinPredicate predName2 = new JoinPredicate("wifi.mac", Predicate.Op.EQUALS, "users.mac");
        SmartJoin join2wifi = new SmartJoin(predName2, join1wifi, s3users);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("users.mac"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2wifi);
        SmartAggregate sp = new SmartAggregate(sp1, "users.mac", "", Aggregator.Op.COUNT);
        return sp;
    }

    public Operator getWifiQ9IDB(TransactionId tid) throws Exception{
        SeqScan s1wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        Impute imp1wifi = new Impute(new Attribute("wifi.lid"),s1wifi);
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(6000)), imp1wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(2500)), sel1wifi);
        SeqScan s2occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        Impute imp2occupancy = new Impute(new Attribute("occupancy.type"),s2occupancy);
        SmartFilter sel3occupancy = new SmartFilter(new Predicate("occupancy.type", Predicate.Op.EQUALS, new IntField(1)), imp2occupancy);
        JoinPredicate predName1 = new JoinPredicate("wifi.lid", Predicate.Op.EQUALS, "occupancy.lid");
        SmartJoin join1wifi = new SmartJoin(predName1, sel2wifi, sel3occupancy);
        SeqScan s3users= new SeqScan(tid, WiFiusers.getId(), "users");
        Impute imp3users = new Impute(new Attribute("users.mac"),s3users);
        JoinPredicate predName2 = new JoinPredicate("wifi.mac", Predicate.Op.EQUALS, "users.mac");
        SmartJoin join2wifi = new SmartJoin(predName2, join1wifi, imp3users);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("users.mac"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2wifi);
        SmartAggregate sp = new SmartAggregate(sp1, "users.mac", "", Aggregator.Op.COUNT);
        return sp;
    }

    public Operator getWifiQ10Quip(TransactionId tid) throws Exception{
        SeqScan s1users= new SeqScan(tid, WiFiusers.getId(), "users");
        SeqScan s2wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(5000)), s2wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(2000)), sel1wifi);
        JoinPredicate predName1 = new JoinPredicate("users.mac", Predicate.Op.EQUALS, "wifi.mac");
        SmartJoin join1users = new SmartJoin(predName1, s1users, sel2wifi);
        SeqScan s3occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        JoinPredicate predName2 = new JoinPredicate("wifi.lid", Predicate.Op.EQUALS, "occupancy.lid");
        SmartJoin join2wifi = new SmartJoin(predName2, join1users, s3occupancy);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("users.ugroup"));
        attributes.add(new Attribute("wifi.duration"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2wifi);
        SmartAggregate sp = new SmartAggregate(sp1, "wifi.duration", "users.ugroup", Aggregator.Op.SUM);
        return sp;
    }

    public Operator getWifiQ10IDB(TransactionId tid) throws Exception{
        SeqScan s1users= new SeqScan(tid, WiFiusers.getId(), "users");
        Impute imp1users = new Impute(new Attribute("users.mac"),s1users);
        SeqScan s2wifi= new SeqScan(tid, WiFiwifi.getId(), "wifi");
        Impute imp2wifi = new Impute(new Attribute("wifi.lid"),s2wifi);
        SmartFilter sel1wifi = new SmartFilter(new Predicate("wifi.et", Predicate.Op.LESS_THAN, new IntField(5000)), imp2wifi);
        SmartFilter sel2wifi = new SmartFilter(new Predicate("wifi.st", Predicate.Op.GREATER_THAN, new IntField(2000)), sel1wifi);
        JoinPredicate predName1 = new JoinPredicate("users.mac", Predicate.Op.EQUALS, "wifi.mac");
        SmartJoin join1users = new SmartJoin(predName1, imp1users, sel2wifi);
        Impute imp3users = new Impute(new Attribute("users.ugroup"),join1users);
        SeqScan s3occupancy= new SeqScan(tid, WiFioccupancy.getId(), "occupancy");
        JoinPredicate predName2 = new JoinPredicate("wifi.lid", Predicate.Op.EQUALS, "occupancy.lid");
        SmartJoin join2wifi = new SmartJoin(predName2, imp3users, s3occupancy);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("users.ugroup"));
        attributes.add(new Attribute("wifi.duration"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2wifi);
        SmartAggregate sp = new SmartAggregate(sp1, "wifi.duration", "users.ugroup", Aggregator.Op.SUM);
        return sp;
    }

    public Operator getACSQ1IDB(TransactionId tid) throws Exception{
        SeqScan s1t0= new SeqScan(tid, ACSt0.getId(), "t0");
        Impute imp1t0 = new Impute(new Attribute("t0.c2"),s1t0);
        Impute imp2t0 = new Impute(new Attribute("t0.c1"),imp1t0);
        Impute imp3t0 = new Impute(new Attribute("t0.c4"),imp2t0);
        Impute imp4t0 = new Impute(new Attribute("t0.c3"),imp3t0);
        SmartFilter sel1t0 = new SmartFilter(new Predicate("t0.c3", Predicate.Op.LESS_THAN_OR_EQ, new IntField(1000)), imp4t0);
        SmartFilter sel2t0 = new SmartFilter(new Predicate("t0.c4", Predicate.Op.LESS_THAN_OR_EQ, new IntField(4500)), sel1t0);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("t0.c2"));
        attributes.add(new Attribute("t0.c1"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, sel2t0);
        SmartAggregate sp = new SmartAggregate(sp1, "t0.c1", "t0.c2", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getACSQ1Quip(TransactionId tid) throws Exception{
        SeqScan s1t0= new SeqScan(tid, ACSt0.getId(), "t0");
        SmartFilter sel1t0 = new SmartFilter(new Predicate("t0.c3", Predicate.Op.LESS_THAN_OR_EQ, new IntField(1000)), s1t0);
        SmartFilter sel2t0 = new SmartFilter(new Predicate("t0.c4", Predicate.Op.LESS_THAN_OR_EQ, new IntField(4500)), sel1t0);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("t0.c2"));
        attributes.add(new Attribute("t0.c1"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, sel2t0);
        SmartAggregate sp = new SmartAggregate(sp1, "t0.c1", "t0.c2", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getACSQ2IDB(TransactionId tid) throws Exception{
        SeqScan s1t3= new SeqScan(tid, ACSt3.getId(), "t3");
        Impute imp1t3 = new Impute(new Attribute("t3.c23"),s1t3);
        Impute imp2t3 = new Impute(new Attribute("t3.c21"),imp1t3);
        Impute imp3t3 = new Impute(new Attribute("t3.c18"),imp2t3);
        SmartFilter sel1t3 = new SmartFilter(new Predicate("t3.c18", Predicate.Op.LESS_THAN_OR_EQ, new IntField(4000)), imp3t3);
        SmartFilter sel2t3 = new SmartFilter(new Predicate("t3.c21", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(1500)), sel1t3);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("t3.c15"));
        attributes.add(new Attribute("t3.c23"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, sel2t3);
        SmartAggregate sp = new SmartAggregate(sp1, "t3.c23", "t3.c15", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getACSQ2Quip(TransactionId tid) throws Exception{
        SeqScan s1t3= new SeqScan(tid, ACSt3.getId(), "t3");
        SmartFilter sel1t3 = new SmartFilter(new Predicate("t3.c18", Predicate.Op.LESS_THAN_OR_EQ, new IntField(4000)), s1t3);
        SmartFilter sel2t3 = new SmartFilter(new Predicate("t3.c21", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(1500)), sel1t3);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("t3.c15"));
        attributes.add(new Attribute("t3.c23"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, sel2t3);
        SmartAggregate sp = new SmartAggregate(sp1, "t3.c23", "t3.c15", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getACSQ3IDB(TransactionId tid) throws Exception{
        SeqScan s1t0= new SeqScan(tid, ACSt0.getId(), "t0");
        Impute imp1t0 = new Impute(new Attribute("t0.c2"),s1t0);
        SmartFilter sel1t0 = new SmartFilter(new Predicate("t0.c2", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(800)), imp1t0);
        Impute imp2t0 = new Impute(new Attribute("t0.c3"),sel1t0);
        SeqScan s2t1= new SeqScan(tid, ACSt1.getId(), "t1");
        Impute imp3t1 = new Impute(new Attribute("t1.c7"),s2t1);
        Impute imp4t1 = new Impute(new Attribute("t1.c9"),imp3t1);
        SmartFilter sel2t1 = new SmartFilter(new Predicate("t1.c9", Predicate.Op.LESS_THAN_OR_EQ, new IntField(2000)), imp4t1);
        JoinPredicate predName1 = new JoinPredicate("t0.c0", Predicate.Op.EQUALS, "t1.c0");
        SmartJoin join1t0 = new SmartJoin(predName1, imp2t0, sel2t1);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("t1.c7"));
        attributes.add(new Attribute("t0.c3"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join1t0);
        SmartAggregate sp = new SmartAggregate(sp1, "t0.c3", "t1.c7", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getACSQ3Quip(TransactionId tid) throws Exception{
        SeqScan s1t0= new SeqScan(tid, ACSt0.getId(), "t0");
        SmartFilter sel1t0 = new SmartFilter(new Predicate("t0.c2", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(800)), s1t0);
        SeqScan s2t1= new SeqScan(tid, ACSt1.getId(), "t1");
        SmartFilter sel2t1 = new SmartFilter(new Predicate("t1.c9", Predicate.Op.LESS_THAN_OR_EQ, new IntField(2000)), s2t1);
        JoinPredicate predName1 = new JoinPredicate("t0.c0", Predicate.Op.EQUALS, "t1.c0");
        SmartJoin join1t0 = new SmartJoin(predName1, sel1t0, sel2t1);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("t1.c7"));
        attributes.add(new Attribute("t0.c3"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join1t0);
        SmartAggregate sp = new SmartAggregate(sp1, "t0.c3", "t1.c7", Aggregator.Op.AVG);
        return sp;
    }

    public Operator getACSQ4IDB(TransactionId tid) throws Exception{
        SeqScan s1t3= new SeqScan(tid, ACSt3.getId(), "t3");
        Impute imp1t3 = new Impute(new Attribute("t3.c21"),s1t3);
        SmartFilter sel1t3 = new SmartFilter(new Predicate("t3.c21", Predicate.Op.LESS_THAN_OR_EQ, new IntField(4000)), imp1t3);
        SeqScan s2t4= new SeqScan(tid, ACSt4.getId(), "t4");
        Impute imp2t4 = new Impute(new Attribute("t4.c32"),s2t4);
        Impute imp3t4 = new Impute(new Attribute("t4.c31"),imp2t4);
        SmartFilter sel2t4 = new SmartFilter(new Predicate("t4.c32", Predicate.Op.LESS_THAN_OR_EQ, new IntField(7500)), imp3t4);
        JoinPredicate predName1 = new JoinPredicate("t3.c20", Predicate.Op.EQUALS, "t4.c20");
        SmartJoin join1t3 = new SmartJoin(predName1, sel1t3, sel2t4);
        SeqScan s3t2= new SeqScan(tid, ACSt2.getId(), "t2");
        Impute imp4t2 = new Impute(new Attribute("t2.c11"),s3t2);
        JoinPredicate predName2 = new JoinPredicate("t3.c15", Predicate.Op.EQUALS, "t2.c15");
        SmartJoin join2t3 = new SmartJoin(predName2, join1t3, imp4t2);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("t4.c31"));
        attributes.add(new Attribute("t2.c11"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2t3);
        SmartAggregate sp = new SmartAggregate(sp1, "t2.c11", "t4.c31", Aggregator.Op.MAX);
        return sp;
    }

    public Operator getACSQ4Quip(TransactionId tid) throws Exception{
        SeqScan s1t3= new SeqScan(tid, ACSt3.getId(), "t3");
        SmartFilter sel1t3 = new SmartFilter(new Predicate("t3.c21", Predicate.Op.LESS_THAN_OR_EQ, new IntField(4000)), s1t3);
        SeqScan s2t4= new SeqScan(tid, ACSt4.getId(), "t4");
        SmartFilter sel2t4 = new SmartFilter(new Predicate("t4.c32", Predicate.Op.LESS_THAN_OR_EQ, new IntField(7500)), s2t4);
        JoinPredicate predName1 = new JoinPredicate("t3.c20", Predicate.Op.EQUALS, "t4.c20");
        SmartJoin join1t3 = new SmartJoin(predName1, sel1t3, sel2t4);
        SeqScan s3t2= new SeqScan(tid, ACSt2.getId(), "t2");
        JoinPredicate predName2 = new JoinPredicate("t3.c15", Predicate.Op.EQUALS, "t2.c15");
        SmartJoin join2t3 = new SmartJoin(predName2, join1t3, s3t2);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("t4.c31"));
        attributes.add(new Attribute("t2.c11"));
        Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join2t3);
        SmartAggregate sp = new SmartAggregate(sp1, "t2.c11", "t4.c31", Aggregator.Op.MAX);
        return sp;
    }

    public Operator getACSQ5IDB(TransactionId tid) throws Exception{
        SeqScan s1t1= new SeqScan(tid, ACSt1.getId(), "t1");
        Impute imp1t1 = new Impute(new Attribute("t1.c6"),s1t1);
        SmartFilter sel1t1 = new SmartFilter(new Predicate("t1.c6", Predicate.Op.LESS_THAN_OR_EQ, new IntField(2000)), imp1t1);
        SeqScan s2t2= new SeqScan(tid, ACSt2.getId(), "t2");
        Impute imp2t2 = new Impute(new Attribute("t2.c11"),s2t2);
        JoinPredicate predName1 = new JoinPredicate("t1.c9", Predicate.Op.EQUALS, "t2.c9");
        SmartJoin join1t1 = new SmartJoin(predName1, sel1t1, imp2t2);
        SeqScan s3t0= new SeqScan(tid, ACSt0.getId(), "t0");
        JoinPredicate predName2 = new JoinPredicate("t1.c0", Predicate.Op.EQUALS, "t0.c0");
        SmartJoin join2t1 = new SmartJoin(predName2, join1t1, s3t0);
        SeqScan s4t3= new SeqScan(tid, ACSt3.getId(), "t3");
        Impute imp3t3 = new Impute(new Attribute("t3.c21"),s4t3);
        SmartFilter sel2t3 = new SmartFilter(new Predicate("t3.c21", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(5500)), imp3t3);
        JoinPredicate predName3 = new JoinPredicate("t2.c15", Predicate.Op.EQUALS, "t3.c15");
        SmartJoin join3t2 = new SmartJoin(predName3, join2t1, sel2t3);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("t2.c11"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join3t2);
        SmartAggregate sp = new SmartAggregate(sp1, "t2.c11", "", Aggregator.Op.MIN);
        return sp;
    }

    public Operator getACSQ5Quip(TransactionId tid) throws Exception{
        SeqScan s1t1= new SeqScan(tid, ACSt1.getId(), "t1");
        SmartFilter sel1t1 = new SmartFilter(new Predicate("t1.c6", Predicate.Op.LESS_THAN_OR_EQ, new IntField(800)), s1t1);
        SeqScan s2t2= new SeqScan(tid, ACSt2.getId(), "t2");
        JoinPredicate predName1 = new JoinPredicate("t1.c9", Predicate.Op.EQUALS, "t2.c9");
        SmartJoin join1t1 = new SmartJoin(predName1, sel1t1, s2t2);
        SeqScan s3t0= new SeqScan(tid, ACSt0.getId(), "t0");
        JoinPredicate predName2 = new JoinPredicate("t1.c0", Predicate.Op.EQUALS, "t0.c0");
        SmartJoin join2t1 = new SmartJoin(predName2, join1t1, s3t0);
        SeqScan s4t3= new SeqScan(tid, ACSt3.getId(), "t3");
        SmartFilter sel2t3 = new SmartFilter(new Predicate("t3.c21", Predicate.Op.GREATER_THAN_OR_EQ, new IntField(12000)), s4t3);
        JoinPredicate predName3 = new JoinPredicate("t2.c15", Predicate.Op.EQUALS, "t3.c15");
        SmartJoin join3t2 = new SmartJoin(predName3, join2t1, sel2t3);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("t2.c11"));
        Type[] types = new Type[]{Type.INT_TYPE};
        SmartProject sp1 = new SmartProject(attributes, types, join3t2);
        SmartAggregate sp = new SmartAggregate(sp1, "t2.c11", "", Aggregator.Op.MIN);
        return sp;
    }

    public Operator getACSQ6IDB(TransactionId tid) throws Exception{
        return null;
    }

    public Operator getACSQ6Quip(TransactionId tid) throws Exception{
        return null;
    }
}
