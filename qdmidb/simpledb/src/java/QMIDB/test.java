package QMIDB;

import org.w3c.dom.Attr;
import simpledb.*;

import javax.annotation.processing.SupportedSourceVersion;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public class test {

    public static void testPath()throws IOException{
        String catalog = "../cdc/queries.txt";
        String queries = "../queryplancodes/queries.txt";
        String catalogPath = Paths.get(catalog).toAbsolutePath().toString();
        String queriesPath = Paths.get(queries).toAbsolutePath().toString();
        System.out.println(catalogPath);
        System.out.println(queriesPath);
    }

    public static void testSplit(){
        String row = ",4,3,4,69,1,73557,,1,,";
        boolean flag = false;
        //last one value is missing
        String output = "";
        List<String> strs = new ArrayList<>();
        String str = "";
        for(int i=0;i<row.length();i++){
            if(i == 0 && row.charAt(i) == ','){
                output += "#";
            }
            output += row.charAt(i);
            if(i<row.length()-1 && row.charAt(i) == ',' && row.charAt(i+1) == ','){
                output += "#";
            }
            if(i == row.length()-1 && row.charAt(i) == ','){
                output += "#";
                strs.add("#");
            }
            if(row.charAt(i) == ','){
                if(i==0){
                    strs.add("#");
                }else{
                    if(str.equals("")){
                        strs.add("#");
                    }
                    else{
                        strs.add(str);
                        str = "";
                    }
                }
            }else{
                str += row.charAt(i);
            }
        }
        System.out.println(output);
        for(int i=0;i<strs.size();i++){
            System.out.println(strs.get(i));
        }
    }

    public static void getQueriesBySelectivity(double selectivity){
        System.out.println(PredicateSet.getPredicateSet().size());
        for(int i=0;i<PredicateSet.getPredicateSet().size();i++){
            PredicateUnit pred = PredicateSet.getPredicateSet().get(i);
            String type = pred.getType();
            switch (type){
                case "Filter":
                    System.out.println("F");
                    Attribute attr = pred.getFilterAttribute();
                    Predicate.Op op = pred.getOp();
                    int operand = Buffer.getQuerySelectivity(attr, selectivity, op);
                    System.out.println(attr.getAttribute() + " " + op + " " + operand);
                    break;
                case "Join":
                    System.out.println("J");
                    System.out.println(pred.getLeft().getAttribute() + " " + pred.getOp() + " " + pred.getRight().getAttribute());
                    break;
                case "Aggregate":
                    System.out.println("A");
                    System.out.println(pred.getAggregateAttribute() + " " + pred.getAop());
                    break;
            }

        }
    }

    public static void runCDC(int queryID, String dataset, String method)throws Exception{
        QueryPlan QP = new QueryPlan();
        //set up heap files
        switch (dataset){
            case "CDC":
                QP.setupCDCHeapFiles();
                break;
            case "WiFi":
                QP.setupWiFiHeapFiles();
                break;
            case "ACS":
                QP.setupACSHeapFiles();
                break;
        }
        TransactionId tid = new TransactionId();
        Operator o = QP.getQueryPlan(queryID, tid, dataset, method);
        Statistics.setStartTime(System.currentTimeMillis());
        int NumTemporal = 0;
        try {
            o.open();
            while (o.hasNext()) {
                Tuple tup = o.next();
                NumTemporal++;
                //System.out.println(tup);
            }
            o.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("number of tuples in temporal results is : " + NumTemporal);

        System.out.println("Total number of missing values in datasets: "+ Schema.getTotalNumberOfMissingValues());
        if(method.equals("ImputeDB")){
            System.out.println("Q" + queryID +  ": Total number of imputation times -- imputedDB cleaning: " + ImputeFactory.getImputationTimes());
        }else{
            System.out.println("Q" + queryID +  ": Total number of imputation times -- Quip cleaning: " + ImputeFactory.getImputationTimes());
        }
        System.out.println("Running Time:" + (Statistics.getDuration() + ImputeFactory.getImputationCost()));
        System.out.println("Number of tuples filtered by MAX/MIN optimization: " + AggregateOptimization.getNumOfFilteredTuples());
    }

    public static void testComplexQuery() throws Exception{
        Type types1[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE};
        String names1[] = new String[]{ "R.a", "R.b"};

        TupleDesc td1 = new TupleDesc(types1, names1);

        Type types2[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE};
        String names2[] = new String[]{"S.b", "S.c" };

        TupleDesc td2 = new TupleDesc(types2, names2);
        //td1.print();

        Type types3[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE};
        String names3[] = new String[]{ "T.a", "T.d"};

        TupleDesc td3 = new TupleDesc(types3, names3);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema the tables.
        HeapFile table1 = new HeapFile(new File("simpledb/testdata/R1.dat"), td1);
        Database.getCatalog().addTable(table1, "R");

        //table1.getTupleDesc().print();

        HeapFile table2 = new HeapFile(new File("simpledb/testdata/S1.dat"), td2);
        Database.getCatalog().addTable(table2, "S");

        HeapFile table3 = new HeapFile(new File("simpledb/testdata/T1.dat"), td3);
        Database.getCatalog().addTable(table3, "T");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "R");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "S");
        SeqScan ss3 = new SeqScan(tid, table3.getId(), "T");
        //System.out.println(ss1.getTupleDesc());

        //ss1.getTupleDesc().print();

        // create a filter for the where condition
        SmartFilter sf1 = new SmartFilter(
                new Predicate("S.c", Predicate.Op.GREATER_THAN, new IntField(0)), ss2);

        //test smartJoin
        JoinPredicate p1 = new JoinPredicate("R.b", Predicate.Op.EQUALS, "S.b");
        SmartJoin sj1 = new SmartJoin(p1,ss1,sf1);

        JoinPredicate p2 = new JoinPredicate("R.a", Predicate.Op.EQUALS, "T.a");
        SmartJoin sj2 = new SmartJoin(p2,sj1,ss3);


        //test smartProject
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("R.a"));
        attributes.add(new Attribute("R.b"));
        attributes.add(new Attribute("S.b"));
        attributes.add(new Attribute("S.c"));
        attributes.add(new Attribute("T.a"));
        attributes.add(new Attribute("T.d"));
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj2);


        // and run it
        //Statistics.setStartTime(System.currentTimeMillis());
        try {
            sp.open();
            while (sp.hasNext()) {
                Tuple tup = sp.next();
                System.out.println(tup);
            }
            sp.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //HashTables.print();
    }

}
