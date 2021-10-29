package QMIDB;

import org.w3c.dom.Attr;
import simpledb.*;

import javax.annotation.processing.SupportedSourceVersion;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public class test {


    public static void testPath()throws IOException{
        String catalog = "demo.db/catalog.txt";
        String queries = "demo.db/queries.txt";
        String catalogPath = Paths.get(catalog).toAbsolutePath().toString();
        String queriesPath = Paths.get(queries).toAbsolutePath().toString();
        System.out.println(catalogPath);
        System.out.println(queriesPath);
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

    public static void testHashTable(){
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};
        TupleDesc descriptor = new TupleDesc(types, names);
        Field fields[] = new Field[]{new IntField(1),new IntField(2),new IntField(3)};
        Tuple t1 = new Tuple(descriptor, fields);
        Field fields1[] = new Field[]{new IntField(2),new IntField(4),new IntField(-1)};
        Tuple t2 = new Tuple(descriptor, fields1);
        Tuple t3 = new Tuple(t1,t2);

        Field key1 = new IntField(1);
        String attribute1 = "field0";

        HashMap<Field, List<Tuple>> table = new HashMap<>();
        table.put(key1, new ArrayList<Tuple>());
        table.get(key1).add(t1);

        HashTables.addHashTable(attribute1,new HashTable(attribute1, table));
        System.out.println("hash table 1: ");

        HashTables.print();

        HashMap<Field, List<Tuple>> table1 = new HashMap<>();
        Field key2 = new IntField(4);
        String attribute2 = "field1";

        table1.put(key2, new ArrayList<Tuple>());
        table1.get(key2).add(t2);

        HashTables.addHashTable(attribute2,new HashTable(attribute2, table1));

        System.out.println("hash table 2: ");
        HashTables.print();
    }

    public static void testHashMap(){
        HashMap<String, HashMap<Integer, Integer>> hashmap = new HashMap<>();
        HashMap<Integer, Integer> subhash = new HashMap<>();
        subhash.put(1,1);
        subhash.put(2,4);
        hashmap.put("a",subhash);

        //print hashmap
        for(Map.Entry<String, HashMap<Integer, Integer>> iter : hashmap.entrySet()){
            System.out.println("key: " + iter.getKey());
            for(Map.Entry<Integer, Integer> iter1: iter.getValue().entrySet()){
                System.out.println(iter1.getKey() + " " + iter1.getValue());
            }
        }

        subhash = new HashMap<>();
        subhash.put(3,6);
        subhash.put(4,8);
        hashmap.put("b",subhash);

        //print hashmap
        for(Map.Entry<String, HashMap<Integer, Integer>> iter : hashmap.entrySet()){
            System.out.println("key: " + iter.getKey());
            for(Map.Entry<Integer, Integer> iter1: iter.getValue().entrySet()){
                System.out.println(iter1.getKey() + " " + iter1.getValue());
            }
        }

    }

    public static void testList(){
        List<Tuple> matching = new ArrayList<>();
        List<Tuple> matching1 = new ArrayList<>();

        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};
        TupleDesc descriptor = new TupleDesc(types, names);
        Field fields[] = new Field[]{new IntField(1),new IntField(2),new IntField(3)};
        Tuple t1 = new Tuple(descriptor, fields);
        Field fields1[] = new Field[]{new IntField(2),new IntField(4),new IntField(-1)};
        Tuple t2 = new Tuple(descriptor, fields1);
        Tuple t3 = new Tuple(t1,t2);


        matching.add(t1);
        matching.add(t2);

        matching1.add(t3);

        matching1.add(matching.get(0));


        for(int i=0;i<matching1.size();i++){
            System.out.println(matching1.get(i));
        }
    }

    public static void testScan(){
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        File file = new File("simpledb/testdata/table_3_col.dat");
        HeapFile table1 = new HeapFile(file, descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId(), "test");
        try {// and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println("Exception : " + e);
        }
    }

    public static void testTuple(){
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};
        TupleDesc descriptor = new TupleDesc(types, names);
        Field fields[] = new Field[]{new IntField(1),new IntField(2),new IntField(3)};
        Tuple t1 = new Tuple(descriptor, fields);
        Field fields1[] = new Field[]{new IntField(-1),new IntField(-1),new IntField(-1)};
        Tuple t2 = new Tuple(descriptor, fields1);
        Tuple t3 = new Tuple(t1,t2);
        System.out.println(t1.getTupleDesc().getFieldName(0));
    }

    public static void testField(){
        Field[] fields = new Field[5];
        fields[0]=new IntField(1);
        fields[1] = new IntField(2);
        System.out.println(fields[1]);
    }

    public static void printSchema(){
        fileHandles fH = new fileHandles();
        List<Attribute> schema = fH.readSchema();
        List<PredicateUnit> predicates = fH.readPredicates();
        System.out.println("printing schema");
        for(int i=0;i<schema.size();i++){
            System.out.println(schema.get(i).getAttribute());
        }
        System.out.println("printing predicates");
        for(int i=0;i<predicates.size();i++){
            predicates.get(i).print();
        }
    }

    public static void testSubDesc(){
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};
        TupleDesc descriptor = new TupleDesc(types, names);
        System.out.println(descriptor.SubTupleDesc(2,1));
    }

    public static void testArray(String s, char a){
        List<String> str = new ArrayList<>();
        int pre = 0;
        boolean flag = false;
        for(int i=0;i<s.length();i++){
            char c = s.charAt(i);
            if(c == a){
                if(i==0){//first char is missing
                    str.add("*");
                }else{
                    if(!flag){
                        flag = true;
                        str.add(s.substring(pre,i));
                        pre = i;
                        continue;
                    }
                    if(i == pre+1){//missing
                        str.add("*");
                    }else{
                        str.add(s.substring(pre+1,i));
                    }
                    if(i == s.length()-1){
                        str.add("*");
                    }
                }
                pre = i;
            }
        }
        for(int i=0;i<str.size();i++){
            System.out.print(str.get(i) + " ");
        }
        System.out.println("");
    }
}
