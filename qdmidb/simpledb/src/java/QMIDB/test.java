package QMIDB;

import org.w3c.dom.Attr;
import simpledb.*;

import javax.annotation.processing.SupportedSourceVersion;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
        HeapFile table1 = new HeapFile(new File("simpledb/testdata/R.dat"), td1);
        Database.getCatalog().addTable(table1, "R");

        //table1.getTupleDesc().print();

        HeapFile table2 = new HeapFile(new File("simpledb/testdata/S.dat"), td2);
        Database.getCatalog().addTable(table2, "S");

        HeapFile table3 = new HeapFile(new File("simpledb/testdata/T.dat"), td3);
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
        SmartJoin sj = new SmartJoin(p1,ss1,sf1);


        //test smartProject
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("R.a"));
        attributes.add(new Attribute("R.b"));
        //attributes.add(new Attribute("S.b"));
        //attributes.add(new Attribute("S.c"));
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        SmartProject sp = new SmartProject(attributes,types, sj);


        //print Hashtables
        //HashTables.print();
        //System.out.println(RelationshipGraph.getNumOfActiveEdge());


        /*JoinPredicate p = new JoinPredicate("t1.a", Predicate.Op.EQUALS, "t2.a");
        JoinPredicate p1 = new JoinPredicate("t2.c", Predicate.Op.EQUALS, "t3.c");
        Join j = new Join(p, sf1, ss2);
        Join j1 = new Join(p1, j, ss3);*/


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
}
