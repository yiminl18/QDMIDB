package QMIDB;

import org.w3c.dom.Attr;
import simpledb.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.*;
import java.nio.file.Paths;
import java.util.HashMap;

public class test {


    public static void testPath()throws IOException{
        String catalog = "demo.db/catalog.txt";
        String queries = "demo.db/queries.txt";
        String catalogPath = Paths.get(catalog).toAbsolutePath().toString();
        String queriesPath = Paths.get(queries).toAbsolutePath().toString();
        System.out.println(catalogPath);
        System.out.println(queriesPath);
    }

    public static void testComplexQuery(){
        Type types2[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names2[] = new String[]{ "a", "b", "c" };

        TupleDesc td2 = new TupleDesc(types2, names2);

        Type types1[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE};
        String names1[] = new String[]{ "a", "d"};

        TupleDesc td1 = new TupleDesc(types1, names1);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema the tables.
        HeapFile table1 = new HeapFile(new File("simpledb/testdata/table_2_col.dat"), td1);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("simpledb/testdata/table_3_col.dat"), td2);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        Filter sf1 = new Filter(
                new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(3)), ss1);

        JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        Join j = new Join(p, sf1, ss2);

        // and run it
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
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
        System.out.println(t3);
    }

    public void testGlobal1(){
        HashTables.setId(100);
    }

    public void testGlobal2(){
        System.out.println(HashTables.getId());
    }

}
