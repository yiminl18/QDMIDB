package simpledb;

import QMIDB.Attribute;
import QMIDB.Schema;
import QMIDB.Statistics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Buffer {
    private static HashMap<Integer, Tuple> buffers = new HashMap<>();
    public static final int MISSING_INTEGER = Integer.MIN_VALUE;
    private static Integer TID = 0;//latest tid of tuple to be inserted
    private static int usersTID=0, spaceTID=0, wifiTID=0;
    private static HashMap<String, List<Integer>> bufferedValues = new HashMap<>();//buffered column values for imputation if needed


    public static void addTuple(Tuple t){
        //return the TID of inserted tuple
        buffers.put(TID,t);
        t.setTID(TID);
        TID++;
    }

    public static void setImputedTID(Tuple t){//set only in scan operator
        String relation = t.getRelation();
        if(relation.equalsIgnoreCase("users")){
            t.setImputedTID(usersTID);
            usersTID++;
        }
        else if(relation.equalsIgnoreCase("space")){
            t.setImputedTID(spaceTID);
            spaceTID++;
        }
        else if(relation.equalsIgnoreCase("wifi")){
            t.setImputedTID(wifiTID);
            wifiTID++;
        }
    }

    public static void updateTuple(Tuple t){
        int tid = t.getTID();
        if(buffers.containsKey(tid)){
            buffers.put(tid,t);
        }
    }

    public static void updateTupleByTID(Tuple t, int tid){
        if(buffers.containsKey(tid)){
            buffers.put(tid,t);
        }
    }

    public static void removeTuple(Tuple t){
        Statistics.addNumOfRemovedTuples(1);
        int tid = t.getTID();
        if(buffers.containsKey(tid)){
            buffers.remove(tid);
        }
    }

    public static Tuple getTuple(int tid){
        if(buffers.containsKey(tid)){
            return buffers.get(tid);
        }
        else{
            return null;
        }
    }

    public static int getTID(){
        return TID;
    }

    public static List<Integer> getBufferCDCValues(String attribute){
        return bufferedValues.get(attribute);
    }

    public static void updateBufferCDCValue(String attribute, int value, int tid){
        if(bufferedValues.get(attribute).get(tid) == MISSING_INTEGER){
            bufferedValues.get(attribute).set(tid, value);
        }
        else{
            System.out.println("Update entry incorrect!");
        }
    }

    public static void bufferCDCValues(List<Attribute> schema){
        for(int i=0;i<schema.size();i++){
            List<Integer> list = new ArrayList<>();
            bufferedValues.put(schema.get(i).getAttribute(), list);
        }
        String demoFile = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/cdcdataset/demoDirty.txt";
        String examsFile = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/cdcdataset/examsDirty.txt";
        String labsFile = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/cdcdataset/labsDirty.txt";
        bufferCDCRelation(schema, demoFile, "demo");
        bufferCDCRelation(schema, examsFile, "exams");
        bufferCDCRelation(schema, labsFile, "labs");
        //test
        for(int i=0;i<schema.size();i++){
            String attr = schema.get(i).getAttribute();
            System.out.println(attr);
            for(int j=0;j<20;j++){
                System.out.println(bufferedValues.get(attr).get(j));
            }
        }
    }

    public static void bufferCDCRelation(List<Attribute> schema, String path, String relation){//buffer for one relation
        int start = 0;
        for(int i=0;i<schema.size();i++){
            if(schema.get(i).getRelation().equals(relation)){
                start = i;
                break;
            }
        }
        try{
            BufferedReader in = new BufferedReader(new FileReader(path));
            String row;
            while ((row = in.readLine()) != null) {
                String data[] = row.split(",");
                for(int i=0;i<data.length;i++){
                    String attribute = schema.get(start+i).getAttribute();
                    if(!schema.get(start+i).getRelation().equals(relation)){
                        System.out.println("Wrong schema for " + relation + ", Stop and check!");
                    }
                    int value = Integer.valueOf(data[i]);
                    bufferedValues.get(attribute).add(value);
                }
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
