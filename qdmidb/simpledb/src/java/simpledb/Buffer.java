package simpledb;

import QMIDB.Attribute;
import QMIDB.Schema;
import QMIDB.Statistics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Buffer {
    private static HashMap<Integer, Tuple> buffers = new HashMap<>();
    public static final int MISSING_INTEGER = Integer.MIN_VALUE;
    private static Integer TID = 0;//latest tid of tuple to be inserted
    private static HashMap<String, List<Integer>> bufferedValues = new HashMap<>();//buffered column values for imputation if needed
    private static HashMap<String, Integer> ImputedTIDs = new HashMap<>();//from relation to its current ImputedTID

    public static void initBuffer(){
        //set up ImputedTIDs
        for(int i=0;i<Schema.getRelations().size();i++){
            ImputedTIDs.put(Schema.getRelations().get(i),0);
        }
    }

    public static void addTuple(Tuple t){
        //return the TID of inserted tuple
        buffers.put(TID,t);
        t.setTID(TID);
        TID++;
    }

    public static void setImputedTID(Tuple t){//set only in scan operator
        String relation = t.getRelation();
        if(ImputedTIDs.containsKey(relation)){
            t.setImputedTID(ImputedTIDs.get(relation));
            ImputedTIDs.put(relation, ImputedTIDs.get(relation)+1);
        }
    }

    public static void updateTuple(Tuple t){
        //used for right join attr and filter because they took raw tuple as input
        int tid = t.getTID();
        if(buffers.containsKey(tid)){
            buffers.put(tid,t);
        }
    }

    public static void updateCompoundTuple(Tuple t, String attr, Field value){
        //used to impute tuples which might take several small tuples as input
        if(!t.isMergeBit()) return ;
        int tid = t.findTID(attr);//raw tuple
        if(tid != -1){
            updateTupleByValue(tid, attr, value);
        }
    }

    public static void updateTupleByTID(Tuple t, int tid){
        if(buffers.containsKey(tid)){
            buffers.put(tid,t);
        }
    }

    public static void updateTupleByValue(int tid, String attr, Field value){
        Tuple t = getTuple(tid);
        int fieldIndex = t.getTupleDesc().fieldNameToIndex(attr);
        t.setField(fieldIndex, value);
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

    public static List<Integer> getBufferValues(String attribute){
        return bufferedValues.get(attribute);
    }

    public static void updateBufferValue(String attribute, int value, int tid){
//        System.out.println("Print in updateBufferedValue:" + bufferedValues.get(attribute).get(tid));
//        System.out.println("----");
        if(bufferedValues.get(attribute).get(tid) == MISSING_INTEGER){
            bufferedValues.get(attribute).set(tid, value);
        }
        else{
            System.out.println("Update entry incorrect!");
        }
    }

    public static void bufferACSValues(List<Attribute> schema){
        for(int i=0;i<schema.size();i++){
            List<Integer> list = new ArrayList<>();
            bufferedValues.put(schema.get(i).getAttribute(), list);
        }
        int ACSColumnSize = 5;
        String RootPath = "../QDMIDB/QDMIDB/qdmidb/simpledb/acsdataset/";
        for(int i=0;i<ACSColumnSize;i++){
            String tableName = "t" + i;
            String path = Paths.get(RootPath + tableName + ".txt").toAbsolutePath().toString();
            bufferRelation(schema, path, tableName);
        }
    }

    public static void bufferCDCValues(List<Attribute> schema){
        for(int i=0;i<schema.size();i++){
            List<Integer> list = new ArrayList<>();
            bufferedValues.put(schema.get(i).getAttribute(), list);
        }
        String demoFile = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/cdcdataset/demoDirty.txt";
        String examsFile = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/cdcdataset/examsDirty.txt";
        String labsFile = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/cdcdataset/labsDirty.txt";
        bufferRelation(schema, demoFile, "demo");
        bufferRelation(schema, examsFile, "exams");
        bufferRelation(schema, labsFile, "labs");
    }

    public static void bufferWiFiValues(List<Attribute> schema){
        for(int i=0;i<schema.size();i++){
            List<Integer> list = new ArrayList<>();
            bufferedValues.put(schema.get(i).getAttribute(), list);
        }
        String wifiFile = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/wifi.txt";
        String usersFile = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/users.txt";
        String occupancyFile = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/occupancy.txt";
        bufferRelation(schema, wifiFile, "wifi");
        bufferRelation(schema, usersFile, "users");
        bufferRelation(schema, occupancyFile, "occupancy");
    }

    public static void bufferRelation(List<Attribute> schema, String path, String relation){//buffer for one relation
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

    public static int getQuerySelectivity(Attribute attr, double selectivity, Predicate.Op op){
        int operand = -1;
        List<Integer> values = bufferedValues.get(attr.getAttribute());
        //sort in aesc order
        int index = (int) (Double.valueOf(values.size())*selectivity);
        if(op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)){
            operand = values.get(values.size() - index);
        }else if(op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)){
            operand = values.get(index);
        }else{
            System.out.println("Invalid Op for selectivity!");
        }
        return operand;
    }
}
