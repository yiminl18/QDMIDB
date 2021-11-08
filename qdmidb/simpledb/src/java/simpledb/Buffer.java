package simpledb;

import java.util.HashMap;

public class Buffer {
    private static HashMap<Integer, Tuple> buffers = new HashMap<>();
    private static Integer TID = 0;//latest tid of tuple to be inserted

    public static void addTuple(Tuple t){
        //return the TID of inserted tuple
        buffers.put(TID,t);
        t.setTID(TID);
        TID++;
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
}
