package simpledb;

import java.util.HashMap;

public class Buffer {
    private static HashMap<Integer, Tuple> buffers = new HashMap<>();
    private static Integer TID = 0;//latest tid of tuple to be inserted

    public static int addTuple(Tuple t){
        //return the TID of inserted tuple
        buffers.put(TID,t);
        TID++;
        return TID-1;
    }

    public static void removeTuple(int tid){
        buffers.remove(tid);
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
