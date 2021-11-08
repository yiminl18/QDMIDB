package QMIDB;
import simpledb.Buffer;
import simpledb.Field;
import simpledb.Tuple;

import java.util.*;
/*
    *this data structure maintains hash table for all joinable attributes
 */
public class HashTable {
    private String attribute;
    //map from attribute value to its corresponding tuple
    private HashMap<Field, List<Integer>> hashMap;//store TIDs of tuples
    private HashMap<Field, Boolean> matchBits;//to indicate if an entry in hashMap is matched in join operator
    private int meanValue;//maintained for Mean imputation method

    public HashTable(String attributeName) {
        attribute = attributeName;
        hashMap = new HashMap<>();
        matchBits = new HashMap<>();
    }

    public HashTable(String attributeName, HashMap<Field, List<Integer>> table){
        attribute = attributeName;
        hashMap = table;
        matchBits = new HashMap<>();
    }

    public void setMatchBit(Field field){
        if(!matchBits.containsKey(field)){
            matchBits.put(field, true);
        }
    }

    public boolean getMatchBit(Field field){
        return matchBits.containsKey(field);
    }

    public List<Integer> getHashTable(Field field){
        return hashMap.get(field);
    }

    public boolean hasKey(Field field){ return hashMap.containsKey(field); }

    public int getMeanValue() {
        return meanValue;
    }

    public void setMeanValue(int meanvalue) {
        meanValue = meanvalue;
    }

    public String getAttribute() {
        return attribute;
    }

    public HashMap<Field, List<Integer>> getHashMap() {
        return hashMap;
    }

    public void setHashMaps(HashMap<Field, List<Integer>> hashmap) {
        hashMap = hashmap;
    }

    public void clear(){hashMap.clear();}

    public void print(){
        //printing hashtable for debugging purpose
        for(Map.Entry<Field, List<Integer>> iter : hashMap.entrySet()){
            System.out.println("Field: " + iter.getKey());
            List<Integer> match = iter.getValue();
            for(int i=0;i<match.size();i++){
                System.out.println("Matching Tuple: " + Buffer.getTuple(match.get(i)));
            }
        }
    }
}
