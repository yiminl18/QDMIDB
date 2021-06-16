package QMIDB;
import simpledb.Field;
import simpledb.Tuple;

import java.util.*;
/*
    *this data structure maintains hash table for all joinable attributes
 */
public class HashTable {
    private static Attribute attribute;
    //map from attribute value to its corresponding tuple
    private static HashMap<Field, List<Tuple>> hashMap;
    private static HashMap<Field, Boolean> matchBits;//to indicate if an entry in hashMap is matched in join operator
    private static int meanValue;//maintained for Mean imputation method

    public HashTable(Attribute attributeName) {
        attribute = attributeName;
        hashMap = new HashMap<>();
        matchBits = new HashMap<>();
    }

    public HashTable(Attribute attributeName, HashMap<Field, List<Tuple>> table){
        attribute = attributeName;
        hashMap = table;
    }

    public void setMatchBit(Field field){
        if(!matchBits.containsKey(field)){
            matchBits.put(field, true);
        }
    }

    public boolean getMatchBit(Field field){
        return matchBits.containsKey(field);
    }

    public List<Tuple> getHashTable(Field field){
        return hashMap.get(field);
    }

    public int getMeanValue() {
        return meanValue;
    }

    public void setMeanValue(int meanvalue) {
        meanValue = meanvalue;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public HashMap<Field, List<Tuple>> getHashMap() {
        return hashMap;
    }

    public void setHashMaps(HashMap<Field, List<Tuple>> hashmap) {
        hashMap = hashmap;
    }

    public void clear(){hashMap.clear();}

    public void print(){
        //printing hashtable for debugging purpose
        for(Map.Entry<Field, List<Tuple>> iter : hashMap.entrySet()){
            System.out.println("Field: " + iter.getKey());
            List<Tuple> match = iter.getValue();
            for(int i=0;i<match.size();i++){
                System.out.println("Matching Tuple: " + match.get(i));
            }
        }
    }
}
