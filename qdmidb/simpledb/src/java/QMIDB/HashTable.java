package QMIDB;
import simpledb.Field;
import simpledb.Tuple;

import java.util.*;
/*
    *this data structure maintains hash table for all joinable attributes
 */
public class HashTable {
    Attribute attribute;
    //map from attribute value to its corresponding tuple
    HashMap<Field, ArrayList<Tuple>> hashMap;
    int meanValue;//maintained for Mean imputation method

    public HashTable(Attribute attribute) {
        this.attribute = attribute;
        hashMap = new HashMap<>();
    }

    public int getMeanValue() {
        return meanValue;
    }

    public void setMeanValue(int meanValue) {
        this.meanValue = meanValue;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public HashMap<Field, ArrayList<Tuple>> getHashMaps() {
        return hashMap;
    }

    public void setHashMaps(HashMap<Field, ArrayList<Tuple>> hashmap) {
        this.hashMap = hashmap;
    }

    public void clear(){hashMap.clear();}
}
