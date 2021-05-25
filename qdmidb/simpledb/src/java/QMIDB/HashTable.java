package QMIDB;
import java.util.*;
/*
    *this data structure maintains hash table for all joinable attributes
 */
public class HashTable {
    Attribute attribute;
    HashMap<Integer, Integer> hashMaps;
    int meanValue;//maintained for

    public int getMeanValue() {
        return meanValue;
    }

    public void setMeanValue(int meanValue) {
        this.meanValue = meanValue;
    }


    public HashTable() {
        hashMaps = new HashMap<>();
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public void setAttribute(Attribute attribute) {
        this.attribute = attribute;
    }

    public HashMap<Integer, Integer> getHashMaps() {
        return hashMaps;
    }

    public void setHashMaps(HashMap<Integer, Integer> hashMaps) {
        this.hashMaps = hashMaps;
    }
}
