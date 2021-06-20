package QMIDB;
import simpledb.Predicate;

import java.util.*;
/*
    * set of hashtable
 */
public class HashTables {
    private static HashMap<String, HashTable> hashTables = new HashMap<>();

    public static void addHashTable(String attribute, HashTable hashTable){
        if(!hashTables.containsKey(attribute)){
            hashTables.put(attribute, hashTable);
        }
    }

    public static void removeHashTable(String attribute){
        hashTables.remove(attribute);
    }

    public static HashTable getHashTable(String attribute){
        return hashTables.get(attribute);
    }

    public static boolean ifExistHashTable(String attribute){
        return hashTables.containsKey(attribute);
    }

    public static void print(){
        //print all hashTables for debugging
        for(Map.Entry<String, HashTable> iter : hashTables.entrySet()){
            System.out.println("Key: " + iter.getKey());
            iter.getValue().print();
        }
    }
}
