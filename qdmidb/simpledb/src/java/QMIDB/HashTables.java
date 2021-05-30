package QMIDB;
import simpledb.Predicate;

import java.util.*;
/*
    * set of hashtable
 */
public class HashTables {
    private static Map<String, HashTable> hashTables = new HashMap<>();

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
}
