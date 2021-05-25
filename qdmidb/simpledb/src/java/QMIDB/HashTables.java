package QMIDB;
import simpledb.Predicate;

import java.util.*;
/*
    * set of hashtable
 */
public class HashTables {
    private static List<HashTable> hashTables = new ArrayList<>();

    public static void addHashTable(HashTable hashTable){
        int id = findHashTable(hashTable);
        if(id == -1){
            hashTables.add(hashTable);
        }
    }

    public static void removeHashTable(HashTable hashTable){
        int id = findHashTable(hashTable);
        if(id != -1){
            hashTables.get(id).clear();//release memory space for this hash table
            hashTables.remove(id);
        }
    }

    public static int findHashTable(HashTable hashTable){//return the id of list in hashTables
        return hashTables.indexOf(hashTable);
    }

    public static HashTable getHashTable(Attribute attribute){
        for(int i=0;i<hashTables.size();i++){
            if(hashTables.get(i).attribute.getHashCode() == attribute.getHashCode()){
                return hashTables.get(i);
            }
        }
        return null;
    }
}
