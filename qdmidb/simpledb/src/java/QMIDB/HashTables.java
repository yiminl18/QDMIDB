package QMIDB;
import java.util.*;
/*
    * set of hashtable
 */
public class HashTables {
    private static List<HashTable> hashTables = new ArrayList<>();
    private static int id;

    public static int getId() {
        return id;
    }

    public static void setId(int ids) {
        id = ids;
    }

    public static void addHashTable(HashTable hashTable){
        hashTables.add(hashTable);
    }

    public static void removeHashTable(HashTable hashTable){
        int id = findHashTable(hashTable);
        if(id != -1){
            hashTables.remove(id);
        }
    }

    public static int findHashTable(HashTable hashTable){//return the id of list in hashTables
        return hashTables.indexOf(hashTable);
    }
}
