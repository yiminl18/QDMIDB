package QMIDB;
import java.util.*;
/*
    * set of hashtable
 */
public class HashTables {
    List<HashTable> hashTables = new ArrayList<>();

    public void addHashTable(HashTable hashTable){
        hashTables.add(hashTable);
    }

    public void removeHashTable(HashTable hashTable){
        int id = findHashTable(hashTable);
        if(id != -1){
            this.hashTables.remove(id);
        }
    }

    public int findHashTable(HashTable hashTable){//return the id of list in hashTables
        return this.hashTables.indexOf(hashTable);
    }
}
