package QMIDB;

import java.util.*;
import simpledb.*;
/*
    this class is sued to store metadata for all queries
 */
public class Queries {
    private static List<PredicateSet> predicateSetList = new ArrayList<>();

    public static void setPredicateSetList(List<PredicateSet> predicates){
        predicateSetList = predicates;
    }

    public static PredicateSet getPredicatesByQuery(int queryID){
        for(int i=0;i<predicateSetList.size();i++){
            if(predicateSetList.get(i).getQueryID().equals(queryID)){
                return predicateSetList.get(i);
            }
        }
        return null;
    }
}
