package QMIDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
    Store all predicates
    A list of predicateUnit
 */
public class PredicateSet {
    private static List<PredicateUnit> predicateSet;
    private static HashMap<String, List<PredicateUnit>> predicateMap;

    public static void initPredicateSet(List<PredicateUnit> predicateSET){
        predicateSet = predicateSET;
        //build hashMap from attribute to its predicate
        //one attribute could be involved in multiple predicates, but each predicates will not contain same attributes
        predicateMap = new HashMap<>();
        for(int i=0;i<predicateSet.size();i++){
            switch (predicateSet.get(i).getType()){
                case "Join":
                    String left = predicateSet.get(i).getLeft().getAttribute();
                    String right = predicateSet.get(i).getRight().getAttribute();
                    if(!predicateMap.containsKey(left)){
                        predicateMap.put(left, new ArrayList<PredicateUnit>());
                    }
                    predicateMap.get(left).add(predicateSet.get(i));
                    break;
                case "Filter":
                    String attribute = predicateSet.get(i).getFilterAttribute().getAttribute();
                    if(!predicateMap.containsKey(attribute)){
                        predicateMap.put(attribute, new ArrayList<PredicateUnit>());
                    }
                    predicateMap.get(attribute).add(predicateSet.get(i));
                    break;
                default:
                    break;
            }
        }
    }

    public static boolean isExist(String attribute){
        return predicateMap.containsKey(attribute);
    }

    public static List<PredicateUnit> getPredicateUnits(String attribute){
        return predicateMap.get(attribute);
    }

    public static void print(){
        for(int i=0;i<predicateSet.size();i++){
            predicateSet.get(i).print();
        }
    }
}
