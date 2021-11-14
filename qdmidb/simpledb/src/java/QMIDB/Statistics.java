package QMIDB;

import simpledb.TupleDesc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
    * this class is used to store global statistics used for decision function
    * 1. # of join test so far
    *
 */
public class Statistics {
    private static double numOfJoin;
    private static double startTime, duration, timeOneJoin;
    private static List<Attribute> attributes;//attributes in predicate set
    private static List<String> attributesString; //String format of attributes, used for fast retrieving
    private static HashMap<String, List<String>> relationToAttributeName = new HashMap<>();//from relation to its attributes in predicate
    private static HashMap<String, Boolean> deadAttributes = new HashMap<>();//to indicate if an attribute is dead

    public static void initStatistics(){
        numOfJoin = 0;
        attributes = RelationshipGraph.getNodes();
        attributesString = new ArrayList<>();
        for(int i=0;i<attributes.size();i++){
            attributesString.add(attributes.get(i).getAttribute());
        }
        for(int i=0;i<attributes.size();i++){
            String relation = attributes.get(i).getRelation();
            String attributeName = attributes.get(i).getAttribute();
            if(relationToAttributeName.containsKey(relation)){
                relationToAttributeName.get(relation).add(attributeName);
            }
            else{
                List<String> aNames = new ArrayList<>();
                aNames.add(attributeName);
                relationToAttributeName.put(relation, aNames);
            }
        }
        //compute deadAttributes
        for(int i=0;i<attributesString.size();i++){
            if(RelationshipGraph.hasNonJoinNeighbor(attributesString.get(i))){
                deadAttributes.put(attributesString.get(i), true);
            }
        }
    }

    public static boolean isDead(String attribute){
        return deadAttributes.containsKey(attribute);
    }

    public static void printDeadAttribute(){
        System.out.println("dead attributes");
        for(Map.Entry<String, Boolean> entry : deadAttributes.entrySet()){
            System.out.print(entry.getKey() + " ");
        }
        System.out.println("");
    }

    public static List<Integer> computePAfield(TupleDesc td){//update in each predicate to save overhead
        List<Integer> PAfield = new ArrayList<>();
        for(int i=0;i<attributesString.size();i++){
            int index = td.fieldNameToIndex(attributesString.get(i));
            //System.out.println(attributesString.get(i) + " " + index);
            if(index != -1){
                PAfield.add(index);
            }
        }
        return PAfield;
    }

    public static List<String> getAttributesInRelation(String relation){
        return relationToAttributeName.get(relation);
    }

    public static boolean isPredicateAttribute(String attribute){
        if(attributesString.indexOf(attribute) == -1){
            return false;
        }
        return true;
    }

    public static List<Attribute> getAttributes(){
        return attributes;
    }

    public static List<String> getAttributesString() {
        return attributesString;
    }

    public static Attribute getAttribute(String attribute){
        int index = attributesString.indexOf(attribute);
        if(index == -1) return null;
        return attributes.get(index);
    }

    public static void addJoins(int n){
        numOfJoin += n;
    }

    public static void setStartTime(double n) {startTime = n;}

    public static double getDuration() {
        duration = System.currentTimeMillis() - startTime;
        return duration;
    }

    public static double getTimeOneJoin(){
        if(numOfJoin == 0){
            return -1;
        }
        timeOneJoin = getDuration()/numOfJoin;
        return timeOneJoin;
    }

    public static void print(){
        //System.out.println("number of Joins so far:" + numOfJoin);
        for(int i=0;i<attributes.size();i++){
            attributes.get(i).print();
        }
    }

}
