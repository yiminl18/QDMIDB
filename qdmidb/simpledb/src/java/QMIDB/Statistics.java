package QMIDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
    * this class is used to store global statistics used for decision function
    * 1. # of join test so far
    *
 */
public class Statistics {
    private static int numOfJoin;
    private static List<Attribute> attributes;
    private static List<String> attributesString; //String format of attributes, used for fast retrieving
    private static HashMap<String, List<String>> relationToAttributeName = new HashMap<>();//from relation to its attributes in predicate


    public Statistics(List<Attribute> attributeList){
        numOfJoin = 0;
        attributes = attributeList;
        attributesString = new ArrayList<>();
        for(int i=0;i<attributes.size();i++){
            attributesString.add(attributes.get(i).toString());
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
    }

    public static List<String> getAttributesInRelation(String relation){
        return relationToAttributeName.get(relation);
    }

    public static List<Attribute> getAttributes(){
        return attributes;
    }

    public static Attribute getAttribute(String attribute){
        int index = attributesString.indexOf(attribute);
        if(index == -1) return null;
        return attributes.get(index);
    }

    public static void addOneJoin(){
        numOfJoin ++;
    }

    public static void addJoins(int n){
        numOfJoin += n;
    }

}
