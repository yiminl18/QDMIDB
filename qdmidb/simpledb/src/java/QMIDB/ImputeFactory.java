package QMIDB;

import simpledb.*;
import weka.core.Instance;
import weka.core.Instances;
import weka.classifiers.Classifier;
import weka.classifiers.trees.REPTree;

import java.util.HashMap;

/*
    * this class is a set of imputation method
 */
public class ImputeFactory {
    private static String imputationMethod;
    private static int imputationTimes;

    //first key is tid, starting from 0 for each relation
    //second key is field index to its imputed value
    private static HashMap<Integer, HashMap<Integer, Integer>> imputedUser = new HashMap<>();
    private static HashMap<Integer, HashMap<Integer, Integer>> imputedSpace = new HashMap<>();
    private static HashMap<Integer, HashMap<Integer, Integer>> imputedWiFi = new HashMap<>();

    public static void loadImputations(String relation, HashMap<Integer, HashMap<Integer, Integer>> imputedValues){
        if(relation.equals("users")){
            imputedUser = imputedValues;
        }else if(relation.equals("space")){
            imputedSpace = imputedValues;
        }else if(relation.equals("wifi")){
            imputedWiFi = imputedValues;
        }
    }

    public static void setImputationMethod(String ImputationMethod){
        imputationMethod = ImputationMethod;
        imputationTimes = 0;
    }

    public static Field Impute(Attribute attribute, Tuple tuple){
        Field imputedValue = new IntField(1);
        imputationTimes ++;
        Statistics.getAttribute(attribute.getAttribute()).incrementNumOfImputed();
        tuple.addImputedField(attribute.getAttribute());//ihe: check if changes
        if(imputationMethod == "REGRESSION_TREE"){
            return RegressionTree(attribute, tuple);
        }else if(imputationMethod == "Manual"){
            return ImputeWiFi(attribute, tuple);//replace manual
        }else if(imputationMethod == "HOTDECK"){
            return HotDeck(attribute, tuple);
        }else if(imputationMethod == "MEAN"){
            return Mean(attribute, tuple);
        }else if(imputationMethod == "RANDOM"){
            return Random(attribute, tuple);
        }else{
            return imputedValue;
        }
    }

    public static int getImputationTimes(){
        return imputationTimes;
    }

    public static double getEstimateTime(){
        //this function estimate the time of each imputation method
        double time = 10;//ms
        if(imputationMethod == "REGRESSION_TREE"){

        }else if(imputationMethod == "HOTDECK"){

        }else if(imputationMethod == "MEAN"){

        }else if(imputationMethod == "RANDOM"){

        }else{

        }
        return time;
    }

    public static Field Manual(Attribute attribute, Tuple tuple){
        Field attributeValue = new IntField(0);
        switch(attribute.getAttribute()){
            case "wifi.room":
                attributeValue = new IntField(1);
                break;
            case "R.b":
                attributeValue = new IntField(6);//5
                break;
            case "S.b":
                attributeValue = new IntField(6);//6
                break;
            case "S.c":
                attributeValue = new IntField(1);
                break;
            case "T.a":
                attributeValue = new IntField(1);//3
                break;
        }
        return attributeValue;
    }

    public static Field ImputeWiFi(Attribute attributeName, Tuple t){
        String attribute = attributeName.getAttribute();
        int fieldValue = 0;
        int rawTID = t.findTID(attribute);
        int fieldIndex = t.getTupleDesc().fieldNameToIndex(attribute);
        String relation = new Attribute(attribute).getRelation();
        if(relation.equals("users")){
            if(!imputedUser.containsKey(rawTID)){
                return null;
            }
            if(!imputedUser.get(rawTID).containsKey(fieldIndex)){
                return null;
            }
            fieldValue = imputedUser.get(rawTID).get(fieldIndex);
        }else if(relation.equals("wifi")){
            if(!imputedWiFi.containsKey(rawTID)){
                return null;
            }
            if(!imputedWiFi.get(rawTID).containsKey(fieldIndex)){
                return null;
            }
            fieldValue = imputedWiFi.get(rawTID).get(fieldIndex);
        }else if(relation.equals("space")){
            if(!imputedSpace.containsKey(rawTID)){
                return null;
            }
            if(!imputedSpace.get(rawTID).containsKey(fieldIndex)){
                return null;
            }
            fieldValue = imputedSpace.get(rawTID).get(fieldIndex);
        }
        return new IntField(fieldValue);
    }

    public static Field RegressionTree(Attribute attribute, Tuple tuple){
        //to do
        Field attributeValue = new IntField(0);
        return attributeValue;
    }

    public static Field HotDeck(Attribute attribute, Tuple tuple){
        Field attributeValue = new IntField(0);
        return attributeValue;
    }

    public static Field Mean(Attribute attribute, Tuple tuple){
        Field attributeValue = new IntField(0);
        return attributeValue;
    }

    public static Field Random(Attribute attribute, Tuple tuple){
        Field attributeValue = new IntField(0);
        return attributeValue;
    }
}
