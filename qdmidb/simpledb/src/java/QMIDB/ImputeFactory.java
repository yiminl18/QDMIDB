package QMIDB;

import simpledb.*;
import java.util.*;
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
    public static final int MISSING_INTEGER = Integer.MIN_VALUE;

    //first key is tid, starting from 0 for each relation
    //second key is field index to its imputed value
    private static HashMap<Integer, HashMap<Integer, Integer>> imputedUser = new HashMap<>();
    private static HashMap<Integer, HashMap<Integer, Integer>> imputedSpace = new HashMap<>();
    private static HashMap<Integer, HashMap<Integer, Integer>> imputedWiFi = new HashMap<>();

    private static int imputeValue;

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
        //System.out.println("print in Impute: "+attribute.getAttribute());
        Statistics.getAttribute(attribute.getAttribute()).incrementNumOfImputed();
        tuple.addImputedField(attribute.getAttribute());//ihe: check if changes
        if(imputationMethod == "REGRESSION_TREE"){
            imputedValue = RegressionTree(attribute, tuple);
        }else if(imputationMethod == "Manual"){
            imputedValue = ImputeWiFi(attribute, tuple);//replace manual
        }else if(imputationMethod == "HOTDECK"){
            imputedValue = HotDeck(attribute, tuple);
        }else if(imputationMethod == "MEAN"){
            imputedValue = Mean(attribute, tuple);
        }
        //update buffered columns
        int tid = tuple.findTID(attribute.getAttribute());//raw tuple
        int ImputedTID = Buffer.getTuple(tid).getImputedTID();//get imputedTID
        //System.out.println(tuple + " " + tuple.getTID() + " " + tid + " " + ImputedTID);
        Buffer.updateBufferCDCValue(attribute.getAttribute(), imputeValue, ImputedTID);
        return imputedValue;
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
        String relation = new Attribute(attribute).getRelation();
        //System.out.println(t.getTupleDesc());
        //System.out.println("original t: " + t);
        //System.out.println(attributeName.getAttribute() + " " + rawTID + " " + Buffer.getTuple(rawTID));
        Tuple rawTuple = Buffer.getTuple(rawTID);
        int imputedTID = rawTuple.getImputedTID();
        int fieldIndex = rawTuple.getTupleDesc().fieldNameToIndex(attribute);

        //System.out.println("inside imputeWiFi: " + rawTID + " " + fieldIndex + " " + relation);
        if(relation.equals("users")){
            if(!imputedUser.containsKey(imputedTID)){
                System.out.println("imputed values users file is wrong!");
            }
            if(!imputedUser.get(imputedTID).containsKey(fieldIndex)){
                System.out.println("imputed values users file is wrong!");
            }
            fieldValue = imputedUser.get(imputedTID).get(fieldIndex);
        }else if(relation.equals("wifi")){
            if(!imputedWiFi.containsKey(imputedTID)){
                System.out.println("imputed values wifi file is wrong!");
            }
            if(!imputedWiFi.get(imputedTID).containsKey(fieldIndex)){
                System.out.println("imputed values wifi file is wrong!");
            }
            fieldValue = imputedWiFi.get(imputedTID).get(fieldIndex);
        }else if(relation.equals("space")){
            if(!imputedSpace.containsKey(imputedTID)){
                System.out.println("imputed values space file is wrong!");
            }
            if(!imputedSpace.get(imputedTID).containsKey(fieldIndex)){
                //System.out.println(t);
                System.out.println(imputedTID + " " + fieldIndex + " imputed values space file is wrong!");
            }
            fieldValue = imputedSpace.get(imputedTID).get(fieldIndex);
        }
        imputeValue = fieldValue;
        return new IntField(fieldValue);
    }

    public static Field RegressionTree(Attribute attribute, Tuple tuple){
        //to do
        Field attributeValue = new IntField(0);
        imputeValue = 0;
        return attributeValue;
    }

    public static Field HotDeck(Attribute attribute, Tuple tuple){
        Field attributeValue;
        List<Integer> values = Buffer.getBufferValues(attribute.getAttribute());
        Random rand = new Random();
        int nextIndex;
        int value;
        while(true){
            nextIndex = rand.nextInt(values.size());
            value = values.get(nextIndex);
            if(value != MISSING_INTEGER){
                break;
            }
        }
        imputeValue = value;
        attributeValue = new IntField(value);
        return attributeValue;
    }

    public static Field Mean(Attribute attribute, Tuple tuple){
        Field attributeValue;
        List<Integer> values = Buffer.getBufferValues(attribute.getAttribute());
        int mean, sum =0;
        for(int i=0;i<values.size();i++){
            if(values.get(i) != MISSING_INTEGER){
                sum += values.get(i);
            }
        }
        mean = sum/values.size();
        imputeValue = mean;
        attributeValue = new IntField(mean);
        return attributeValue;
    }
}
