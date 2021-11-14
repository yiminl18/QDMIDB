package QMIDB;

import simpledb.*;
import weka.core.Instance;
import weka.core.Instances;
import weka.classifiers.Classifier;
import weka.classifiers.trees.REPTree;
/*
    * this class is a set of imputation method
 */
public class ImputeFactory {
    private static String imputationMethod;
    private static int imputationTimes;

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
            return Manual(attribute, tuple);
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
