package QMIDB;

import simpledb.*;
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
        Statistics.getAttribute(attribute.toString()).incrementNumOfImputed();
        if(imputationMethod == "REGRESSION_TREE"){
            return RegressionTree(attribute, tuple);
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

    public Integer getEstimateTime(){
        //this function estimate the time of each imputation method
        Integer time = 0;
        if(this.imputationMethod == "REGRESSION_TREE"){

        }else if(this.imputationMethod == "HOTDECK"){

        }else if(this.imputationMethod == "MEAN"){

        }else if(this.imputationMethod == "RANDOM"){

        }else{

        }
        return time;
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
