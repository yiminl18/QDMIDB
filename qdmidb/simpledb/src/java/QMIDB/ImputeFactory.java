package QMIDB;

import simpledb.*;
/*
    * this class is a set of imputation method
 */
public class ImputeFactory {
    private static String imputationMethod;

    public static void setImputationMethod(String ImputationMethod){
        imputationMethod = ImputationMethod;
    }

    public static Field Impute(Field attributeValue){
        Field imputedValue = new IntField(1);
        if(imputationMethod == "REGRESSION_TREE"){
            return RegressionTree(attributeValue);
        }else if(imputationMethod == "HOTDECK"){
            return HotDeck(attributeValue);
        }else if(imputationMethod == "MEAN"){
            return Mean(attributeValue);
        }else if(imputationMethod == "RANDOM"){
            return Random(attributeValue);
        }else{
            return imputedValue;
        }
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

    public static Field RegressionTree(Field attributeValue){
        //to do
        return attributeValue;
    }

    public static Field HotDeck(Field attributeValue){
        return attributeValue;
    }

    public static Field Mean(Field attributeValue){
        return attributeValue;
    }

    public static Field Random(Field attributeValue){
        return attributeValue;
    }
}
