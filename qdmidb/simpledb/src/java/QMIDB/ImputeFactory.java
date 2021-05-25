package QMIDB;

import simpledb.*;
/*
    * this class is a set of imputation method
 */
public class ImputeFactory {
    private String imputationMethod;

    public void setImputationMethod(String imputationMethod){
        this.imputationMethod = imputationMethod;
    }

    public Field Impute(Field attributeValue){
        Field imputedValue = new IntField(0);
        if(this.imputationMethod == "REGRESSION_TREE"){
            return RegressionTree(attributeValue);
        }else if(this.imputationMethod == "HOTDECK"){
            return HotDeck(attributeValue);
        }else if(this.imputationMethod == "MEAN"){
            return Mean(attributeValue);
        }else if(this.imputationMethod == "RANDOM"){
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

    public Field RegressionTree(Field attributeValue){
        //to do
        return attributeValue;
    }

    public Field HotDeck(Field attributeValue){
        return attributeValue;
    }

    public Field Mean(Field attributeValue){
        return attributeValue;
    }

    public Field Random(Field attributeValue){
        return attributeValue;
    }
}
