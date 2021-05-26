package QMIDB;

import simpledb.DbFileIterator;
import simpledb.DbIterator;
import simpledb.TransactionId;

/*
    This class is used to ingest and store query plan.
    The first version is to manually transform the query plan from outside to to be in the code
 */

public class QueryPlan {
    private static String dataSet;//{R1,R2,S1,S2}
    private static int queryID;
    private static DbIterator iter = null;

    public QueryPlan(String dataset, int queryid){
        dataSet = dataset;
        queryID = queryid;
    }

    public static DbIterator getQueryPlan(){
        switch (dataSet){
            case "R1":
                return getQueryR1();
            case "R2":
                return getQueryR2();
            case "S1":
                return getQueryS1();
            case "S2":
                return getQueryS2();
        }
        return iter;
    }

    private static DbIterator getQueryR1(){
        switch (queryID){
            case 1:
                break;
        }
        return iter;
    }

    private static DbIterator getQueryR2(){
        switch (queryID){
            case 1:
                break;
        }
        return iter;
    }

    private static DbIterator getQueryS1(){
        switch (queryID){
            case 1:
                break;
        }
        return iter;
    }

    private static DbIterator getQueryS2(){
        switch (queryID){
            case 1:
                break;
        }
        return iter;
    }
}
