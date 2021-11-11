package QMIDB;

import java.io.IOException;
import java.util.List;

import simpledb.*;

public class main {
    public static void main(String args[]) throws IOException,Exception {
        QueryProcessing QP = new QueryProcessing();
        //PredicateSet.print();

        test t = new test();
        t.testComplexQuery();

        //System.out.println(ImputeFactory.getImputationTimes());

        //DataPreparation dp = new DataPreparation();
        //dp.generateSchema();
    }

}
