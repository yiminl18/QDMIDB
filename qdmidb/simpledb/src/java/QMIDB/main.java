package QMIDB;

import Experiment.*;
import java.io.IOException;
import java.util.List;

import simpledb.*;

public class main {
    public static void main(String args[]) throws IOException,Exception {
        //QueryProcessing QP = new QueryProcessing();

        //System.out.println(Type.MISSING_INTEGER + " "  + Type.NULL_INTEGER);
        //PredicateSet.print();
        //Schema.print();
        //test t = new test();
        //t.testArray("77,4,,0,2,73569,,1,10,",',');
        //System.out.println(RelationshipGraph.getLeftJoinAttribute());
        //t.testSubDesc();

        DataPreparation dp = new DataPreparation();
        dp.generateSchema();
    }

}
