package QMIDB;

import java.io.IOException;
import java.util.List;

import simpledb.*;

public class main {
    public static void main(String args[]) throws IOException,Exception {
        QueryProcessing QP = new QueryProcessing();

        //System.out.println(Type.MISSING_INTEGER + " "  + Type.NULL_INTEGER);
        //PredicateSet.print();
        //Schema.print();
        test t = new test();
        t.testComplexQuery();
        //System.out.println(RelationshipGraph.getLeftJoinAttribute());
        //t.testSubDesc();
    }

}
