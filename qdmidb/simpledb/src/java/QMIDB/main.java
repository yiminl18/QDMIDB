package QMIDB;

import java.io.IOException;
import java.util.List;

import simpledb.*;

public class main {
    public static void main(String args[]) throws IOException,Exception {
        QueryProcessing QP = new QueryProcessing();
        test t = new test();
        //t.testComplexQuery();
        t.testSubDesc();
    }

}
