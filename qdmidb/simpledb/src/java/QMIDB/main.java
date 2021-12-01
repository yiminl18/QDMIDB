package QMIDB;

import java.io.IOException;
import java.util.List;

import simpledb.*;

public class main {
    public static void main(String args[]) throws IOException,Exception {
        int Qid = 8;
        QueryProcessing QP = new QueryProcessing(Qid);

        test t = new test();
        t.runCDC(Qid);
    }
}
