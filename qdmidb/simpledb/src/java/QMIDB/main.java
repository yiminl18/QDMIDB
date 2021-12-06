package QMIDB;

import java.io.IOException;
import java.util.List;

import simpledb.*;

public class main {
    public static void main(String args[]) throws IOException,Exception {
        int Qid = 1;
        String dataset = "ACS";//CDC, ACS
        String method = "ImputeDB";//Quip, ImputeDB
        QueryProcessing QP = new QueryProcessing(Qid, dataset);

        test t = new test();
        //t.runCDC(Qid, dataset, method);
    }
}
