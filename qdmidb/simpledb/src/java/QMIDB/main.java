package QMIDB;

import java.io.IOException;
import java.util.List;

import simpledb.*;

public class main {
    public static void main(String args[]) throws IOException,Exception {
        int Qid = 10;
        String dataset = "WiFi";//CDC
        String method = "ImputeDB";//Quip, ImputeDB
        QueryProcessing QP = new QueryProcessing(Qid, dataset);

        test t = new test();
        t.runCDC(Qid, dataset, method);
    }
}
