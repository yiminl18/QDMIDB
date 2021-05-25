package QMIDB;

import java.io.IOException;
import simpledb.*;

public class main {
    public static void main(String args[]) throws IOException {
        test t = new test();
        HashTables hashTables = null;
        hashTables.setId(1);
        t.testGlobal1();
        t.testGlobal2();
    }
}
