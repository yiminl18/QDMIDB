package QMIDB;
/*
    * this class is used to store global statistics used for decision function
    * 1. # of join test so far
    *
 */
public class Statistics {
    private static int numOfJoin;


    public Statistics(){
        numOfJoin = 0;
    }

    public static void addOneJoin(){
        numOfJoin ++;
    }

    public static void addJoins(int n){
        numOfJoin += n;
    }

}
