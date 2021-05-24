package QMIDB;

import simpledb.*;
/*
    *This class implements the decision function which takes input of attribute or value and returns
    *A boolen variable to indicate whether or not this null value is clean now or delay.
 */
public class Decision {
    Predicate pred;

    public Decision(Predicate pred) {
        this.pred = pred;
    }

    public boolean Decide(){
        //to do
        return false;
    }
}
