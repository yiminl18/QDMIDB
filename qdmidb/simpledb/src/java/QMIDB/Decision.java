package QMIDB;

import javafx.util.Pair;
import simpledb.*;
/*
    *This class implements the decision function which takes input of attribute or value and returns
    *A boolen variable to indicate whether or not this null value is clean now or delay.
 */
public class Decision {
    private Predicate pred;
    private boolean isJoin = false;
    private JoinPredicate joinpred;
    private Pair<Boolean,Boolean> JoinDecision;


    public Decision(Predicate pred) {
        this.pred = pred;
    }

    public Decision(JoinPredicate joinPred) {this.joinpred = joinPred; isJoin = true;}

    public boolean Decide(String attribute){//true -> clean now
        Attribute attr = Statistics.getAttribute(attribute);
        //return attr.getDecision();
        return true;
    }
}
