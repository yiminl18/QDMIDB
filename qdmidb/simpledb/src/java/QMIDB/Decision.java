package QMIDB;

import javafx.util.Pair;
import simpledb.*;
/*
    *This class implements the decision function which takes input of attribute or value and returns
    *A boolen variable to indicate whether or not this null value is clean now or delay.
 */
public class Decision {
    Predicate pred;
    boolean isJoin = false;
    JoinPredicate joinpred;
    Pair<Boolean,Boolean> JoinDecision;

    public Decision(Predicate pred) {
        this.pred = pred;
    }

    public Decision(JoinPredicate joinPred) {this.joinpred = joinPred; isJoin = true;}

    public boolean DecideNonJoin(){
        //to do
        return false;
    }

    public Pair<Boolean,Boolean> DecideJoin(){//cleanNow bit for left and right relation in join predicate
        //to do
        return new Pair<>(false,false);
    }
}
