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

    public boolean DecideNonJoin(){
        //to do
        return false;
    }

    public Pair<Boolean,Boolean> DecideJoin(){
        JoinDecision = new Pair<>(false, false);
        return JoinDecision;
    }

    public boolean Decide(String attribute){//true -> clean now
        //System.out.println(attribute);
        //Statistics.print();
        Attribute attr = Statistics.getAttribute(attribute);
        //if this attribute is dead, clean now
        if(Statistics.isDead(attribute)){
            return true;
        }
        double Prob = attr.getProb();
        double imputeCost = ImputeFactory.getEstimateTime();
        double evaluateVc = attr.getEvaluateVc();
        double evaluateVd = attr.getEvaluateVd();
        double expectedClean = imputeCost + evaluateVc;
        double expectedDelay = Prob*(imputeCost+evaluateVd)+(1-Prob)*evaluateVd;

        //Statistics.print();
        System.out.println("clean vs delay expected cost: " + expectedClean + " " + expectedDelay);
        if(expectedClean > expectedDelay){
            return false;
        }
        else{
            return true;
        }
    }
}
