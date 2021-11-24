package QMIDB;
import simpledb.*;
import java.util.*;
/*
    this class implements optimizations "learn from failure" for max/min queries
 */
public class AggregateOptimization {
    private static Field temporalMax = new IntField(Integer.MIN_VALUE), temporalMin = new IntField(Integer.MAX_VALUE);
    private static List<PredicateUnit> aggregatePredicates = new ArrayList<>();
    public static final Field virtual = new StringField("virtual",7);

    public static void init(){
        if(PredicateSet.getPredicateSet().size() == 0){
            System.out.println("Read predicates first!");
            return ;
        }
        for(int i=0;i<PredicateSet.getPredicateSet().size();i++){
            PredicateUnit pred = PredicateSet.getPredicateSet().get(i);
            if(pred.getType().equals("Aggregate")){
                if(pred.getAop().equals(Aggregator.Op.MAX) || pred.getAop().equals(Aggregator.Op.MIN)){
                    aggregatePredicates.add(pred);
                }
            }
        }
    }

    public static Field getTemporalMax() {
        return temporalMax;
    }

    public static void setTemporalMax(Field temporalMaxValue) {
        if(temporalMaxValue.compare(Predicate.Op.GREATER_THAN, temporalMax) ){
            temporalMax = temporalMaxValue;
        }
    }

    public static Field getTemporalMin() {
        return temporalMin;
    }

    public static void setTemporalMin(Field temporalMinValue) {
        if(temporalMinValue.compare(Predicate.Op.LESS_THAN, temporalMin)){
            temporalMin = temporalMinValue;
        }
    }
}
