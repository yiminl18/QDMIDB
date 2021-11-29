package QMIDB;
import simpledb.*;
import java.util.*;
/*
    this class implements optimizations "learn from failure" for max/min queries
 */
public class AggregateOptimization {
    public static Field temporalMax = new IntField(Integer.MIN_VALUE+1), temporalMin = new IntField(Integer.MAX_VALUE);
    private static PredicateUnit aggregatePred = null;

    public static PredicateUnit getAggregatePred() {
        return aggregatePred;
    }

    public static void init(){
        if(PredicateSet.getPredicateSet().size() == 0){
            System.out.println("Read predicates first!");
            return ;
        }
        //currently assume only one MAX/MIN, can be easily extended to a list
        for(int i=0;i<PredicateSet.getPredicateSet().size();i++){
            PredicateUnit pred = PredicateSet.getPredicateSet().get(i);
            if(pred.getType().equals("Aggregate")){
                aggregatePred = pred;
                break;
            }
        }
    }

    public static boolean passVirtualFilter(Tuple t){
        if(aggregatePred == null){
            //no MAX/MIN predicate, pass the test
            return true;
        }
        Attribute attr = aggregatePred.getAggregateAttribute();
        int fieldIndex = t.getTupleDesc().fieldNameToIndex(attr.getAttribute());
        if(fieldIndex == -1){
            //t does not contain values to be filter
            return true;
        }
        Field value = t.getField(fieldIndex);
        if(aggregatePred != null && aggregatePred.getAop().equals(Aggregator.Op.MAX)){
            return value.compare(Predicate.Op.GREATER_THAN_OR_EQ, temporalMax);
        }
        if(aggregatePred != null && aggregatePred.getAop().equals(Aggregator.Op.MIN)){
            return value.compare(Predicate.Op.LESS_THAN_OR_EQ, temporalMin);
        }
        return true;
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
