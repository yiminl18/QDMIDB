package QMIDB;
import simpledb.*;
import java.util.*;
/*
    this class implements optimizations "learn from failure" for max/min queries
 */
public class AggregateOptimization {
    private static boolean OptimizeFlag = false;
    private static Field temporalMax = new IntField(Integer.MIN_VALUE), temporalMin = new IntField(Integer.MAX_VALUE);
    private static List<PredicateUnit> aggregatePredicates = new ArrayList<>();

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
        if(aggregatePredicates.size() > 0){
            OptimizeFlag = true;
        }
    }

    public static boolean isOptimizeFlag() {
        return OptimizeFlag;
    }

    public static void setOptimizeFlag(boolean optimizeFlag) {
        OptimizeFlag = optimizeFlag;
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

    public static boolean isFiltered (Tuple t)throws Exception{
        //return if given tuple satisfy max/min selections
        //true: satisfied; false: not satisfied
        if(!OptimizeFlag){//no constraint -> pass
            return true;
        }else{
            for(int i=0;i<aggregatePredicates.size();i++){
                PredicateUnit pred = aggregatePredicates.get(i);
                if(pred.getOp().equals(Aggregator.Op.MAX)){
                    int fieldIndex = t.getTupleDesc().fieldNameToIndex(pred.getAggregateAttribute().getAttribute());
                    if(fieldIndex == -1){
                        throw new Exception("Projected tuple or attributes error.");
                    }else{
                        Field field = t.getField(fieldIndex);
                        if(field.compare(Predicate.Op.LESS_THAN, temporalMax)){//filter away
                            return false;
                        }
                    }
                }
                if(aggregatePredicates.get(i).getOp().equals(Aggregator.Op.MIN)){
                    int fieldIndex = t.getTupleDesc().fieldNameToIndex(pred.getAggregateAttribute().getAttribute());
                    if(fieldIndex == -1){
                        throw new Exception("Projected tuple or attributes error.");
                    }else{
                        Field field = t.getField(fieldIndex);
                        if(field.compare(Predicate.Op.GREATER_THAN, temporalMin)){//filter away
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }
}
