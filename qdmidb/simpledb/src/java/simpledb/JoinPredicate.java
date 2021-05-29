package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private int field1, field2;
    private final String attribute1, attribute2;
    private final Predicate.Op op;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * field1
     *            The field index into the first tuple in the predicate
     * field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(String attribute1, Predicate.Op op, String attribute2) {
        this.attribute1 = attribute1;
        this.attribute2 = attribute2;
        this.op = op;
    }

    public void setField(DbIterator dbIterator){
        this.field1 = dbIterator.getTupleDesc().fieldNameToIndex(this.attribute1);
        this.field2 = dbIterator.getTupleDesc().fieldNameToIndex(this.attribute2);
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        return t1.getField(field1).compare(op, t2.getField(field2));
    }
    
    public int getField1()
    {
        return field1;
    }
    
    public int getField2()
    {
        return field2;
    }
    
    public Predicate.Op getOperator()
    {
        return op;
    }

    public boolean isMissingLeft(Tuple t1){//check if the given tuple in left relation is missing
        return t1.getField(field1).isMissing();
    }

    public boolean isMissingRight(Tuple t2){//check if the given tuple in right relation is missing
        return t2.getField(field2).isMissing();
    }

    public Tuple updateTupleLeft(Tuple t, Field value){//update tuple in left relation
        t.setField(field1, value);
        return t;
    }

    public Tuple updateTupleRight(Tuple t, Field value){//update tuple in right relation
        t.setField(field2, value);
        return t;
    }
}
