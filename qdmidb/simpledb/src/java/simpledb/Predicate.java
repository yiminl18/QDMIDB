package simpledb;


import QMIDB.AggregateOptimization;
import QMIDB.Attribute;
import QMIDB.PredicateUnit;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Constants used for return codes in Field.compare */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }
    
    private int field;//this is the index of attribute, not specified the relation
    private final Op op;
    private final Field operand;
    private String attribute;

    /**
     * Constructor.
     *
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     * @param attribute
     *            unique name of attribute (global)
     */
    public Predicate(String attribute, Op op, Field operand) {
        this.attribute = attribute;
        this.op = op;
        this.operand = operand;
    }

    public Predicate(int field, Op op, Field operand) {
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    public QMIDB.PredicateUnit toPredicateUnit(){
        return new PredicateUnit(new Attribute(attribute),op,operand);
    }

    public void setField(DbIterator dbIterator) throws Exception {
        this.field = dbIterator.getTupleDesc().fieldNameToIndex(this.attribute);
        if(this.field == -1) {throw new Exception("attribute cannot found!");}
    }

    /**
     * @return the field number
     */
    public int getField()
    {
    	return field;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
    	return op;
    }
    
    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 
     * @param t
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        return t.getField(field).compare(op, operand);
    }

    public boolean isMissing(Tuple t){
        //check tuple t on predicate value is NULL or not
        return t.getField(field).isMissing();
    }

    public Tuple updateTuple(Tuple t, Field value){
        //System.out.println(value);
        t.setField(field, value);
        return t;
    }

    public Field getOperand()
    {
        return operand;
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string
     */
    public String toString() {
        return String.format("λ f. f[%d] %s %s", field, op.toString(), operand.toString());
    }
}
