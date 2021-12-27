package QMIDB;

import simpledb.*;

import java.util.NoSuchElementException;
/*
    Impute operator imputes given attribute value
 */

public class Impute extends Operator {

    private static final long serialVersionUID = 1L;

    private Attribute attribute;
    private DbIterator child;
    private int fieldIndex;



    public Impute(Attribute attribute, DbIterator child) {
        this.attribute = attribute;
        this.child = child;
        this.fieldIndex = child.getTupleDesc().fieldNameToIndex(attribute.getAttribute());
    }

    public Attribute getAttribute() {
        return this.attribute;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException, Exception {
        super.open();
        child.open();
    }

    @Override
    public void close() {
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    @Override
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException, Exception {
        while (child.hasNext()) {
            Tuple t = child.next();
            Field value = t.getField(fieldIndex);

            if(value.isMissing()){
                value = ImputeFactory.Impute(this.attribute, t);
                t.setField(fieldIndex, value);
                if(RelationshipGraph.getNode(attribute.getAttribute()) != null){
                    //if imputed values happen to be in join, update it
                    //otherwise, no need to update this stats
                    RelationshipGraph.getNode(attribute.getAttribute()).NumOfNullValuesMinusOne();
                }
                Buffer.updateCompoundTuple(t, attribute.getAttribute(), value);
            }
            //System.out.println(missingNUM);
            return t;
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("Expected a single new child.");
        }
        child = children[0];
    }
}