package QMIDB;

import simpledb.*;

import java.util.NoSuchElementException;

/*
    *this function is used to decide if we need to clean or delay NULL values
    *and also filter tuples given predicate
 */
public class SmartFilter extends Operator{
    private static final long serialVersionUID = 1L;

    private final Predicate pred;
    private DbIterator child;
    private Decision decideNode;
    private Attribute attribute;
    private boolean isClean = false;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public SmartFilter(Predicate p, DbIterator child) throws Exception{
        pred = p;
        this.child = child;
        this.decideNode = new Decision(p);
        pred.setField(this.child);
        getAttribute();
    }

    public Predicate getPredicate() {
        return pred;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException,Exception {
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
            if(pred.isMissing(t)){
                //ask decision function if clean now
                isClean = this.decideNode.Decide(this.attribute.getAttribute());
                //Statistics.print();
                if(isClean){
                    //clean this tuple
                    t = pred.updateTuple(t,ImputeFactory.Impute(attribute, t));
                    //update numofNullValue for corresponding node
                    RelationshipGraph.getNode(attribute.getAttribute()).NumOfNullValuesMinusOne();
                    Buffer.updateTuple(t);
                }
            }
            if (!pred.filter(t)) {
                //t failed predicate test
                Buffer.removeTuple(t);
                continue;
            }else{
                return  t;
            }
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

    public void getAttribute(){//return the attribute on selection operator
        TupleDesc schema = child.getTupleDesc();
        String fName = schema.getFieldName(pred.getField());
        this.attribute = new Attribute(fName);
    }
}
