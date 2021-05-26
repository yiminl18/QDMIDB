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
    private int nullType = -999;
    private Decision decideNode;
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
    public SmartFilter(Predicate p, DbIterator child) {
        pred = p;
        this.child = child;
        this.decideNode = new Decision(p);
        isClean = this.decideNode.DecideNonJoin();
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
            TransactionAbortedException {
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
            TransactionAbortedException, DbException {
        while (child.hasNext()) {
            Tuple t = child.next();
            if(pred.isMissing(t)){
                //ask decision function if clean now
                if(isClean){
                    //clean this tuple
                    t = pred.updateTuple(t,ImputeFactory.Impute(t.getField(pred.getField())));
                }
                return t;
            }
            else if (pred.filter(t)) {
                return t;
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


}
