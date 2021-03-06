package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {
	private static enum Type { NESTED_LOOPS, HASH };
    private static final long serialVersionUID = 1L;
    
    private final JoinPredicate pred;
    private DbIterator child1, child2;
    private Tuple t1 = null;
    private final Type type;
    
    private HashMap<Field, ArrayList<Tuple>> table;
    private Iterator<Tuple> matches = null;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) throws Exception{
        pred = p;
        this.child1 = child1;
        this.child2 = child2;
        pred.setField(this.child1, this.child2);

        switch(pred.getOperator()) {
		case EQUALS:
		case LIKE:
			type = Type.HASH;
			break;
		case GREATER_THAN:
		case GREATER_THAN_OR_EQ:
		case LESS_THAN:
		case LESS_THAN_OR_EQ:
		case NOT_EQUALS:
			type = Type.NESTED_LOOPS;
			break;
		default:
			throw new RuntimeException("Unexpected join predicate.");
        }
    }

    public JoinPredicate getJoinPredicate() {
        return pred;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
    	TupleDesc schema = child1.getTupleDesc();
        String fName = schema.getFieldName(pred.getField1());
        // TODO: Should be qualified. DbIterator doesn't support this though...
        return fName;
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
    	TupleDesc schema = child2.getTupleDesc();
        String fName = schema.getFieldName(pred.getField2());
        // TODO: Should be qualified. DbIterator doesn't support this though...
        return fName;
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException, Exception {
        super.open();
        child1.open();
        child2.open();
        
        if (type == Type.HASH) {
        	table = new HashMap<>();
        	int joinAttrIdx = pred.getField2();
			while (child2.hasNext()) {
				Tuple t = child2.next();
				Field joinAttr = t.getField(joinAttrIdx);
				if (!table.containsKey(joinAttr)) {
					table.put(joinAttr, new ArrayList<Tuple>());
				}
				table.get(joinAttr).add(t);
			}
        }
    }

    @Override
    public void close() {
        super.close();
        child1.close();
        child2.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, Exception {
    	switch (type) {
		case HASH:
			if (table.size() == 0) { return null; }
			
			// Iterate over the outer relation and select matching tuples from the table.
			while (true) {
				// If we don't have a working tuple from the outer relation, get one.
				// If nothing's available, we're done.
				if (t1 == null) {
					if (child1.hasNext()) {
						t1 = child1.next();
					} else {
						return null;
					}
				}
				
				if (matches == null) {
					ArrayList<Tuple> m = table.get(t1.getField(pred.getField1()));
					if (m == null) {
						t1 = null;
						continue;
					}
					matches = m.iterator();
				}

				while (true) {
					if (matches.hasNext()) {
						Tuple t2 = matches.next();
						return new Tuple(t1, t2);
					} else {
						t1 = null;
						matches = null;
						break;
					}
				}
			}
		case NESTED_LOOPS:
			// Iterate over the outer relation and select matching tuples from the table.
			while (true) {
				// If we don't have a working tuple from the outer relation, get one.
				// If nothing's available, we're done.
				if (t1 == null) {
					if (child1.hasNext()) {
						t1 = child1.next();
					} else {
						return null;
					}
				}

				while (true) {
					if (child2.hasNext()) {
						Tuple t2 = child2.next();
						if (pred.filter(t1, t2)) {
							return new Tuple(t1, t2);
						}
					} else {
						t1 = null;
						child2.rewind();
						break;
					}
				}
			}
		default:
			throw new RuntimeException("Unexpected type.");
    	}   	
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { child1, child2 };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 2) {
        	throw new IllegalArgumentException("Expected two new children.");
        }
        this.child1 = children[0];
        this.child2 = children[1];
    }
}
