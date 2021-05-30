package QMIDB;
import simpledb.*;

import java.util.*;

public class SmartJoin extends Operator{
    private static enum Type { NESTED_LOOPS, HASH };
    private static final long serialVersionUID = 1L;

    private final JoinPredicate pred;
    private DbIterator child1, child2;
    private Attribute attribute1, attribute2;
    private boolean CleanNow1, CleanNow2;//ask decision node if we need to clean missing values in this join operator
    private Tuple t1 = null, t11 = null;
    private final Type type;

    private HashTable table;
    private Iterator<Tuple> matches, selfJoinResult = null;//similar to list

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
    public SmartJoin(JoinPredicate p, DbIterator child1, DbIterator child2) throws Exception{
        Decision decide = new Decision(p);
        CleanNow1 = decide.JoinDecision.getKey();
        CleanNow2 = decide.JoinDecision.getValue();
        attribute1 = new Attribute(getJoinField1Name());
        attribute2 = new Attribute(getJoinField2Name());
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
            //this implementation can make sure all join-able attributes are AT MOST indexed ONCE
            //check if hashTable for second child exist
            if(!HashTables.ifExistHashTable(getJoinField2Name())){//not found, build a new one
                int joinAttrIdx = pred.getField2();
                while (child2.hasNext()) {
                    Tuple t = child2.next();
                    //check cleaning for right relation : child 2
                    if(pred.isMissingRight(t)){
                        //ask decision function if clean now
                        if(CleanNow2){
                            //clean this tuple
                            t = pred.updateTupleRight(t,ImputeFactory.Impute(t.getField(pred.getField2())));
                            //update NumOfNullValue for corresponding graph node
                            RelationshipGraph.getNode(this.attribute2).NumOfNullValuesMinusOne();
                            trigger();
                        }
                        else{
                            continue;//do not build hashTable for null values
                        }
                    }

                    Field joinAttr = t.getField(joinAttrIdx);
                    if(joinAttr == new IntField(simpledb.Type.NULL_INTEGER)){continue;}
                    if (!table.getHashMap().containsKey(joinAttr)) {
                        table.getHashMap().put(joinAttr, new ArrayList<Tuple>());
                    }
                    table.getHashMap().get(joinAttr).add(t);
                }
            }else{
                table = HashTables.getHashTable(getJoinField2Name());
            }

        }
    }

    public void trigger(){
        if(RelationshipGraph.getNode(attribute1).getNumOfNullValues() == 0 && RelationshipGraph.getNode(attribute2).getNumOfNullValues() == 0){
            RelationshipGraph.getEdge(attribute1,attribute2).setActive();
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
                if (table.getHashMap().size() == 0) { return null; }

                // Iterate over the outer relation and select matching tuples from the table.
                // we build hashTable for child 1 for later using if not done before while iterating
                /*Attribute fieldName1 = new Attribute(getJoinField1Name());
                HashTable table1 = new HashTable(fieldName1);
                boolean isBuild = true;
                if(HashTables.findHashTable(table1) == -1){isBuild = false;}//not found the hashTable*/

                while (true) {
                    // If we don't have a working tuple from the outer relation, get one.
                    // If nothing's available, we're done.
                    if (t1 == null) {
                        if (child1.hasNext()) {
                            t1 = child1.next();
                            //check if t1 can be applied using self join condition
                            selfJoinResult = selfJoin(t1).iterator();
                            if(selfJoinResult == null){
                                //t1 is filtered away
                                continue;
                            }
                            while(selfJoinResult.hasNext()){
                                t11 = selfJoinResult.next();
                                //check cleaning for left relation : child 1
                                if(pred.isMissingLeft(t11)){
                                    //ask decision function if clean now
                                    if(CleanNow1){
                                        //clean this tuple
                                        t11 = pred.updateTupleLeft(t11,ImputeFactory.Impute(t11.getField(pred.getField1())));
                                        //update NumOfNullValue for corresponding graph node
                                        RelationshipGraph.getNode(this.attribute1).NumOfNullValuesMinusOne();
                                        trigger();
                                    }
                                }

                                //find all matching tuples in inner relation,and store in matches
                                if (matches == null) {
                                    List<Tuple> m = table.getHashMap().get(t11.getField(pred.getField1()));
                                    if (m == null) {
                                        //implement outer join only for tuples containing null values
                                        if(t11.hasMissingFields()){
                                            t1 = null;
                                            //ihe: to verify this logic
                                            return new Tuple(t11, constructNullTuple(child2));
                                        }
                                        else{
                                            t1 = null;
                                            continue;
                                        }
                                    }else{
                                        matches = m.iterator();
                                    }
                                }

                                while (true) {
                                    if (matches.hasNext()) {
                                        Tuple t2 = matches.next();
                                        return new Tuple(t11, t2);
                                    } else {
                                        t1 = null;
                                        matches = null;
                                        break;
                                    }
                                }
                            }
                        } else {
                            return null;
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

    public Tuple constructNullTuple(DbIterator child){
        TupleDesc schema = child.getTupleDesc();
        Field[] fields = new Field[schema.getSize()];
        for(int i=0;i<schema.getSize();i++){
            fields[i] = new IntField(simpledb.Type.NULL_INTEGER);
        }
        return new Tuple(schema, fields);
    }

    public List<Tuple> selfJoin(Tuple t) throws Exception{//t must be in the left relation
        List<Tuple> matching = null;
        List<String> validPredicates = RelationshipGraph.findActiveEdge(getJoinField1Name());
        Field leftValue  = t.getField(pred.getField1());
        boolean flag = false; //fast check first
        for(int i=0;i<validPredicates.size();i++){
            String validPred = validPredicates.get(i);
            if(!HashTables.ifExistHashTable(validPred)){throw new Exception("Hashtable NOT Exist!");}
            else{
                if(!HashTables.getHashTable(validPred).getHashMap().containsKey(leftValue)){
                    return null;
                }
            }
        }
        //update tuples and construct matching
        matching.add(t);
        for(int i=0;i<validPredicates.size();i++){
            int size = matching.size();
            for(int j=0;j<size;j++){
                String validPred = validPredicates.get(i);
                List<Tuple> temporalMatch = HashTables.getHashTable(validPred).getHashMap().get(leftValue);
                int tupleSize = temporalMatch.get(0).getTupleDesc().getSize();
                String firstFieldName = temporalMatch.get(0).getTupleDesc().getFieldName(0);
                int firstFieldIndex = t.getTupleDesc().fieldNameToIndex(firstFieldName);
                if(firstFieldIndex == -1){//attributes of right tuple is not included in left tuple
                    break;//jump to next predicate
                }
                for(int k=0;k<temporalMatch.size();k++){//iterate all the matching tuples
                    Tuple tt = matching.get(j);
                    for(int kk=0;kk<tupleSize;kk++){
                        tt.setField(firstFieldIndex+kk, temporalMatch.get(j).getField(kk));
                    }
                    matching.add(tt);
                }
            }
        }
        return matching;
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
