package QMIDB;
import simpledb.*;

import java.util.*;

public class SmartJoin extends Operator{
    private static enum Type { NESTED_LOOPS, HASH };
    private static final long serialVersionUID = 1L;

    private final JoinPredicate pred;
    private DbIterator child1, child2;
    private Attribute attribute1, attribute2;
    private boolean CleanNow2;//ask decision node if we need to clean missing values in this join operator
    private Tuple t1 = null, t11 = null, rightTuple = null;//t11 stores selfJoinResult
    private final Type type;
    private boolean selfJoinFlag = false;
    private boolean child2IsMissing = false;//if right relation has missing values on predicate attribute (attribute2)

    private HashMap<Field, List<Integer>> table;//table stores the hashTable for child2 in join operator
    private Iterator<Tuple> selfJoinResult = null, nullOuterTuple = null;//similar to list
    private Iterator<Integer> matches = null;
    private List<Tuple> tempOuterNullTuples; //store tuples containing missing values by outer join in right relation

    private List<Integer> PAfieldLeft;

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
        pred = p;
        this.child1 = child1;
        this.child2 = child2;
        pred.setField(this.child1, this.child2);
        attribute1 = new Attribute(getJoinField1Name());
        attribute2 = new Attribute(getJoinField2Name());
        tempOuterNullTuples = new ArrayList<>();
        Statistics.getAttribute(attribute2.getAttribute()).setRight(true);
        PAfieldLeft = Statistics.computePAfield(child1.getTupleDesc());

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
        Decision decide = new Decision(pred);
        CleanNow2 = decide.Decide(this.attribute2.getAttribute());
        super.open();
        child1.open();
        child2.open();

        /*
            * build hash table for right relation - child 2
            * add tuples in right relation containing missing values to tempOuterNullTuples
            * tuples in tempOuterNullTuples will finally be returned in getNextRightTuple()
         */

        if (type == Type.HASH) {
            //this implementation can make sure all join-able attributes are AT MOST indexed ONCE
            //check if hashTable for second child exist
            table = new HashMap<>();
            if(!HashTables.ifExistHashTable(getJoinField2Name())){//not found, build a new one
                int joinAttrIdx = pred.getField2();
                while (child2.hasNext()) {
                    Tuple t = child2.next();

                    //System.out.println("hash tables before: *******8");
                    //System.out.println(t);

                    if (pred.isMissingRight(t) && CleanNow2) {
                        //clean right join attribute in this tuple
                        t = pred.updateTupleRight(t, ImputeFactory.Impute(attribute2, t));
                        //update NumOfNullValue for corresponding graph node
                        RelationshipGraph.getNode(this.attribute2.getAttribute()).NumOfNullValuesMinusOne();
                        RelationshipGraph.trigger(this.attribute2);
                        Buffer.updateTuple(t);
                    }
                    else if (pred.isMissingRight(t) && !CleanNow2){
                        child2IsMissing = true;
                    }
                    if(!pred.isMissingRight(t)){
                        Field joinAttr = t.getField(joinAttrIdx);
                        if(joinAttr == new IntField(simpledb.Type.NULL_INTEGER)){continue;}
                        if (!table.containsKey(joinAttr)) {
                            table.put(joinAttr, new ArrayList<Integer>());
                        }
                        //in left deep tree, right join relation always gets raw tuple from its relation
                        table.get(joinAttr).add(t.getTID());
                    }else{
                        //t has missing value in right join attribute
                        //generate outer join tuple to preserve this missing value
                        Tuple tt = new Tuple(constructNullTuple(child1), t);
                        tt.setRawBit(false);
                        tt.setAttribute2TID(t.getAttribute2TID());
                        tt.copyTidSource(t.getTidSource());
                        tt.setMergeBit(true);
                        Buffer.addTuple(tt);
                        tempOuterNullTuples.add(tt);
                    }
                }
                //update table to HashTable
                HashTables.addHashTable(attribute2.getAttribute(), new HashTable(attribute2.getAttribute(), table));
            }else{
                table = HashTables.getHashTable(getJoinField2Name()).getHashMap();
            }
            //System.out.println("print hashTable in " +attribute2);
            //HashTables.print();
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
                // we build hashTable for child 1 for later using if not done before while iterating
                /*Attribute fieldName1 = new Attribute(getJoinField1Name());
                HashTable table1 = new HashTable(fieldName1);
                boolean isBuild = true;
                if(HashTables.findHashTable(table1) == -1){isBuild = false;}//not found the hashTable*/

                while(true){
                    if(t1 == null){
                        if(selfJoinFlag){
                            if(selfJoinResult.hasNext()){
                                selfJoinFlag = true;
                                t11 = selfJoinResult.next();
                            }else{
                                t1 = null;
                                selfJoinFlag = false;
                                continue;
                            }
                        }else if(child1.hasNext()){
                            t1 = child1.next();
                            //ihe:print
                            //System.out.println(t1);
                            //pred.toPredicateUnit().print();
                            //Statistics.print();
                            //System.out.println("Join: " + t1);
                            List<Tuple> joinResult = selfJoin(t1);
                            if(joinResult == null){
                                t1 = null;
                                selfJoinFlag = false;
                                //t1 is filtered away
                                continue;
                            }
                            selfJoinResult = joinResult.iterator();
                            if(selfJoinResult.hasNext()){
                                selfJoinFlag = true;
                                t11 = selfJoinResult.next();
                            }else{
                                t1 = null;
                                selfJoinFlag = false;
                                continue;
                            }
                        }else{
                            //when scanning left relation is done, scan hashtable for right relation
                            return getNextRightTuple();
                        }
                    }

                    if (matches == null) {
                        Field rightField = t11.getField(pred.getField1());
                        //System.out.println("* " + rightField + " " + attribute1.getAttribute() + " " + attribute2.getAttribute());
                        //ihe:ifmatch
                        t11.setPAfield(PAfieldLeft);
                        List<Integer> m = table.get(rightField);

                        /*if(m!=null){
                            System.out.println("printf hash table");
                            for(int i=0;i<m.size();i++){
                                System.out.println(m.get(i));
                            }
                        }*/
                        //there is non matched tuple for t11
                        if (m == null) {
                            //implement outer join only for tuples containing null values
                            //the following if-else is optimization for space, in this case we need to build hashtables for left relation
                            //otherwise, we need to output left tuple + NULL
                            /*if(t11.hasMissingFields()){
                                t1 = null;
                                return new Tuple(t11, constructNullTuple(child2));
                            }
                            else{
                                //space optimization: for non-matching tuples which do not have missing values, do not store
                                //their information is contained in hashTable
                                t1 = null;
                                continue;
                            }*/
                            t11.countMissingValueBy(1);
                            t11.countOuterTupleBy(1);
                            t11.countImputedJoinBy(1);
                            Statistics.addJoins(1);
                            t1 = null;
                            if(t11.hasMissingFields() || child2IsMissing){//ihe: check
                                t11.addOuterAttribute(attribute2.getAttribute());
                                Tuple t2 = new Tuple(t11, constructNullTuple(child2));
                                Buffer.addTuple(t2);
                                t2.setRawBit(false);
                                t2.setMergeBit(true);
                                t2.setAttribute2TID(t11.getAttribute2TID());
                                t2.copyTidSource(t11.getTidSource());
                                return t2;
                            }
                            else{
                                continue;
                            }
                        }else{
                            //set matchBits for rightField
                            t11.countMissingValueBy(m.size());
                            t11.countOuterTupleBy(m.size());
                            t11.countImputedJoinBy(m.size());
                            Statistics.addJoins(m.size());
                            HashTables.getHashTable(attribute2.getAttribute()).setMatchBit(rightField);
                            matches = m.iterator();
                        }
                    }

                    while (true) {
                        if (matches.hasNext()) {
                            Tuple t22 = Buffer.getTuple(matches.next());
                            Tuple tn = new Tuple(t11, t22);
                            Buffer.addTuple(tn);
                            tn.mergeAttribute2TID(t11.getAttribute2TID(),t22.getAttribute2TID());
                            tn.mergeTidSource(t11.getTidSource(), t22.getTidSource());
                            tn.setMergeBit(true);
                            tn.setRawBit(false);
                            return tn;
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

    public Tuple constructNullTuple(DbIterator child){
        TupleDesc schema = child.getTupleDesc();
        Field[] fields = new Field[schema.numFields()];
        for(int i=0;i<schema.numFields();i++){
            fields[i] = new IntField(simpledb.Type.NULL_INTEGER);
        }
        return new Tuple(schema, fields);
    }

    public Tuple getNextRightTuple(){
        if(nullOuterTuple == null){
            nullOuterTuple = tempOuterNullTuples.iterator();
        }
        while(nullOuterTuple.hasNext()){
            return nullOuterTuple.next();
        }
        return null;
    }

    boolean isRemove(Tuple t, String leftAttribute)throws Exception{
        /*
            * given a tuple t and some active leftAttribute
            * return if t will be removed by this triggered predicate
            * true: can be removed
         */
        if(t.getApplied_bit(leftAttribute)){//if leftAttribute has been applied before, skip
            return false;
        }
        Field leftValue = t.getField(t.getTupleDesc().fieldNameToIndex(leftAttribute));
        List<String> activeRightAttributes = RelationshipGraph.findRelatedActiveRightAttributes(leftAttribute);

        Statistics.addJoins(activeRightAttributes.size());

        for(int j=0;j<activeRightAttributes.size();j++){
            String rightAttribute = activeRightAttributes.get(j);
            if(!HashTables.ifExistHashTable(rightAttribute)){
                throw new Exception("HashTable Not Found!");
            }
            else{
                if(!HashTables.getHashTable(rightAttribute).getHashMap().containsKey(leftValue)){//non-matching
                    Buffer.removeTuple(t);
                    return true;
                }
            }
        }
        t.setApplied_Bit(leftAttribute);
        return false;
    }


    /*
        * given a tuple t
        * check if predicate attribute values in t have missing values
        * if t has missing values, then the corresponding predicate cannot be triggered
        * predicate trigger condition is that there are no missing values in their columns
        * ask if clean now
        * if clean, update hash tables and relationship graph
    */

    public List<Tuple> selfJoin(Tuple t) throws Exception{//t must be in the left relation
        //System.out.println("in join: " + AggregateOptimization.temporalMax);
        if(!AggregateOptimization.passVirtualFilter(t)){
            //System.out.println("in join: " + AggregateOptimization.temporalMax);
            return null;
        }
        //check all current active predicates to see if t can be removed
        List<String> activeLeftAttributes = RelationshipGraph.getActiveLeftAttribute();

        boolean flag = false; //fast check first
        for(int i=0;i<activeLeftAttributes.size();i++){//one left could correspond to multiple right attributes
            String leftAttribute = activeLeftAttributes.get(i);
            int index = t.getTupleDesc().fieldNameToIndex(leftAttribute);
            //skip non-exist values in this tuple
            if(index == -1){
                continue;
            }
            if(t.getField(index).isMissing() || t.getField(index).isNull()){
                continue;
            }
            flag = isRemove(t, leftAttribute);
            if(flag) break;
        }

        //if flag = true, tuple t will be removed
        if(flag){
            Buffer.removeTuple(t);
            //if now a missing value is removed, update relationship graph
            for(int i=0;i<t.getTupleDesc().numFields();i++){
                if(t.getField(i).isMissing()){
                    String attribute = (t.getTupleDesc().getFieldName(i));
                    updateGraph(attribute);
                }
            }
            return null;
        }
        //System.out.println("second " + t);

        //if codes go here, it cannot be removed by current active predicates
        //next ask decision node if we need to impute missing values if any

        boolean flag1 = false;

        List<Tuple> matching = new ArrayList<>();
        //check if the predicate attribute have missing values
        for(int i=0;i<t.getTupleDesc().numFields();i++){
            if(t.getField(i).isMissing()){
                String field = t.getTupleDesc().getFieldName(i);//field is current attribute name
                //System.out.println("attribute name = " + field);
                //only consider attribute in predicate set
                if(!PredicateSet.isExist(field)){
                    continue;
                }
                //ask decision node if clean now

                //find all the predicates containing this attribute
                List<PredicateUnit> predicates = PredicateSet.getPredicateUnits(field);
                for(int j=0;j<predicates.size();j++){
                    String type = predicates.get(j).getType();
                    if(type == "Join") {
                        Decision decideJoin = new Decision(predicates.get(j).toJoinPredicate());
                        Attribute left = predicates.get(j).getLeft();
                        Attribute right = predicates.get(j).getRight();
                        boolean CleanLeft = decideJoin.Decide(left.getAttribute());
                        boolean CleanRight = decideJoin.Decide(right.getAttribute());
                        boolean isUpdate = false;
                        if(left.getAttribute().equals(field) && CleanLeft){//left relation
                            isUpdate = true;
                            //clean tuple
                            t.setField(i,ImputeFactory.Impute(left, t));
                            Buffer.updateTuple(t);
                        }else if(right.getAttribute().equals(field) && CleanRight){
                            isUpdate = true;
                            //clean tuple
                            Field newValue = ImputeFactory.Impute(right, t);
                            t.setField(i,newValue);
                            //t must not be in the hash table of right attribute as it has missing right join attribute value before
                            //in updateHashTable: first update corresponding tuple in buffer pool, then add tid into hash table
                            addEntryInHashTable(right.getAttribute(), newValue, subTuple(right.getAttribute(), t));
                        }
                        if(isUpdate){
                            //update graph
                            updateGraph(right.getAttribute());
                            //check now if current attribute active or not, if yes, check if this tuple can be removed
                            if(RelationshipGraph.isActiveLeft(field)){
                                if(isRemove(t, field)){
                                    Buffer.removeTuple(t);
                                    flag1 = true;
                                    break;
                                }
                            }
                        }
                    }
                    else if(type == "Filter") {//Filter would always appear before Join
                        Decision decideFilter = new Decision(predicates.get(j).toPredicate());
                        boolean CleanFilter = decideFilter.Decide(predicates.get(j).getFilterAttribute().getAttribute());
                        if(CleanFilter){
                            Field newValue = ImputeFactory.Impute(predicates.get(j).getFilterAttribute(),t);
                            //if satisfy filter predicate, then update the tuple
                            if(newValue.compare(predicates.get(j).getOp(),predicates.get(j).getOperand())){
                                t.setField(i,newValue);
                                Buffer.updateTuple(t);
                            }
                            else{//not satisfied, remove this tuple
                                flag1 = true;
                                Buffer.removeTuple(t);
                                break;
                            }
                        }
                    }
                }
            }
        }

        if(flag1){//t will be removed
            return null;
        }
        //System.out.println("third " + t);

        matching.add(t);
        t.setPAfield(PAfieldLeft);

        return matching;
    }

    public Tuple subTuple(String attribute, Tuple t){
        String first = Schema.getFirstAttribute(attribute);
        int start = t.getTupleDesc().fieldNameToIndex(first);
        int width = Schema.getWidth(attribute);
        TupleDesc subTD = t.getTupleDesc().SubTupleDesc(start, width);
        Tuple subT = new Tuple(subTD);
        subT.setTID(t.getTID());
        //subT.setMergeBit(t.isMergeBit());
        subT.setAttribute2TID(t.getAttribute2TID());
        for(int i=0;i<width;i++){
            subT.setField(i, t.getField(i+start));
        }
        return subT;
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

    public void updateGraph(String attribute){
        RelationshipGraph.getNode(attribute).NumOfNullValuesMinusOne();
        RelationshipGraph.trigger(new Attribute(attribute));
    }

    public void addEntryInHashTable(String attribute, Field value, Tuple t){
        //in updateHashTable: first update corresponding tuple in buffer pool, then add tid into hash table
        int tid;
        //the content of t will always be raw tuple, but its source is not decided yet
        //first update corresponding tuple in buffer pool
        if(t.isMergeBit()){//joined tuple
            tid = t.findTID(attribute);
        }else{//raw tuple
            tid = t.getTID();
        }
        t.setTidSource(tid);
        Buffer.updateTupleByTID(t, tid);
        //add tid into hash table
        if(!HashTables.ifExistHashTable(attribute)){
            HashMap<Field, List<Integer>> hashMap = new HashMap<>();
            hashMap.put(value, new ArrayList<>());
            hashMap.get(value).add(tid);
            HashTables.addHashTable(attribute, new HashTable(attribute, hashMap));
        }
        else{
            if(!HashTables.getHashTable(attribute).hasKey(value)){
                HashTables.getHashTable(attribute).getHashMap().put(value, new ArrayList<>());
            }
            HashTables.getHashTable(attribute).getHashMap().get(value).add(tid);
        }
    }
}
