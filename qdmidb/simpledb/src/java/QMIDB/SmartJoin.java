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
    private Tuple t1 = null, t11 = null, rightTuple = null;//t11 stores selfJoinResult
    private final Type type;
    private boolean selfJoinFlag = false;

    private HashTable table;//table stores the hashTable for child2 in join operator
    private Iterator<Tuple> matches, selfJoinResult, hashMatch, nullOuterTuple = null;//similar to list
    private Iterator rightRelation = null;
    private HashMap<Predicate, Boolean> CleanBits;//call
    private Map.Entry mapElement = null;
    private List<Tuple> tempOuterNullTuples;

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
        table = new HashTable(attribute2);
        tempOuterNullTuples = new ArrayList<>();

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
        CleanNow1 = decide.DecideJoin().getKey();
        CleanNow2 = decide.DecideJoin().getValue();
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
                            //create temp null values for outer join purpose
                            tempOuterNullTuples.add(new Tuple(constructNullTuple(child1), t));
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
                //update table to HashTable
                HashTables.addHashTable(attribute2.getAttribute(), table);
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

    public void trigger(Attribute left, Attribute right){
        if(RelationshipGraph.getNode(left).getNumOfNullValues() == 0 && RelationshipGraph.getNode(right).getNumOfNullValues() == 0){
            RelationshipGraph.getEdge(left,right).setActive();
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
                            List<Tuple> joinResult = selfJoin(t1);
                            if(joinResult == null){
                                t1 = null;
                                selfJoinFlag = false;
                                //t1 is filtered away
                                continue;
                            }
                            selfJoinResult = selfJoin(t1).iterator();
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
                        List<Tuple> m = table.getHashMap().get(rightField);
                        //set matchBits for rightField
                        HashTables.getHashTable(attribute2.getAttribute()).setMatchBit(rightField);
                        table.setMatchBit(rightField);
                        if (m == null) {
                            //implement outer join only for tuples containing null values
                            if(t11.hasMissingFields()){
                                t1 = null;
                                return new Tuple(t11, constructNullTuple(child2));
                            }
                            else{
                                //space optimization: for non-matching tuples which do not have missing values, do not store
                                //their information is contained in hashTable
                                t1 = null;
                                continue;
                            }
                        }else{
                            matches = m.iterator();
                        }
                    }

                    while (true) {
                        if (matches.hasNext()) {
                            Tuple t22 = matches.next();
                            return new Tuple(t11, t22);
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
        while(true){
            if(rightRelation == null){
                rightRelation = table.getHashMap().entrySet().iterator();
                nullOuterTuple = tempOuterNullTuples.iterator();
            }
            if(!rightRelation.hasNext()){
                while(nullOuterTuple.hasNext()){
                    return nullOuterTuple.next();
                }
                return null;
            }
            mapElement = (Map.Entry)rightRelation.next();
            Field field = (Field) mapElement.getKey();
            if(hashMatch == null){
                if(!HashTables.getHashTable(attribute2.getAttribute()).getMatchBit(field)){//not matched, construct new tuple and return
                    List<Tuple> tempTuples = HashTables.getHashTable(attribute2.getAttribute()).getHashTable(field);
                    hashMatch = tempTuples.iterator();
                }else{//this field has already been matched before
                    continue;
                }
            }
            while(true){
                if(hashMatch.hasNext()){
                    return new Tuple(constructNullTuple(child1), hashMatch.next());
                }else{
                    hashMatch = null;
                    break;
                }
            }
        }
    }

    public List<Tuple> selfJoin(Tuple t) throws Exception{//t must be in the left relation
        List<Tuple> matching = new ArrayList<>();
        //check if the predicate attribute have missing values
        for(int i=0;i<t.getTupleDesc().numFields();i++){
            if(t.getField(i).isMissing()){
                String field = t.getTupleDesc().getFieldName(i);
                //System.out.println("attribute name = " + field);
                if(!PredicateSet.isExist(field)){
                    continue;
                }
                //ask decision node if clean now

                List<PredicateUnit> predicates = PredicateSet.getPredicateUnits(field);
                for(int j=0;j<predicates.size();j++){
                    String type = predicates.get(j).getType();
                    switch (type){
                        case "Join":
                            Decision decideJoin = new Decision(predicates.get(j).toJoinPredicate());
                            boolean CleanLeft = decideJoin.DecideJoin().getKey();
                            boolean CleanRight = decideJoin.DecideJoin().getValue();
                            Attribute left = predicates.get(j).getLeft();
                            Attribute right = predicates.get(j).getRight();
                            boolean isUpdate = false;
                            if(left.getAttribute().equals(field) && CleanLeft){//left relation
                                isUpdate = true;
                                //clean tuple
                                t.setField(i,ImputeFactory.Impute(null));
                            }else if(right.getAttribute().equals(field) && CleanRight){
                                isUpdate = true;
                                //clean tuple
                                Field newValue = ImputeFactory.Impute(null);
                                t.setField(i,newValue);
                                //update hashTables
                                if(!HashTables.ifExistHashTable(right.getAttribute())){
                                    HashMap<Field, List<Tuple>> hashMap = new HashMap<>();
                                    hashMap.put(newValue, new ArrayList<>());
                                    hashMap.get(right.getAttribute()).add(subTuple(t,right,i));
                                    HashTables.addHashTable(right.getAttribute(), new HashTable(right, hashMap));
                                }
                            }
                            if(isUpdate){
                                //update graph
                                RelationshipGraph.getNode(new Attribute(field)).NumOfNullValuesMinusOne();
                                trigger(predicates.get(j).getLeft(), predicates.get(j).getRight());
                            }
                            break;
                        case "Filter":
                            Decision decideFilter = new Decision(predicates.get(j).toPredicate());
                            boolean CleanFilter = decideFilter.DecideNonJoin();
                            if(CleanFilter){
                                Field newValue = ImputeFactory.Impute(null);
                                //if satisfy filter predicate, then update the tuple
                                if(newValue.compare(predicates.get(j).getOp(),predicates.get(j).getOperand())){
                                    t.setField(i,newValue);
                                }
                            }
                            break;
                    }
                }
            }
        }

        matching.add(t);
        //System.out.println("here " + t);
        //check all current active predicates
        List<String> activeLeftAttribute = RelationshipGraph.findAllActiveEdge();
        if(activeLeftAttribute.size() == 0) return matching;

        boolean flag = false; //fast check first
        for(int i=0;i<activeLeftAttribute.size();i++){
            String leftAttribute = activeLeftAttribute.get(i);
            Field leftValue = t.getField(t.getTupleDesc().fieldNameToIndex(leftAttribute));
            if(!HashTables.ifExistHashTable(leftAttribute)){
                throw new Exception("HashTable Not Found!");
            }
            else{
                if(HashTables.getHashTable(leftAttribute).getHashMap().containsKey(leftValue)){//non-matching
                    flag = true;
                    break;
                }
            }
        }

        if(flag){
            //if now a missing value is removed, update relationship graph
            for(int i=0;i<t.getTupleDesc().numFields();i++){
                if(t.getField(i).isMissing()){
                    Attribute attribute = new Attribute(t.getTupleDesc().getFieldName(i));
                    RelationshipGraph.getNode(attribute).NumOfNullValuesMinusOne();
                    //only trigger join: find relevant join predicates
                    RelationshipGraph.triggerByAttribute(attribute);
                }
            }
            return null;
        }

        //update tuples and construct matching
        for(int i=0;i<activeLeftAttribute.size();i++){
            int size = matching.size();
            for(int j=0;j<size;j++){
                String leftAttribute = activeLeftAttribute.get(i);
                Field leftValue = t.getField(t.getTupleDesc().fieldNameToIndex(leftAttribute));
                List<Tuple> temporalMatch = HashTables.getHashTable(leftAttribute).getHashMap().get(leftValue);
                int tupleSize = temporalMatch.get(0).getTupleDesc().numFields();
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

    public Tuple subTuple(Tuple t, Attribute attribute, int start){
        int width = Schema.getWidth(attribute.getAttribute());
        TupleDesc subTD = t.getTupleDesc().SubTupleDesc(start, width);
        Tuple subT = new Tuple(subTD);
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
}
