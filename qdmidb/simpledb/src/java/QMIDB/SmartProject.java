package QMIDB;
/*
    This operator takes care of null values to clean remove null values and output the desired tuple
 */
import simpledb.*;

import java.util.*;

public class SmartProject extends Operator {
    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private TupleDesc td;//projected schema
    private List<Integer> outFieldIds;//projected attributes ids

    private List<Tuple> matching = new ArrayList<>();
    private Iterator<Tuple> candidateMatchIterator, candidateResult = null;
    private List<Tuple> candidateMatching =  new ArrayList<>(), matchedTuples = new ArrayList<>();
    private List<Boolean> candidateMatchingBits = new ArrayList<>();//it is used to indicate which tuple in candidateMatching has been filtered away
    private boolean flag, flagCandidateMatch = false;
    Tuple candidateT = null;
    private String pickedColumn = RelationshipGraph.getNextColumn();
    private final List<String> leftJoinAttributes = copyList(RelationshipGraph.getLeftJoinAttribute());

    /**
     * Constructor accepts a child operator to read tuples to apply projection
     * to and a list of fields in output tuple
     *
     * @param attributes
     *            The ids of the fields child's tupleDesc to project out
     * @param types
     *            the types of the fields in the final projection
     * @param child
     *            The child operator
     */

    public SmartProject(List<Attribute> attributes, Type[] types,
                   DbIterator child) {
        //attributes are projected attributes
        this.child = child;
        TupleDesc childtd = child.getTupleDesc();
        outFieldIds = new ArrayList<>();
        for(int i=0;i<attributes.size();i++){
            outFieldIds.add(childtd.fieldNameToIndex(attributes.get(i).getAttribute()));
        }
        String[] fieldAr = new String[outFieldIds.size()];

        for (int i = 0; i < fieldAr.length; i++) {
            fieldAr[i] = attributes.get(i).getAttribute();
        }
        td = new TupleDesc(types, fieldAr);
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException, Exception {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Operator.fetchNext implementation. Iterates over tuples from the child
     * operator, projecting out the fields from the tuple
     *
     * @return The next tuple, or null if there are no more tuples
     */

    /*
        * selfJoin(t): ->equivalent to clean operator
        * t is removed if some active predicate triggers in t and non-matched result
        * if t has any matches, will return all the matched tuples in matching.iterator()
        * if any attribute in t is left join and inactive attribute, add t to candidateMatching
        * means that t can potentially have more matched tuples
        *
        * only after all tuples are scanned, Quip will start dealing with tuples in CandidateMatching
        * getNextFromCandidateMatching -> real project operator
        * So before getNextFromCandidateMatching(), there should be no inactive predicates before all tuples have been scanned
        * So actually, project operator has two phases: selfJoin(t) and getNextFromCandidateMatching()
        * they are executed consecutively without overlapping
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException, Exception {
        //filter all tuple t using current active predicates in selfJoin, and add tuples to candidateMatching
        while (child.hasNext()) {
            Tuple t = child.next();
            selfJoin(t);
        }
        //getNext from CandidateMatching
        while(true){
            if(!flagCandidateMatch){
                matching.clear();
                //System.out.println("candidate Matching:");
                for(int i=0;i<candidateMatching.size();i++) {
                    candidateMatchingBits.add(false);//true: this tuple has been filtered away
                    //System.out.println(candidateMatching.get(i));
                }
                getNextFromCandidateMatching();
                //System.out.println("final tuples");
                //for(int i=0;i<matchedTuples.size();i++){
                //    System.out.println(matchedTuples.get(i));
                //}
                candidateMatchIterator = matchedTuples.iterator();
                flagCandidateMatch = true;
            }
            else{
                while(candidateMatchIterator.hasNext()){
                    Tuple finalResult = Project(candidateMatchIterator.next());
                    if(finalResult == null) continue;
                    //System.out.println("in project: " + finalResult);
                    return finalResult;
                }
                return null;
            }
        }
    }

    public Tuple Project(Tuple t){
        Tuple newTuple = new Tuple(td);
        boolean isContainNull = false;
        for (int i = 0; i < td.numFields(); i++) {
            Field value = t.getField(outFieldIds.get(i));
            String attr = t.getTupleDesc().getFieldName(outFieldIds.get(i));
            if(value.isMissing()){
                value = ImputeFactory.Impute(new Attribute(attr),t);
            }
            if(value.isNull()){
                isContainNull = true;
                break;
            }
            newTuple.setField(i, value);
        }
        if(!isContainNull){
            return newTuple;
        }
        else{
            return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }

    public List<String> copyList(List<String> list){
        List<String> l = new ArrayList<>();
        for(int i=0;i<list.size();i++){
            l.add(list.get(i));
        }
        return l;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (this.child!=children[0])
        {
            this.child = children[0];
        }
    }

    public Tuple subTuple(String attribute, Tuple t){
        String first = Schema.getFirstAttribute(attribute);
        int start = t.getTupleDesc().fieldNameToIndex(first);
        int width = Schema.getWidth(attribute);
        TupleDesc subTD = t.getTupleDesc().SubTupleDesc(start, width);
        Tuple subT = new Tuple(subTD);
        subT.setTID(t.getTID());
        //subT.setTID(t.findTID(attribute));
        //subT.setMergeBit(t.isMergeBit());
        subT.setAttribute2TID(t.getAttribute2TID());
        for(int i=0;i<width;i++){
            subT.setField(i, t.getField(i+start));
        }
        return subT;
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
        if(!AggregateOptimization.passVirtualFilter(t)){
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
        *Logic of selfJoin and project, see implementation/note 53
     */
    public void selfJoin(Tuple t) throws Exception{//t must be in the left relation
        matching.clear();

        if(pickedColumn == null){
            //this query does not have join operator, just return this tuple
            //ihe: update temporal value --  this should be put after final project, which needs to be improved later for full pipelineing
            AggregateOptimization.setTemporalValue(t);
            candidateMatching.add(t);
            return ;
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
            //if now a missing value is removed, update relationship graph
            for(int i=0;i<t.getTupleDesc().numFields();i++){
                if(t.getField(i).isMissing()){
                    String attribute = (t.getTupleDesc().getFieldName(i));
                    updateGraph(attribute);
                }
            }
            return ;
        }

        //ihe: update temporal value --  this should be put after final project, which needs to be improved later for full pipelineing
        AggregateOptimization.setTemporalValue(t);
        //System.out.println("print in project: " + AggregateOptimization.temporalMax);

        //if codes go here, t passes the test of all current active predicates
        //pickedColumn is right join attribute
        int index = t.getTupleDesc().fieldNameToIndex(pickedColumn);
        Field pickedValue = t.getField(index);
        if(pickedValue.isMissing()){
            //this tuple must not in hash table before because its right join attribute value, pickedValue, is missing before
            //then update tuples in buffer pool and add this entry to hash table
            pickedValue = ImputeFactory.Impute(new Attribute(pickedColumn),t);
            t.setField(index, pickedValue);
            updateGraph(pickedColumn);
            addEntryInHashTable(pickedColumn, pickedValue, subTuple(pickedColumn, t));
        }
        candidateMatching.add(t);

    }

    public void getNextFromCandidateMatching(){
        while(true){
            String nextColumn = RelationshipGraph.getNextColumn();
            if(pickedColumn == null){
                break;
            }
            for(int i=0;i<candidateMatching.size();i++){
                if(candidateMatchingBits.get(i)) continue;//if this tuple has already been filter away, do not consider anymore
                //use pickedColumn to do self-Join
                Tuple t = candidateMatching.get(i);
                List<String> leftAttributes = RelationshipGraph.getLeftJoinAttribute(pickedColumn);
                boolean flag = false;
                for(int j=0;j<leftAttributes.size();j++){
                    String leftAttribute = leftAttributes.get(j);
                    int index = t.getTupleDesc().fieldNameToIndex(leftAttribute);
                    Field value = t.getField(index);
                    if(value.isMissing()){//this round uses pickedColumn to do self-join, so we need to clean left join attribute
                        value = ImputeFactory.Impute(new Attribute(leftAttribute),t);
                        t.setField(index, value);
                    }
                    if(value.isNull()) continue;
                    Statistics.addJoins(1);
                    t.countImputedJoinBy(1);
                    t.countOuterTupleBy(1);
//                    System.out.println("Print in project, hash tables");
//                    HashTables.print();
                    //System.out.println("print in Project: " + pickedColumn);
                    if(!HashTables.getHashTable(pickedColumn).getHashMap().containsKey(value)){
                        //remove this tuple
                        flag = true;
                        candidateMatchingBits.set(i, true);
                        Buffer.removeTuple(t);
                        break;
                    }
                }
                if(!flag){//if codes go here, meaning tuple t passes pickedColumn, then clean nextcolumn of this tuple
                    if(nextColumn != null){
                        int index = t.getTupleDesc().fieldNameToIndex(nextColumn);
                        Field value = t.getField(index);
                        if(value.isMissing()){
                            //value is right join attribute, so t must not be in hash table before, add it
                            value = ImputeFactory.Impute(new Attribute(nextColumn),t);
                            t.setField(index, value);
                            updateGraph(nextColumn);
                            addEntryInHashTable(nextColumn, value, subTuple(nextColumn, t));
                        }
                    }
                }
            }
            //debugging
            //int IT = ImputeFactory.getImputationTimes();
            //int RN = Statistics.getNumOfRemovedTuples();
            //System.out.println(pickedColumn + " " + nextColumn + " " + IT + " " + RN);
            if(nextColumn == null) break;
            //complete this iteration
            pickedColumn = nextColumn;
        }


        //if the codes are here, merge and update tuples
        for(int i=0;i<candidateMatching.size();i++) {
            if (candidateMatchingBits.get(i)) continue;
            Tuple t = candidateMatching.get(i);
            List<Tuple> tupleMatching = new ArrayList<>();
            tupleMatching.add(t);
            int k = 0;//cursor to current tuple in tupleMatching
            for (int j = 0; j < leftJoinAttributes.size(); j++) {
                String leftJoinAttribute = leftJoinAttributes.get(j);
                while (k < tupleMatching.size()) {
                    Field leftValue = tupleMatching.get(k).getField(tupleMatching.get(k).getTupleDesc().fieldNameToIndex(leftJoinAttribute));
                    if (leftValue.isNull()) {
                        k++;
                        continue;
                    }
                    List<String> activeRightAttributes = RelationshipGraph.findRelatedActiveRightAttributes(leftJoinAttribute);
                    List<Integer> temporalMatch;
                    tupleMatching.get(k).countImputedJoinBy(activeRightAttributes.size());
                    Statistics.addJoins(activeRightAttributes.size());
                    t.countImputedJoinBy(activeRightAttributes.size());
                    t.countOuterTupleBy(activeRightAttributes.size());
                    for (int p = 0; p < activeRightAttributes.size(); p++) {//iterate right join attributes corresponding to current leftJoinAttribute
                        String rightAttribute = activeRightAttributes.get(p);
                        temporalMatch = HashTables.getHashTable(rightAttribute).getHashMap().get(leftValue);
                        int tupleSize = Buffer.getTuple(temporalMatch.get(0)).getTupleDesc().numFields();
                        String firstFieldName = Buffer.getTuple(temporalMatch.get(0)).getTupleDesc().getFieldName(0);
                        int firstFieldIndex = t.getTupleDesc().fieldNameToIndex(firstFieldName);
                        if (firstFieldIndex == -1) {//attributes of right tuple is not included in left tuple
                            continue;//jump to next predicate
                        }
                        for (int q = 0; q < temporalMatch.size(); q++) {//iterate all the matching tuples
                            Tuple tt = new Tuple(tupleMatching.get(k).getTupleDesc(), tupleMatching.get(k).getFields());//tt is a copy of tupleMatching.get(k)
                            tt.copyTidSource(tupleMatching.get(k).getTidSource());
                            tt.setAttribute2TID(tupleMatching.get(k).getAttribute2TID());
                            Tuple tTemp = Buffer.getTuple(temporalMatch.get(q));
                            if (!tt.isTidSource(temporalMatch.get(q))) {//tTemp has not been merged to tt before
                                //merged part is NULL, can be imputed
                                if (tt.getField(firstFieldIndex).isNull()) {
                                    for (int kk = 0; kk < tupleSize; kk++) {
                                        tt.setField(firstFieldIndex + kk, tTemp.getField(kk));
                                    }
                                    tt.mergeTidSource(tt.getTidSource(), tTemp.getTidSource());
                                    tt.mergeAttribute2TID(tt.getAttribute2TID(), tTemp.getAttribute2TID());
                                    tupleMatching.add(tt);
                                } else {
                                    break;//try another activeRightAttribute to merge
                                }
                            }
                        }
                    }
                    k++;
                    break;//one join attribute is done, jump to next
                }
            }

            //System.out.println("final tuples in this round!");
            //add final results to MatchedTuples
            for(int j=0;j<tupleMatching.size();j++){
                Tuple filterT = testFilter(tupleMatching.get(j));
                if(filterT != null){
                    //System.out.println(filterT);
                    matchedTuples.add(filterT);
                }
            }
        }
    }

    public Tuple testFilter(Tuple tt){//true: tuple can be removed
        //iterate all filter predicates
        //this operation should be put in the final step after merging and update because
        //1) in join we only push tuples in right relation which only have missing values in join attribute
        //if a tuple have missing values in other attributes, it will not come out before merging

        for(int k=0;k<Schema.getFilterAttributeNames().size();k++){
            String filterAttribute = Schema.getFilterAttributeNames().get(k);
            int index = tt.getTupleDesc().fieldNameToIndex(filterAttribute);
            if(tt.getField(index).isMissing()){
                Field value = ImputeFactory.Impute(new Attribute(filterAttribute), tt);
                //if satisfy filter predicate, then update the tuple
                List<PredicateUnit> filterPredicates = PredicateSet.getFilterPredicates(filterAttribute);
                //one attribute could have multiple filter predicates
                boolean flag = false;
                for(int p=0;p<filterPredicates.size();p++){
                    if(!value.compare(filterPredicates.get(p).getOp(), filterPredicates.get(p).getOperand())){
                        flag = true;
                        break;
                    }
                }
                if(!flag){
                    tt.setField(index,value);
                }
                else{
                    Buffer.removeTuple(tt);
                    return null;
                }
            }
        }
        return tt;
    }

    public void updateGraph(String attribute){
        RelationshipGraph.getNode(attribute).NumOfNullValuesMinusOne();
        RelationshipGraph.trigger(new Attribute(attribute));
    }

    public void addEntryInHashTable(String attribute, Field value, Tuple t){
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

    public void modifyEntryInHashTable(String attribute, Field value, Tuple t){
        //find tid of raw tuple
        int tid;
        if(t.isMergeBit()){//joined tuple
            tid = t.findTID(attribute);
        }else{//raw tuple
            tid = t.getTID();
        }
        //update buffer pool without touching hash table because tid is same
        Buffer.updateTupleByTID(t, tid);
    }
}
