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
    private List<Integer> outFieldIds;

    private List<Tuple> matching = new ArrayList<>();
    private Iterator<Tuple> matchingResult, candidateMatchIterator, candidateResult = null;
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
        while (true) {
            if(!flag){
                if(child.hasNext()){
                    Tuple t = child.next();
                    //System.out.println("Project: " + t);
                    selfJoin(t);
                    if(matching.size() == 0){
                        continue;
                    }
                    flag = true;
                    matchingResult = matching.iterator();
                }
                else{
                    break;
                }
            }
            while(matchingResult.hasNext()){
                Tuple matchTuple = matchingResult.next();
                //System.out.println("match tuple: " + matchTuple);
                Tuple newTuple = Project(matchTuple);
                if(newTuple == null){
                    continue;
                }else{
                    return newTuple;
                }
            }
            flag = false;
        }
        //getNext from CandidateMatching
        while(true){
            if(!flagCandidateMatch){
                matching.clear();
                //System.out.println("candidate Mathcing:");
                for(int i=0;i<candidateMatching.size();i++){
                    candidateMatchingBits.add(false);
                    //System.out.println(candidateMatching.get(i));
                }
                getNextFromCandidateMatching();
                /*System.out.println("final tuples");
                for(int i=0;i<matchedTuples.size();i++){
                    System.out.println(matchedTuples.get(i));
                }*/
                candidateMatchIterator = matchedTuples.iterator();
                flagCandidateMatch = true;
            }
            else{
                while(candidateMatchIterator.hasNext()){
                    Tuple finalResult = Project(candidateMatchIterator.next());
                    if(finalResult == null) continue;
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
        for(int i=0;i<width;i++){
            subT.setField(i, t.getField(i+start));
        }
        return subT;
    }

    /*
        *Logic of selfJoin and project, see implementation/note 53
     */
    public void selfJoin(Tuple t) throws Exception{//t must be in the left relation
        matching.clear();

        boolean isSelfJoin = false;
        boolean isAdd = false;

        List<GraphEdge> relatedEdges;

        List<String> leftActiveAttributes = RelationshipGraph.getActiveLeftAttribute();
        List<String> inactiveLeftAttributes = copyList(RelationshipGraph.getLeftJoinAttribute());
        inactiveLeftAttributes.removeAll(leftActiveAttributes);

        //clean attribute value in pickedColumn is missing
        int index = t.getTupleDesc().fieldNameToIndex(pickedColumn);
        Field pickedValue = t.getField(index);
        if(pickedValue.isMissing()){
            pickedValue = ImputeFactory.Impute(new Attribute(pickedColumn),t);
            t.setField(index, pickedValue);
            updateGraph(pickedColumn);
            updateHashTable(pickedColumn, pickedValue, subTuple(pickedColumn, t));
        }

        for (int i = 0; i< t.getTupleDesc().numFields(); i++) {
            Field value = t.getField(i);
            String attribute = t.getTupleDesc().getFieldName(i);

            //if this attribute is in the left attribute of an active join predicate
            if(leftActiveAttributes.contains(attribute) && !value.isNull()){
                isSelfJoin = true;
                if(value.isMissing()){
                    value = ImputeFactory.Impute(new Attribute(attribute),t);//ihe: null should be the corresponding active join predicate
                    t.setField(i, value);
                }
                //check if non-matching
                List<String> rightActiveAttributes = RelationshipGraph.findRelatedActiveRightAttributes(attribute);
                Statistics.addJoins(rightActiveAttributes.size());
                for(int j=0;j<rightActiveAttributes.size();j++){
                    String rightActiveAttribute = rightActiveAttributes.get(j);
                    if(!HashTables.getHashTable(rightActiveAttribute).getHashMap().containsKey(value)){
                        //at least one attribute value in this tuple is violated with at least one predicate
                        return ;
                    }
                }
            }
            //if this attribute is left join attribute and in an inactive predicate, add to candidateMatching
            if(inactiveLeftAttributes.contains(attribute)){
                candidateMatching.add(t);
                return;
            }
        }

        matching.add(t);
        if(!isSelfJoin){//selfJoin is not triggered
            return ;
        }
        //if codes go here, then this tuple should be merged and returned
        for (int j = 0; j< t.getTupleDesc().numFields(); j++) {
            Field value = t.getField(j);
            String attribute = t.getTupleDesc().getFieldName(j);
            relatedEdges = RelationshipGraph.findRelatedEdge(attribute);

            for(int i=0;i<relatedEdges.size();i++){
                int size = matching.size();
                t.countImputedJoinBy(size);
                t.countOuterTupleBy(size);
                Statistics.addJoins(size);
                for(int p=0;p<size;p++){
                    String validPred = relatedEdges.get(i).getEndNode().getAttribute().getAttribute();
                    List<Tuple> temporalMatch = HashTables.getHashTable(validPred).getHashMap().get(value);
                    int tupleSize = temporalMatch.get(0).getTupleDesc().numFields();
                    String firstFieldName = temporalMatch.get(0).getTupleDesc().getFieldName(0);
                    int firstFieldIndex = t.getTupleDesc().fieldNameToIndex(firstFieldName);
                    if(firstFieldIndex == -1){//attributes of right tuple is not included in left tuple
                        break;//jump to next predicate
                    }
                    for(int k=0;k<temporalMatch.size();k++){//iterate all the matching tuples
                        Tuple tt = new Tuple(matching.get(p).getTupleDesc(), matching.get(p).getFields());
                        if(!temporalMatch.get(k).isMergeBit()){
                            for(int kk=0;kk<tupleSize;kk++){
                                tt.setField(firstFieldIndex+kk, temporalMatch.get(k).getField(kk));
                            }
                            temporalMatch.get(k).setMergeBit(true);
                            matching.add(tt);
                        }
                        else{
                            boolean hasNuLL = false;
                            for(int kk=0;kk<tupleSize;kk++){
                                Field rawValue = tt.getField(firstFieldIndex+kk);
                                if(rawValue.isNull()){
                                    hasNuLL = true;
                                    tt.setField(firstFieldIndex+kk, temporalMatch.get(k).getField(kk));
                                }
                            }
                            if(hasNuLL){
                                temporalMatch.get(k).setMergeBit(true);
                                matching.add(tt);
                            }
                        }
                    }
                }
            }
        }
    }

    /*

     */

    public void getNextFromCandidateMatching(){
        while(true){
            String nextColumn = RelationshipGraph.getNextColumn();
            if(nextColumn == null) break;
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
                    if(!HashTables.getHashTable(pickedColumn).getHashMap().containsKey(value)){
                        //remove this tuple
                        flag = true;
                        candidateMatchingBits.set(i, true);
                        break;
                    }
                }
                if(!flag){//if codes go here, then clean nextcolumn of this tuple
                    int index = t.getTupleDesc().fieldNameToIndex(nextColumn);
                    Field value = t.getField(index);
                    if(value.isMissing()){
                        value = ImputeFactory.Impute(new Attribute(nextColumn),t);
                        t.setField(index, value);
                        updateGraph(nextColumn);
                        updateHashTable(nextColumn, value, subTuple(nextColumn, t));
                    }
                }
            }
        }
        //the codes are here, merge and update tuples
        for(int i=0;i<candidateMatching.size();i++){
            if(candidateMatchingBits.get(i)) continue;
            Tuple t = candidateMatching.get(i);

            List<Tuple> tupleMatching = new ArrayList<>();
            tupleMatching.add(t);
            for(int j=0;j<leftJoinAttributes.size();j++){
                String leftJoinAttribute = leftJoinAttributes.get(j);
                for(int k=0;k<tupleMatching.size();k++){
                    Field leftValue = tupleMatching.get(k).getField(tupleMatching.get(k).getTupleDesc().fieldNameToIndex(leftJoinAttribute));
                    if(leftValue.isNull()){
                        continue;
                    }
                    List<String> activeRightAttributes = RelationshipGraph.findRelatedActiveRightAttributes(leftJoinAttribute);
                    List<Tuple> temporalMatch;
                    tupleMatching.get(k).countImputedJoinBy(activeRightAttributes.size());
                    Statistics.addJoins(activeRightAttributes.size());
                    t.countImputedJoinBy(activeRightAttributes.size());
                    t.countOuterTupleBy(activeRightAttributes.size());
                    for(int p=0;p<activeRightAttributes.size();p++){
                        String rightAttribute = activeRightAttributes.get(p);
                        temporalMatch = HashTables.getHashTable(rightAttribute).getHashMap().get(leftValue);
                        int tupleSize = temporalMatch.get(0).getTupleDesc().numFields();
                        String firstFieldName = temporalMatch.get(0).getTupleDesc().getFieldName(0);
                        int firstFieldIndex = t.getTupleDesc().fieldNameToIndex(firstFieldName);
                        if(firstFieldIndex == -1){//attributes of right tuple is not included in left tuple
                            break;//jump to next predicate
                        }
                        for(int q=0;q<temporalMatch.size();q++){//iterate all the matching tuples
                            Tuple tt = new Tuple(tupleMatching.get(k).getTupleDesc(), tupleMatching.get(k).getFields());
                            if(!temporalMatch.get(q).isMergeBit()){
                                for(int kk=0;kk<tupleSize;kk++){
                                    tt.setField(firstFieldIndex+kk, temporalMatch.get(q).getField(kk));
                                }
                                temporalMatch.get(q).setMergeBit(true);
                                tupleMatching.add(tt);
                            }
                            else{
                                boolean hasNull = false;
                                for(int kk=0;kk<tupleSize;kk++){
                                    Field tempValue = temporalMatch.get(q).getField(kk);
                                    Field rawValue = tt.getField(firstFieldIndex+kk);
                                    if(rawValue.isNull()){
                                        hasNull = true;
                                        tt.setField(firstFieldIndex+kk, tempValue);
                                    }
                                }
                                if(hasNull){
                                    temporalMatch.get(q).setMergeBit(true);
                                    tupleMatching.add(tt);
                                }
                            }
                        }
                    }
                }
            }
            //add final results to MatchedTuples
            for(int j=0;j<tupleMatching.size();j++){
                matchedTuples.add(tupleMatching.get(j));
            }
        }
    }

    public void updateGraph(String attribute){
        RelationshipGraph.getNode(attribute).NumOfNullValuesMinusOne();
        RelationshipGraph.trigger(new Attribute(attribute));
    }

    public void updateHashTable(String attribute, Field value, Tuple t){
        if(!HashTables.ifExistHashTable(attribute)){
            HashMap<Field, List<Tuple>> hashMap = new HashMap<>();
            hashMap.put(value, new ArrayList<>());
            hashMap.get(value).add(t);
            HashTables.addHashTable(attribute, new HashTable(attribute, hashMap));
        }
        else{
            if(!HashTables.getHashTable(attribute).hasKey(value)){
                HashTables.getHashTable(attribute).getHashMap().put(value, new ArrayList<>());
            }
            HashTables.getHashTable(attribute).getHashMap().get(value).add(t);
        }
    }
}
