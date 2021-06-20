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
    private List<Tuple> candidateMatching = new ArrayList<>();
    private boolean flag, flagCandidateMatch = false;
    Tuple candidateT = null;

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
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException, Exception {
        while (child.hasNext()) {
            if(!flag){
                Tuple t = child.next();
                selfJoin(t);
                if(matching.size() == 0){
                    continue;
                }
                flag = true;
                matchingResult = matching.iterator();
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
        return getNextFromCandidateMatching();
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

    @Override
    public void setChildren(DbIterator[] children) {
        if (this.child!=children[0])
        {
            this.child = children[0];
        }
    }

    public Tuple subTuple(String attribute, Tuple t, int start){
        int width = Schema.getWidth(attribute);
        TupleDesc subTD = t.getTupleDesc().SubTupleDesc(start, width);
        Tuple subT = new Tuple(subTD);
        for(int i=0;i<width;i++){
            subT.setField(i, t.getField(i+start));
        }
        return subT;
    }

    public void selfJoin(Tuple t) throws Exception{//t must be in the left relation
        matching.clear();

        boolean isSelfJoin = false;
        boolean isAdd = false;

        List<GraphEdge> relatedEdges = new ArrayList<>();

        List<String> leftActiveAttributes = RelationshipGraph.getActiveLeftAttribute();
        List<String> rightAttributes = RelationshipGraph.getRightJoinAttribute();
        List<String> leftAttributes = RelationshipGraph.getLeftJoinAttribute();
        List<String> inactiveLeftAttributes = leftAttributes;
        inactiveLeftAttributes.removeAll(leftActiveAttributes);

        for (int i = 0; i< t.getTupleDesc().numFields(); i++) {
            Field value = t.getField(i);
            String attribute = t.getTupleDesc().getFieldName(i);
            //if this attribute is in the left attribute of an active join predicate
            if(leftActiveAttributes.contains(attribute)){
                isSelfJoin = true;
                if(value.isMissing()){
                    value = ImputeFactory.Impute(value);
                    t.setField(i, value);
                }
                //check if non-matching
                List<String> rightActiveAttributes = RelationshipGraph.findRelatedActiveRightAttributes(attribute);
                for(int j=0;j<rightActiveAttributes.size();j++){
                    String rightActiveAttribute = rightActiveAttributes.get(j);
                    if(!HashTables.getHashTable(rightActiveAttribute).getHashMap().containsKey(value)){
                        //at least one attribute value in this tuple is violated with at least one predicate
                        return ;
                    }
                }
            }
            //if this attribute is in the right attribute of any join predicate
            if(rightAttributes.contains(attribute)){
                if(value.isMissing()){
                    value = ImputeFactory.Impute(value);
                    t.setField(i, value);
                    //update graph
                    RelationshipGraph.getNode(attribute).NumOfNullValuesMinusOne();
                    RelationshipGraph.trigger(new Attribute(attribute));
                    //update hashTables
                    if(!HashTables.ifExistHashTable(attribute)){
                        HashMap<Field, List<Tuple>> hashMap = new HashMap<>();
                        hashMap.put(value, new ArrayList<>());
                        hashMap.get(value).add(subTuple(attribute,t,i));
                        HashTables.addHashTable(attribute, new HashTable(attribute, hashMap));
                    }else{
                        if(!HashTables.getHashTable(attribute).hasKey(value)){
                            HashTables.getHashTable(attribute).getHashMap().put(value, new ArrayList<>());
                        }
                        HashTables.getHashTable(attribute).getHashMap().get(value).add(subTuple(attribute,t,i));
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
                        Tuple tt = matching.get(p);
                        if(temporalMatch.get(k).isMergeBit()) continue;
                        for(int kk=0;kk<tupleSize;kk++){
                            tt.setField(firstFieldIndex+kk, temporalMatch.get(k).getField(kk));
                        }
                        temporalMatch.get(k).setMergeBit(true);
                        matching.add(tt);
                    }
                }
            }
        }
    }

    public Tuple getNextFromCandidateMatching(){
        if(!flagCandidateMatch){
            candidateMatchIterator = candidateMatching.iterator();
            flagCandidateMatch = true;
            matching.clear();//reuse matching
        }
        boolean matchBit = false;
        while(true){
            if(candidateT == null){
                //get next tuple from candidateMatching
                if(candidateMatchIterator.hasNext()){
                    candidateT = candidateMatchIterator.next();
                }else{
                    return null;
                }
                //construct all matching tuples for candidateT
                if(matching.size() == 0) {
                    //all predicates should be active now
                    //fast check
                    boolean isConflicted = false;
                    for (int i = 0; i < candidateT.getTupleDesc().numFields(); i++) {
                        Field value = candidateT.getField(i);
                        String attribute = candidateT.getTupleDesc().getFieldName(i);
                        List<GraphEdge> activeEdges = RelationshipGraph.findRelatedEdge(attribute);
                        for (int j = 0; j < activeEdges.size(); j++) {
                            String rightAttribute = activeEdges.get(j).getEndNode().getAttribute().getAttribute();
                            if (!HashTables.getHashTable(rightAttribute).getHashMap().containsKey(value)) {
                                isConflicted = true;
                                break;
                            }
                        }
                        if (isConflicted) {
                            break;
                        }
                    }
                    if (isConflicted) {
                        candidateT = null;
                        continue;
                    }
                    //merge and update
                    matching.add(candidateT);
                    for (int j = 0; j < candidateT.getTupleDesc().numFields(); j++) {
                        Field value = candidateT.getField(j);
                        String attribute = candidateT.getTupleDesc().getFieldName(j);
                        List<GraphEdge> relatedEdges = RelationshipGraph.findRelatedEdge(attribute);

                        for (int i = 0; i < relatedEdges.size(); i++) {
                            int size = matching.size();
                            for (int p = 0; p < size; p++) {
                                String validPred = relatedEdges.get(i).getEndNode().getAttribute().getAttribute();
                                List<Tuple> temporalMatch = HashTables.getHashTable(validPred).getHashMap().get(value);
                                int tupleSize = temporalMatch.get(0).getTupleDesc().numFields();
                                String firstFieldName = temporalMatch.get(0).getTupleDesc().getFieldName(0);
                                int firstFieldIndex = candidateT.getTupleDesc().fieldNameToIndex(firstFieldName);
                                if (firstFieldIndex == -1) {//attributes of right tuple is not included in left tuple
                                    break;//jump to next predicate
                                }
                                for (int k = 0; k < temporalMatch.size(); k++) {//iterate all the matching tuples
                                    Tuple tt = matching.get(p);
                                    for (int kk = 0; kk < tupleSize; kk++) {
                                        tt.setField(firstFieldIndex + kk, temporalMatch.get(p).getField(kk));
                                    }
                                    matching.add(tt);
                                }
                            }
                        }
                        candidateResult = matching.iterator();
                    }
                }
            }
            //return matching tuples
            if(candidateResult != null){
                while(candidateResult.hasNext()){
                    Tuple tt = candidateResult.next();
                    Tuple ProjectTuple = Project(tt);
                    if(ProjectTuple == null) {
                        continue;
                    }else{
                        return ProjectTuple;
                    }
                }
                candidateT = null;
            }
        }
    }
}
