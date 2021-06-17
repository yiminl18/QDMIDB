package QMIDB;
/*
    This operator takes care of null values to clean remove null values and output the desired tuple
 */
import simpledb.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SmartProject extends Operator {
    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private TupleDesc td;//projected schema
    private List<Integer> outFieldIds;

    private List<Tuple> matching = new ArrayList<>();
    private Iterator<Tuple> matchingResult = null;
    private List<Tuple> candidateMatching = new ArrayList<>();
    private boolean flag = false;

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
                Tuple newTuple = new Tuple(td);
                boolean isContainNull = false;
                for (int i = 0; i < td.numFields(); i++) {
                    Field value = matchTuple.getField(outFieldIds.get(i));
                    if(value.isNull()){
                        isContainNull = true;
                        break;
                    }
                    newTuple.setField(i, value);
                }
                if(!isContainNull){
                    return newTuple;
                }
            }
            flag = false;
        }
        return null;
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

    public void selfJoin(Tuple t) throws Exception{//t must be in the left relation
        matching.clear();

        boolean isSelfJoin = false;
        boolean isAdd = false;

        List<GraphEdge> relatedEdges = new ArrayList<>();

        for (int i = 0; i < td.numFields(); i++) {//iterate all projected missing values
            Field value = t.getField(outFieldIds.get(i));
            if(value.isNull()){
                return ;
            }else{//missing or has value
                if(value.isMissing()){
                    value = ImputeFactory.Impute(value);
                    t.setField(i,value);
                }else{
                    value = t.getField(i);
                }
                //in final project should force to check for each related predicate, not valid
                //because in pipeline implementation, valid predicates may come later
                relatedEdges = RelationshipGraph.findRelatedEdge(td.getFieldName(outFieldIds.get(i)));
                for(int j=0;j<relatedEdges.size();j++){
                    if(!relatedEdges.get(j).isActive() && !isAdd){//if there exists one inactive edge, add this tuple to candidateMatching
                        isAdd = true;
                        candidateMatching.add(t);
                    }
                    isSelfJoin = true;
                    String validPred = relatedEdges.get(j).getEndNode().getAttribute().getAttribute();
                    //HashTables.getHashTable(validPred).print();
                    if(!HashTables.ifExistHashTable(validPred)){throw new Exception("Hashtable NOT Exist!");}
                    else{
                        if(!HashTables.getHashTable(validPred).getHashMap().containsKey(value)){
                            //at least one projected values in this tuple is violated with at least one predicate
                            return ;
                        }
                    }
                }
            }
        }

        matching.add(t);
        if(!isSelfJoin){//selfJoin is not triggered
            return ;
        }
        //System.out.println(matching.get(0));
        //if codes go here, then this tuple should be merged and returned
        for (int j = 0; j < td.numFields(); j++) {
            Field value = t.getField(outFieldIds.get(j));
            relatedEdges = RelationshipGraph.findRelatedEdge(td.getFieldName(outFieldIds.get(j)));

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
                        for(int kk=0;kk<tupleSize;kk++){
                            tt.setField(firstFieldIndex+kk, temporalMatch.get(p).getField(kk));
                        }
                        matching.add(tt);
                    }
                }
            }
        }
    }
}
