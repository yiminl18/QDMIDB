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
    private TupleDesc td;
    private List<Integer> outFieldIds;
    private List<Attribute> attributes = new ArrayList<>();

    List<Tuple> matching;
    private Iterator<Tuple> matchingResult = null;

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
            Tuple t = child.next();
            selfJoin(t);
            if(matching.size() == 0){
                continue;
            }
            matchingResult = matching.iterator();
            while(matchingResult.hasNext()){
                Tuple matchTuple = matchingResult.next();
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
                    return matchTuple;
                }
            }
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

        boolean flag = false; //fast check first

        for (int i = 0; i < td.numFields(); i++) {//iterate all projected missing values
            Field value = t.getField(outFieldIds.get(i));
            if(value.isMissing()){
                //check if there is matching tuples for this missing value
                List<String> validPredicates = RelationshipGraph.findActiveEdge(td.getFieldName(outFieldIds.get(i)));
                for(int j=0;j<validPredicates.size();j++){
                    String validPred = validPredicates.get(j);
                    if(!HashTables.ifExistHashTable(validPred)){throw new Exception("Hashtable NOT Exist!");}
                    else{
                        if(!HashTables.getHashTable(validPred).getHashMap().containsKey(value)){
                            flag = true;
                            break;
                        }
                    }
                }
                if(flag){//this value cannot find any matching tuples
                    break;
                }
            }
        }

        if(flag){//at least one projected values in this tuple is violated with at least one predicate
            return ;
        }

        //if codes go here, then this tuple should be cleaned, merged and returned
        matching.add(t);
        for (int j = 0; j < td.numFields(); j++) {
            Field value = t.getField(outFieldIds.get(j));
            if(value.isMissing()){
                List<String> validPredicates = RelationshipGraph.findActiveEdge(td.getFieldName(outFieldIds.get(j)));

                for(int i=0;i<validPredicates.size();i++){
                    int size = matching.size();
                    for(int p=0;p<size;p++){
                        String validPred = validPredicates.get(i);
                        List<Tuple> temporalMatch = HashTables.getHashTable(validPred).getHashMap().get(value);
                        int tupleSize = temporalMatch.get(0).getTupleDesc().getSize();
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
}
