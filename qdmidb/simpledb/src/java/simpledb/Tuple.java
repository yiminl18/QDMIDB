package simpledb;

import QMIDB.Statistics;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private Field[] fields;
    private List<String> outerAttribute = new ArrayList<>();//size > 0 -> this tuple is outer join tuple due to the attributes in outerAttribute
    private int TID;//ID of this tuple in buffer pool
    private TupleDesc schema;
    private RecordId rid;
    private List<Integer> PAfield;//store the fields of attributes in predicate
    private List<String> imputedField = new ArrayList<>();//store the name of attribute whose value is imputed in this tuple
    private boolean mergeBit = false;//this bit is used to indicate if this tuple has been already merged in any join operator
    //applied_bit is used to store those attributes that have been already applied for testing
    //these attributes are either left attribute in join or the one in filter
    private HashMap<String, Boolean> applied_bit = new HashMap<>();
    private HashMap<String, Integer> attribute2TID = new HashMap<>();//from attribute name to its TID

    public HashMap<String, Integer> getAttribute2TID(){
        return this.attribute2TID;
    }

    public void mergeAttribute2TID(HashMap<String, Integer> mp1, HashMap<String, Integer> mp2){
        if(this.mergeBit){//to prevent operations for outer join tuple
            //only deal with matched join tuple
            //note that keys in mp1 and mp2 must be different because they are joined from two non-overlapping relation set
            //thus self join will not be considered for now
            this.attribute2TID = mp1;
            this.attribute2TID.putAll(mp2);
        }
    }

    public void setAttribute2TID(HashMap<String, Integer> attribute2TID){
        this.attribute2TID = attribute2TID;
    }

    public int findTID(String attribute){
        if(attribute2TID.containsKey(attribute)){
            return attribute2TID.get(attribute);
        }else{
            return -1;
        }
    }

    public int getTID(){
        return this.TID;
    }

    public void setTID(int TID){
        this.TID = TID;
    }

    public void setApplied_Bit(String attribute){
        if(!applied_bit.containsKey(attribute)){
            applied_bit.put(attribute, true);
        }
    }

    public boolean getApplied_bit(String attribute){
        return applied_bit.containsKey(attribute);
    }

    public void addOuterAttribute(String attribute){
        if(outerAttribute.indexOf(attribute) == -1){
            outerAttribute.add(attribute);
        }
    }

    public void setPAfield(List<Integer> PAfield){
        this.PAfield = PAfield;
    }

    public boolean isMergeBit() {
        return mergeBit;
    }

    public void setMergeBit(boolean mergeBit) {
        this.mergeBit = mergeBit;
    }

    public void addImputedField(String attribute){//update one time in imputation function
        imputedField.add(attribute);
    }

    public void countImputedJoinBy(int offset){//add the number of join test for imputed values by offset
        for(int i=0;i<imputedField.size();i++){
            Statistics.getAttribute(imputedField.get(i)).addNumOfImputedJoinBy(offset);
        }
    }

    public void countMissingValueBy(int offset){//add the number of join test for missing values by offset
        for(int i=0;i<PAfield.size();i++){
            String fieldName = schema.getFieldName(PAfield.get(i));
            if(fields[PAfield.get(i)].isMissing()){
                Statistics.getAttribute(fieldName).addNumOfJoinForMissingBy(offset);
            }
        }
    }

    public void countOuterTupleBy(int offset){
        for(int i=0;i<outerAttribute.size();i++){
            Statistics.getAttribute(outerAttribute.get(i)).addNumOfOuterJoinTestBy(offset);
        }
    }

    /**
     * Create a new tuple with the specified schema (type).
     * 
     *
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        schema = td;
        fields = new Field[schema.numFields()];
    }
    
    public Tuple(TupleDesc td, Field[] fields) {
        if (td.numFields() != fields.length) {
            throw new IllegalArgumentException("Schema does not match fields.");
        }
        schema = td;
        this.fields = new Field[fields.length];
        for(int i=0;i<fields.length;i++){
            this.fields[i] = fields[i];
        }
    }
    
    /**
     * Copy constructor
     * @param t
     */
    public Tuple(Tuple t){
        this(t.schema, Arrays.copyOf(t.fields, t.fields.length));
    }
    
    /**
     * Create a new tuple which is the concatenation of two existing tuples.
     */
    public Tuple(Tuple t1, Tuple t2) {
        schema = TupleDesc.merge(t1.schema, t2.schema);
        fields = new Field[schema.numFields()];
        System.arraycopy(t1.fields, 0, fields, 0, t1.fields.length);
        System.arraycopy(t2.fields, 0, fields, t1.fields.length, t2.fields.length);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return schema;
    }

    public Field[] getFields() { return this.fields; }


    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            sb.append(fields[i].toString());
            if (i < fields.length - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return Arrays.asList(fields).iterator();
    }
    
    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td) {
        schema = td;
    }

    /**
     * True if tuple has any missing fields
     * @return
     */
    public boolean hasMissingFields() {
        for(Field field : fields) {
            if (field.isMissing()) {
                return true;
            }
        }
        return false;
    }

    public boolean hasMissingFieldInPredicateAttribute(){
        for(int i=0;i<fields.length;i++){
            if(fields[i].isMissing() && Statistics.isPredicateAttribute(schema.getFieldName(i))){
                return true;
            }
        }
        return false;
    }
    
    /**
     * Check if any of the fields specified by `fields` are mising
     * @param fields list of fields to check if missing
     * @return
     */
    public boolean hasMissingFieldsIndices(Collection<Integer> fields){
        for (int i : fields){
            if (getField(i).isMissing()){
                return false;
            }
        }
        
        return true;
    }

    /**
     * Check if any of the fields specified by `fields` are mising
     * @param dropFields list of fields to check if missing
     * @return
     */
    public boolean hasMissingFields(Collection<String> dropFields){
        for (String name : dropFields){
            if (getField(getTupleDesc().fieldNameToIndex(name)).isMissing()){
                return true;
            }
        }
        
        return false;
    }

    /**
     * Returns list of missing field indices
     * @return
     */
    public List<Integer> missingFieldsIndices() {
        List<Integer> missing = new ArrayList<>();
        for(int i = 0; i < getTupleDesc().numFields(); i++) {
            if (getField(i).isMissing()) {
                missing.add(i);
            }
        }
        return missing;
    }


    public double error(Tuple t) throws BadErrorException {
		if (!t.schema.equals(schema)) {
			throw new IllegalArgumentException("Tuples have different schemas.");
		}
		try {
			double err = 0.0;
			for (int i = 0; i < fields.length; i++) {
				if (fields[i] instanceof IntField) {
					IntField f1 = (IntField) fields[i], f2 = (IntField) t.fields[i];
					err += Math.pow(f1.getValue() - f2.getValue(), 2);
				} else if (fields[i] instanceof DoubleField) {
					DoubleField f1 = (DoubleField) fields[i], f2 = (DoubleField) t.fields[i];
					err += Math.pow(f1.getValue() - f2.getValue(), 2);
				}
			}
			return err;
		} catch (UnsupportedOperationException e) {
			throw new BadErrorException();
		}
    }

}
