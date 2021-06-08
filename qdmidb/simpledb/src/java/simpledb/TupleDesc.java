package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable, Iterable<TDItem> {
    //private final TDItem[] schema;
    private List<TDItem> schema;
    private int size;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return schema.iterator();
    }

    public int getField(TDItem item){
        return schema.indexOf(item);
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	if (typeAr.length != fieldAr.length) {
    		throw new IllegalArgumentException("typeAr.length != fieldAr.length");
    	}
    	int len = typeAr.length;
    	schema = new ArrayList<>();
    	for (int i = 0; i < len; i++) {
    		schema.add(new TDItem(typeAr[i], fieldAr[i]));
    	}
    	
    	int s = 0;
        for (TDItem i : schema) {
        	s += i.fieldType.length;
        }
        size = s;
    }


    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	int len = typeAr.length;
    	schema = new ArrayList<>();
    	for (int i = 0; i < len; i++) {
    		schema.add(new TDItem(typeAr[i], null));
    	}
    	
    	int s = 0;
        for (TDItem i : schema) {
        	s += i.fieldType.length;
        }
        size = s;
    }
    
    public TupleDesc(TupleDesc td, String prefix) {
    	schema = new ArrayList<>();
    	for (int i = 0; i < td.schema.size(); i++) {
    		schema.add(new TDItem(td.schema.get(i).fieldType, prefix + "." + td.schema.get(i).fieldName));
    	}
    	
    	int s = 0;
        for (TDItem i : schema) {
        	s += i.fieldType.length;
        }
        size = s;
    }

    public TupleDesc(TupleDesc td){
        schema = td.schema;
        size = td.size;
    }

    public TupleDesc SubTupleDesc(int start, int numOfAttribute){
        return new TupleDesc(this.schema.subList(start, start + numOfAttribute));
    }


    /**
     * Constructor. Create a new tuple desc directly from a schema list.
     */
    private TupleDesc(List<TDItem> schema) {
    	this.schema = schema;
    	
    	int s = 0;
        for (TDItem i : schema) {
        	s += i.fieldType.length;
        }
        size = s;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return schema.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= numFields()) {
        	throw new NoSuchElementException();
        }
        String name = schema.get(i).fieldName;
        return name == null ? "null" : name;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if (i < 0 || i >= numFields()) {
        	throw new NoSuchElementException();
        }
        return schema.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	if (name == null) {
    		throw new NoSuchElementException();
    	}
        int index = 0;
    	int flag = -1;
        for (TDItem i : schema) {
        	if (name.equals(i.fieldName)) {
        	    flag = 0;
        		return index;
        	}
        	index++;
        }
        return flag;
    }

    public void print(){
        for(TDItem i : schema){
            System.out.println(i.fieldName);
        }
    }
    
    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(QualifiedName name) throws NoSuchElementException {
    	return fieldNameToIndex(name.toString());
    }

    public Collection<Integer> fieldNamesToIndices(Collection<QualifiedName> names) throws NoSuchElementException {
        List<Integer> indices = new ArrayList<>();
        for(QualifiedName name : names) {
            indices.add(fieldNameToIndex(name));
        }
        return indices;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        List<TDItem> s1 = td1.schema,
        		 s2 = td2.schema, 
        		 s = new ArrayList<>();
        int j = 0;
        for (int i = 0; i < s1.size(); i++) {
        	s.add(s1.get(i));
        }
        for (int i = 0; i < s2.size(); i++) {
        	s.add(s2.get(i));
        }
        return new TupleDesc(s);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     *             Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		TupleDesc other = (TupleDesc) obj;
		if (schema.size() != other.schema.size()) {
			return false;
		}
		for (int i = 0; i < schema.size(); i++) {
			Type t1 = schema.get(i).fieldType, t2 = other.schema.get(i).fieldType;
			if (!t1.equals(t2)) {
				return false;
			}
		}
		return true;
	}
	
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

	/**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	for (int i = 0; i < schema.size(); i++) {
    		TDItem td = schema.get(i);
    		sb.append(String.format("%s[%d](%s[%d])", td.fieldType, i, td.fieldName, i));
    		if (i < schema.size() - 1) {
    			sb.append(", ");
    		}
    	}
    	return sb.toString();
    }

	public boolean containsField(String fname) {
		for (TDItem td : schema) {
			if (td.fieldName.equals(fname)) {
				return true;
			}
		}
		return false;
	}
}
