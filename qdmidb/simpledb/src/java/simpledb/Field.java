package simpledb;

import java.io.*;

/**
 * Interface for values of fields in tuples in SimpleDB.
 */
public interface Field extends Serializable{
    /**
     * Write the bytes representing this field to the specified
     * DataOutputStream.
     * @see DataOutputStream
     * @param dos The DataOutputStream to write to.
     */
    void serialize(DataOutputStream dos) throws IOException;

    /**
     * Compare the value of this field object to the passed in value.
     * @param op The operator
     * @param value The value to compare this Field to
     * @return Whether or not the comparison yields true.
     */
    public boolean compare(Predicate.Op op, Field value);

    /**
     * Returns the type of this field (see {@link Type#INT_TYPE} or {@link Type#STRING_TYPE}
     * @return type of this field
     */
    public Type getType();
    
    /**
     * Hash code.
     * Different Field objects representing the same value should probably
     * return the same hashCode.
     */
    public int hashCode();
    public boolean equals(Object field);

    public boolean isMissing();

    public boolean isNull();

    public String toString();

    static boolean areEqual(Field fo, Field fi) {
        boolean result = false;
        if (fo != null && fi != null){
            if (fo instanceof IntField && fi instanceof IntField){
                result = ((IntField) fo).equals((IntField) fi);
            } else if (fo instanceof DoubleField && fi instanceof DoubleField){
                result = ((DoubleField) fo).equals((DoubleField) fi);
            } else if  (fo instanceof StringField && fi instanceof StringField){
                result = ((StringField) fo).equals((StringField) fi);
            }
        }
        return result;
    }

}
