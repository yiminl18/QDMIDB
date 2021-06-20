package QMIDB;

/*
    * this class implements Attribute for relations
 */
public class Attribute {
    //format should be "tableName.attributeName"
    private String attribute;
    private int schemaWidth;

    //store statistics
    private int cardinality, numOfNullValue;

    public int getCardinality() {
        return cardinality;
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }

    public int getNumOfNullValue() {
        return numOfNullValue;
    }

    public void setNumOfNullValue(int numOfNullValue) {
        this.numOfNullValue = numOfNullValue;
    }

    public int getSchemaWidth() {
        return schemaWidth;
    }

    public void setSchemaWidth(int schemaWidth) {
        this.schemaWidth = schemaWidth;
    }

    public Attribute(String attribute) {
        this.attribute = attribute;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public int getHashCode(){
        return this.attribute.hashCode();
    }

    public String getRelation(){
        return attribute.split("\\.")[0];
    }
}
