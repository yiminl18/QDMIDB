package QMIDB;

/*
    * this class implements Attribute/columns for relations
    * also store some statistics, such as cardinality, number of Null values, number of attributes in this relation
    * attribute class cannot be static, should be the member of predicateUnit, which should be static
 */
public class Attribute {
    //format should be "tableName.attributeName"
    private String attribute;
    private boolean isLeft;//is this attribute in join attribute
    private int schemaWidth;//number of attributes in this relation

    //store statistics
    private int cardinality, numOfNullValue;

    //numOfJoinForMissing: number of joins for those tuples which have missing values in this attribute
    //numOfMissingSoFar: number of missing values in this attribute seen so far
    //numOfImputed: number of imputed values in this attribute so far
    private int numOfJoinForMissing=0, numOfMissingSoFar=0, numOfImputed=0;

    //Prob: probability that a value in this attribute will be imputed in query processing

    public void incrementNumOfJoinForMissing(){
        numOfJoinForMissing += 1;
    }

    public void incrementNumOfMissingSoFar(){
        numOfMissingSoFar += 1;
    }

    public void incrementNumOfImputed(){
        numOfImputed += 1;
    }

    public double getProb(){
        return Double.valueOf(numOfImputed)/Double.valueOf(numOfMissingSoFar);
    }

    public int getNumOfImputed() { return numOfImputed; }

    public int getNumOfJoinTest() {
        return numOfJoinForMissing;
    }

    public int getNumOfMissingSoFar() {
        return numOfMissingSoFar;
    }

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
