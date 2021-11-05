package QMIDB;

/*
    * this class implements Attribute/columns for relations
    * also store some statistics, such as cardinality, number of Null values, number of attributes in this relation
    * attribute class cannot be static, should be the member of predicateUnit, which should be static
 */
public class Attribute {
    //format should be "tableName.attributeName"
    private String attribute;
    //in join predicate, missing values in right attribute will cause outer joins in left relation
    private boolean isRight = false;//is this attribute in join attribute
    private int schemaWidth;//number of attributes in this relation

    //store statistics
    private int cardinality, numOfNullValue;

    //numOfJoinForMissing: number of joins for those tuples which have missing values in this attribute
    //numOfMissingSoFar: number of missing values in this attribute seen so far
    //numOfImputed: number of imputed values in this attribute so far
    //numOfOuterJoinTest: number of join tests for outer join tuples due to missing values in this attribute
    //numOfImputedJoin: number of join tests for tuples containing imputed value in this attribute
    private double numOfJoinForMissing=0, numOfMissingSoFar=0, numOfImputed=0, numOfOuterJoinTest = 0, numOfImputedJoin = 0;

    //Prob: probability that a value in this attribute will be imputed in query processing
    private double Prob;
    private double evaluateVc, evaluateVd;

    public boolean isRight() {
        return isRight;
    }

    public void setRight(boolean right) {
        isRight = right;
    }


    public void addNumOfImputedJoinBy(int offset) {numOfImputedJoin += offset;}

    public void addNumOfJoinForMissingBy(int offset) {numOfJoinForMissing += offset;}

    public void addNumOfOuterJoinTestBy(int offset) {numOfOuterJoinTest += offset;}

    public void incrementNumOfMissingSoFar(){
        numOfMissingSoFar += 1;
    }

    public void incrementNumOfImputed(){
        numOfImputed += 1;
    }

    public void addNumOfImputedBy(int offset) {numOfImputed += offset;}

    public double getProb(){
        if(numOfMissingSoFar == 0){
            return -1;
        }
        this.Prob = numOfImputed/numOfMissingSoFar;
        return this.Prob;
    }

    public double getEvaluateVc(){//on average, the number of join tests for vc
        if(numOfImputed == 0){
            return -1;
        }
        this.evaluateVc = numOfImputedJoin*Statistics.getTimeOneJoin()/numOfImputed;
        return this.evaluateVc;
    }

    public double getEvaluateVd(){
        if(numOfMissingSoFar == 0){
            return -1;
        }
        this.evaluateVd = numOfJoinForMissing;
        if(this.isRight){
            this.evaluateVd += numOfOuterJoinTest;
        }
        this.evaluateVd = this.evaluateVd*Statistics.getTimeOneJoin()/numOfMissingSoFar;
        return this.evaluateVd;
    }

    public boolean getDecision(){
        //if this attribute is dead, clean now
        if(Statistics.isDead(attribute)){
            System.out.println(attribute + " is dead!");
            return true;
        }
        this.Prob = getProb();
        this.evaluateVd = getEvaluateVd();
        this.evaluateVc = getEvaluateVc();
        double imputeCost = ImputeFactory.getEstimateTime();

        //when statistics is not enough to make decision, always delay
        if(this.Prob == -1 || this.evaluateVd == -1 || this.evaluateVc == -1){
            System.out.println("Statistics not enough! Delay imputation!");
            return false;
        }
        double expectedClean = imputeCost + this.evaluateVc;
        double expectedDelay = this.Prob*(imputeCost+this.evaluateVd)+(1-this.Prob)*this.evaluateVd;
        System.out.println("clean vs delay expected cost: " + expectedClean + " " + expectedDelay);
        if(expectedClean > expectedDelay){
            return false;
        }
        else{
            return true;
        }
    }

    public double getNumOfImputed() { return numOfImputed; }

    public double getNumOfJoinTest() {
        return numOfJoinForMissing;
    }

    public double getNumOfMissingSoFar() {
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

    public void print(){
        System.out.println(attribute);
        System.out.println("numOfJoinForMissing:" + numOfJoinForMissing);
        System.out.println("numOfMissingSoFar:" + numOfMissingSoFar);
        System.out.println("numOfImputed:" + numOfImputed);
        System.out.println("numOfOuterJoinTest:" + numOfOuterJoinTest);
        System.out.println("numOfImputedJoin:" + numOfImputedJoin);
        System.out.println("Prob:" + getProb());
        System.out.println("getEvaluateVc:" + getEvaluateVc());
        System.out.println("getEvaluateVd:" + getEvaluateVd());
        System.out.println("Imputation Decision: " + getDecision());
    }
}
