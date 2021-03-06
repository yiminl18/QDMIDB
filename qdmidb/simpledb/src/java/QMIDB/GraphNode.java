package QMIDB;

/*
    *This class implements graph node in relationship graph
 */
public class GraphNode {
    private int cardinality;
    private int numOfNullValues;
    private Attribute attribute;
    private boolean isPicked = false;

    public boolean isPicked() {
        return isPicked;
    }

    public void setPicked(boolean picked) {
        isPicked = picked;
    }

    public GraphNode(Attribute attribute) {
        this.attribute = attribute;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public void setAttribute(Attribute attribute) {
        this.attribute = attribute;
    }

    public int getCardinality() {
        return cardinality;
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }

    public int getNumOfNullValues() {
        return numOfNullValues;
    }

    public void setNumOfNullValues(int numOfNullValues) {
        this.numOfNullValues = numOfNullValues;
    }

    public boolean NumOfNullValuesMinusOne(){
        this.numOfNullValues = this.numOfNullValues -1;
        if(this.numOfNullValues <= 0){
            return true;
        }
        return false;
    }
}
