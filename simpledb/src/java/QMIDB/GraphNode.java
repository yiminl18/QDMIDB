package QMIDB;

/*
    *This class implements graph node in relationship graph
 */
public class GraphNode {
    private int cardinality;
    private int numOfNullValues;
    private Attribute attribute;
    private int id;//integer index of node, attribute

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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
}
