package QMIDB;

import simpledb.JoinPredicate;

/*
    * this class implements the edge in relationship graph
 */
public class GraphEdge {
    private int edgeType;//0 - join edge; 1- otherwise
    private boolean active = false;//if the operator associated with this edge has been applied to trigger self-join imputation
    private GraphNode startNode,endNode;//start node and end node
    private JoinPredicate joinPredicate;
    private String hashCode;

    public GraphEdge(int edgeType, GraphNode startNode, GraphNode endNode, JoinPredicate joinPredicate) {
        this.edgeType = edgeType;
        this.startNode = startNode;
        this.endNode = endNode;
        this.joinPredicate = joinPredicate;
        this.hashCode = this.startNode.getAttribute().getAttribute() + this.endNode.getAttribute().getAttribute();
    }

    public GraphEdge(GraphNode startNode, GraphNode endNode){
        this.startNode = startNode;
        this.endNode = endNode;
    }

    public String getHashCode(){
        return this.hashCode;
    }

    public JoinPredicate getJoinPredicate(){
        return this.joinPredicate;
    }

    public GraphNode getStartNode() {
        return startNode;
    }

    public void setStartNode(GraphNode startNode) {
        this.startNode = startNode;
    }

    public GraphNode getEndNode() {
        return endNode;
    }

    public void setEndNode(GraphNode endNode) {
        this.endNode = endNode;
    }

    public GraphEdge(int edgeType, boolean active) {
        this.edgeType = edgeType;
        this.active = active;
    }

    public int getEdgeType() {
        return edgeType;
    }

    public void setEdgeType(int edgeType) {
        this.edgeType = edgeType;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive() {
        this.active = true;
    }
}
