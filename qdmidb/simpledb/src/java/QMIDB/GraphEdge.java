package QMIDB;

/*
    * this class implements the edge in relationship graph
 */
public class GraphEdge {
    private int edgeType;//0 - join edge; 1- otherwise
    private boolean usedBit = false;//if the operator associated with this edge has been applied to trigger self-join imputation
    private GraphNode startNode,endNode;//start node and end node

    public GraphEdge(int edgeType, GraphNode startNode, GraphNode endNode) {
        this.edgeType = edgeType;
        this.startNode = startNode;
        this.endNode = endNode;
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



    public GraphEdge(int edgeType, boolean usedBit) {
        this.edgeType = edgeType;
        this.usedBit = usedBit;
    }

    public int getEdgeType() {
        return edgeType;
    }

    public void setEdgeType(int edgeType) {
        this.edgeType = edgeType;
    }

    public boolean isUsedBit() {
        return usedBit;
    }

    public void setUsedBit(boolean usedBit) {
        this.usedBit = usedBit;
    }
}
