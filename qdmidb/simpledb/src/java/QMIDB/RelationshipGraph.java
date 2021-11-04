package QMIDB;
import org.w3c.dom.Attr;
import simpledb.*;
import weka.gui.treevisualizer.Edge;

import java.util.ArrayList;
import java.util.*;
import java.util.List;

/*
    *Relationship graph is used to maintain the status of imputation in query processing
    *It takes input of predicates set of query to initialize the graph
 */
public class RelationshipGraph {
    private static Map<String, List<String>> adjNodes = new HashMap<>();//from right to left attribute in join predicates
    private static Map<String, GraphNode> nodeMap = new HashMap<>();//mapping from String attribute to graph node
    private static Map<String, List<String>> nonJoinNeighbors = new HashMap<>();//mapping from attribute to its non-join neighbors
    private static List<GraphNode> NodeSet = new ArrayList<>();
    private static List<GraphEdge> EdgeSet = new ArrayList<>();
    private static List<String> activeLeftAttribute = new ArrayList<>(), activeRightAttribute = new ArrayList<>(), leftAttribute = new ArrayList<>(), rightAttribute = new ArrayList<>();
    private static List<PredicateUnit> preds;

    public static void initGraph(List<Attribute> Attributes, List<PredicateUnit> Preds) {
        preds = Preds;
        //initialize nodes
        for(int i=0;i<Attributes.size();i++){
            GraphNode node = new GraphNode(Attributes.get(i));
            node.setCardinality(Attributes.get(i).getCardinality());
            node.setNumOfNullValues(Attributes.get(i).getNumOfNullValue());
            NodeSet.add(node);
            nodeMap.put(Attributes.get(i).getAttribute(),node);
        }
        //initialize join-edges
        //join-edges come with order: left and right
        for(int i=0;i<preds.size();i++){
            if(preds.get(i).getIsJoin()){
                GraphEdge edge = new GraphEdge(0,new GraphNode(preds.get(i).getLeft()), new GraphNode(preds.get(i).getRight()), preds.get(i).transform());
                EdgeSet.add(edge);
                addEdge(edge);
                rightAttribute.add(preds.get(i).getRight().getAttribute());
                leftAttribute.add(preds.get(i).getLeft().getAttribute());
            }
        }
        //initialize non-join-edges
        for(int i=0;i<Attributes.size();i++){
            for(int j = i+1; j<Attributes.size(); j++){
                String left = Attributes.get(i).getAttribute();
                String right = Attributes.get(j).getAttribute();
                if(Attributes.get(i).getRelation().equals(Attributes.get(j).getRelation())){//if in same relation
                    GraphEdge edge = new GraphEdge(1, new GraphNode(Attributes.get(i)), new GraphNode(Attributes.get(j)), null);
                    EdgeSet.add(edge);
                    if(!nonJoinNeighbors.containsKey(left)){
                        List<String> neighbors = new ArrayList<>();
                        neighbors.add(right);
                        nonJoinNeighbors.put(left, neighbors);
                    }
                    else{
                        nonJoinNeighbors.get(left).add(right);
                    }
                    if(!nonJoinNeighbors.containsKey(right)){
                        List<String> neighbors = new ArrayList<>();
                        neighbors.add(left);
                        nonJoinNeighbors.put(right, neighbors);
                    }
                    else{
                        nonJoinNeighbors.get(right).add(left);
                    }
                }
            }
        }
    }

    public static boolean hasNonJoinNeighbor(String attribute){//return if given attribute has non-join neighbors
        if(!nonJoinNeighbors.containsKey(attribute)){
            return false;
        }
        if(nonJoinNeighbors.get(attribute).size() == 0){
            return false;
        }
        return true;
    }

    public static boolean isActiveLeft(String attribute){
        //the given attribute should be in the left side of join predicate
        //return if this attribute active or not
        return activeLeftAttribute.contains(attribute);
    }

    public boolean edgeEqual(GraphEdge edge1, GraphEdge edge2){
        String edge1left = edge1.getStartNode().getAttribute().getAttribute();
        String edge1right = edge1.getEndNode().getAttribute().getAttribute();
        String edge2left = edge2.getStartNode().getAttribute().getAttribute();
        String edge2right = edge2.getEndNode().getAttribute().getAttribute();
        if(edge1left.compareTo(edge1right) > 0){
            String temp = edge1left;
            edge1left = edge1right;
            edge1right = temp;
        }
        if(edge2left.compareTo(edge2right) > 0){
            String temp = edge2left;
            edge2left = edge2right;
            edge2right = temp;
        }
        if(edge1left.equals(edge2left) && edge1right.equals(edge2right)){
            return true;
        }else{
            return false;
        }
    }

    public static List<String> getLeftJoinAttribute(String right){
        return adjNodes.get(right);
    }

    public static void addEdge(GraphEdge edge){
        String startNode = edge.getStartNode().getAttribute().getAttribute();
        String endNode = edge.getEndNode().getAttribute().getAttribute();
        if(adjNodes.get(endNode) == null){
            adjNodes.put(endNode, new ArrayList<>());
            adjNodes.get(endNode).add(startNode);
        }
        else{
            if(!adjNodes.get(endNode).contains(startNode)){
                adjNodes.get(endNode).add(startNode);
            }
        }
    }

    public static GraphNode getNode(String attribute){
        return nodeMap.get(attribute);
    }

    public static String getNextColumn(){//find next column to clean in SmartProject
        int MinNumOfMissingValue = Integer.MAX_VALUE;
        String nextColumn = null;
        for(int i=0;i<rightAttribute.size();i++){
            String right = rightAttribute.get(i);
            if(activeRightAttribute.contains(right)) continue;
            if(!nodeMap.get(right).isPicked() && nodeMap.get(right).getNumOfNullValues() < MinNumOfMissingValue){
                MinNumOfMissingValue = nodeMap.get(right).getNumOfNullValues();
                nextColumn = right;
                nodeMap.get(right).setPicked(true);
            }
        }
        return nextColumn;
    }

    public static List<GraphEdge> findRelatedEdge(String attribute){//return right (non-left) attribute for each related predicate
        List<GraphEdge> edges = new ArrayList<>();
        for(int i=0;i<EdgeSet.size();i++){
            if(EdgeSet.get(i).getEdgeType() == 0 && EdgeSet.get(i).getStartNode().getAttribute().getAttribute().equals(attribute)) {
                edges.add(EdgeSet.get(i));
            }
        }
        return edges;
    }

    public static List<String> findRelatedActiveRightAttributes(String left){
        List<String> attributes = new ArrayList<>();
        for(int i=0;i<EdgeSet.size();i++){
            if(EdgeSet.get(i).getEdgeType() == 0 && EdgeSet.get(i).getStartNode().getAttribute().getAttribute().equals(left) && EdgeSet.get(i).isActive()){
                attributes.add(EdgeSet.get(i).getEndNode().getAttribute().getAttribute());
            }
        }
        return attributes;
    }

    public static void printJoinEdge(){
        for(int i=0;i<EdgeSet.size();i++){
            if(EdgeSet.get(i).getEdgeType() == 0){
                System.out.println(EdgeSet.get(i).getStartNode().getAttribute().getAttribute() + " " + EdgeSet.get(i).getEndNode().getAttribute().getAttribute());
            }
        }
    }

    public static List<String> getActiveLeftAttribute(){//return left attribute for all active predicates
        return activeLeftAttribute;
    }

    public static List<String> getActiveRightAttribute(){
        return activeRightAttribute;
    }

    public static List<String> getRightJoinAttribute(){
        return rightAttribute;
    }

    public static List<String> getLeftJoinAttribute(){
        return leftAttribute;
    }

    public static void trigger(Attribute right){
        //first find all relevant join edges which have connected to given attribute
        //and then trigger them if they are active
        for(int i=0;i<EdgeSet.size();i++){
            if(EdgeSet.get(i).getEdgeType() == 0 && EdgeSet.get(i).getEndNode().getAttribute().getAttribute().equals(right.getAttribute())){
                //trigger condition is that the right column in join predicate does not have missing values
                if(EdgeSet.get(i).getEndNode().getNumOfNullValues() == 0){
                    EdgeSet.get(i).setActive();
                    String leftNode = EdgeSet.get(i).getStartNode().getAttribute().getAttribute();
                    String rightNode = EdgeSet.get(i).getEndNode().getAttribute().getAttribute();
                    //update active edges
                    if(!activeLeftAttribute.contains(leftNode)){
                        activeLeftAttribute.add(leftNode);
                    }
                    if(!activeRightAttribute.contains(rightNode)){
                        activeRightAttribute.add(rightNode);
                    }
                }
            }
        }
    }
}
