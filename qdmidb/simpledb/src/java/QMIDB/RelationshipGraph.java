package QMIDB;
import org.w3c.dom.Attr;
import simpledb.*;

import java.util.ArrayList;
import java.util.*;
import java.util.List;

/*
    *Relationship graph is used to maintain the status of imputation in query processing
    *It takes input of predicates set of query to initialize the graph
 */
public class RelationshipGraph {
    private static Map<String, List<GraphNode>> adjNodes = new HashMap<>();//use the integer index of nodes to construct the graph
    private static Map<String, GraphNode> nodeMap = new HashMap<>();//mapping from String attribute to graph node
    private static Map<String, GraphEdge> edgeMap = new HashMap<>();//mapping from String attribute to graph edge
    private static List<GraphNode> NodeSet = new ArrayList<>();
    private static List<GraphEdge> EdgeSet = new ArrayList<>();
    private static List<PredicateUnit> preds;
    private static List<GraphEdge> activeEdge = new ArrayList<>();

    public RelationshipGraph(List<Attribute> Attributes, List<PredicateUnit> Preds) {
        preds = Preds;
        //initialize nodes
        for(int i=0;i<Attributes.size();i++){
            GraphNode node = new GraphNode(Attributes.get(i));
            NodeSet.add(node);
            nodeMap.put(Attributes.get(i).getAttribute(),node);
        }
        //initialize join-edges
        //join-edges come with order: left and right
        for(int i=0;i<preds.size();i++){
            if(preds.get(i).getIsJoin()){
                GraphEdge edge = new GraphEdge(0,new GraphNode(preds.get(i).getLeft()), new GraphNode(preds.get(i).getRight()), preds.get(i).transform());
                EdgeSet.add(edge);
                edgeMap.put(preds.get(i).getLeft().getAttribute() + preds.get(i).getRight().getAttribute(),edge);
                addEdge(edge);
            }
        }
        //initialize non-join-edges
        for(int i=0;i<Attributes.size();i++){
            for(int j = i+1; j<Attributes.size(); j++){
                if(Attributes.get(i).getRelation().equals(Attributes.get(j).getRelation())){//if in same relation
                    GraphEdge edge = new GraphEdge(1, new GraphNode(Attributes.get(i)), new GraphNode(Attributes.get(j)), null);
                    edgeMap.put(Attributes.get(i).getAttribute(),edge);
                    EdgeSet.add(edge);
                }
            }
        }
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

    public static void addEdge(GraphEdge edge){
        if(adjNodes.get(edge.getStartNode().getAttribute().getAttribute()) == null){
            adjNodes.put(edge.getStartNode().getAttribute().getAttribute(), new ArrayList<>());
        }
        else{
            if(!adjNodes.containsKey(edge.getStartNode().getAttribute().getAttribute())){
                adjNodes.get(edge.getStartNode().getAttribute().getAttribute()).add(edge.getEndNode());
            }
        }
    }

    public static GraphNode getNode(Attribute attribute){
        return nodeMap.get(attribute.getAttribute());
    }

    public static GraphEdge getEdge(Attribute attribute1, Attribute attribute2){
        String key1 = attribute1.getAttribute() + attribute2.getAttribute();
        String key2 = attribute2.getAttribute() + attribute1.getAttribute();
        if(edgeMap.containsKey(key1)){
            return edgeMap.get(key1);
        }else if(edgeMap.containsKey(key2)){
            return edgeMap.get(key2);
        }
        else{
            return null;
        }
    }
}
