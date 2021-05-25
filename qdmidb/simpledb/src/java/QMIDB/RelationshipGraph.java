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
    private static Map<GraphNode, List<GraphNode>> adjNodes = new HashMap<>();//use the integer index of nodes to construct the graph
    private static Map<Integer, GraphNode> nodeMap = new HashMap<>();//mapping from integer index to graph node
    private static List<GraphNode> NodeSet = new ArrayList<>();
    private static List<PredicateUnit> preds;

    public RelationshipGraph(List<Attribute> Attributes, List<PredicateUnit> Preds) {
        preds = Preds;
        //initialize nodes
        for(int i=0;i<Attributes.size();i++){
            GraphNode node = new GraphNode(Attributes.get(i));
            NodeSet.add(node);
            nodeMap.put(i,node);
        }
        //initialize join-edges
        for(int i=0;i<preds.size();i++){
            if(preds.get(i).getIsJoin()){
                addEdge(new GraphEdge(0,new GraphNode(preds.get(i).getLeft()), new GraphNode(preds.get(i).getRight())));
                addEdge(new GraphEdge(0,new GraphNode(preds.get(i).getRight()), new GraphNode(preds.get(i).getLeft())));
            }
        }
        //initialize non-join-edges
        for(int i=0;i<Attributes.size();i++){
            for(int j = i+1; j<Attributes.size(); j++){
                if(Attributes.get(i).getRelation().equals(Attributes.get(j).getRelation())){
                    addEdge(new GraphEdge(1, new GraphNode(Attributes.get(i)), new GraphNode(Attributes.get(j))));
                    addEdge(new GraphEdge(1, new GraphNode(Attributes.get(j)), new GraphNode(Attributes.get(i))));
                }
            }
        }
    }

    public static void addEdge(GraphEdge edge){
        if(adjNodes.get(edge.getStartNode()) == null){
            adjNodes.put(edge.getStartNode(), new ArrayList<>());
        }
        else{
            if(!adjNodes.containsKey(edge.getStartNode())){
                adjNodes.get(edge.getStartNode()).add(edge.getEndNode());
            }
        }
    }
}
