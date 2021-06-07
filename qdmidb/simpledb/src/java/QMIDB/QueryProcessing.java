package QMIDB;

import java.util.List;
import simpledb.*;


public class QueryProcessing {
    public QueryProcessing() {
        //initialization
        fileHandles fH = new fileHandles();
        List<Attribute> schema = fH.readSchema();
        List<PredicateUnit> predicates = fH.readPredicates();
        RelationshipGraph.initGraph(schema, predicates);
        PredicateSet.initPredicateSet(predicates);
        Schema.setSchema(schema);
    }

    public DbIterator constructQueryPlan(List<PredicateUnit> preds){
        DbIterator iterator = null;
        //construct query plan given preds in order of query plan tree
        return iterator;
    }

}
