package QMIDB;

import java.util.List;
import simpledb.*;



public class QueryProcessing {
    public QueryProcessing(List<Attribute> Attributes, List<PredicateUnit> preds) {
        //initialization
        RelationshipGraph RG = new RelationshipGraph(Attributes, preds);
        HashTables hashTables = new HashTables();

    }

    public DbIterator constructQueryPlan(List<PredicateUnit> preds){
        DbIterator iterator = null;
        //construct query plan given preds in order of query plan tree
        return iterator;
    }

}
