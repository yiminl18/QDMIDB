package QMIDB;

import java.util.ArrayList;
import java.util.List;
import simpledb.*;


public class QueryProcessing {
    public QueryProcessing() {
        //initialization
        fileHandles fH = new fileHandles();
        List<Attribute> schema = fH.readSchema();
        //List<Attribute> schema = ManualSchema();
        //predicates: must put Filter first
        //List<PredicateSet> predicates = fH.readPredicatesAllQueries();
        //for(int i=0;i<predicates.size();i++){

        //}
        List<PredicateUnit> predicates = ManualPredicates();
        RelationshipGraph.initGraph(schema, predicates);
        //RelationshipGraph.printJoinEdge();
        PredicateSet.initPredicateSet(predicates);
        Schema.setSchema(schema, predicates);
        Statistics.initStatistics();
        ImputeFactory.setImputationMethod("Manual");
        //RelationshipGraph.printNonJoinNeighbor();
        //System.out.println(Statistics.getAttribute("R.b").getNumOfNullValue());
        //System.out.println(Statistics.getAttribute("S.b").getNumOfNullValue());
    }

    public DbIterator constructQueryPlan(List<PredicateUnit> preds){
        DbIterator iterator = null;
        //construct query plan given preds in order of query plan tree
        return iterator;
    }

    public List<PredicateUnit> ManualPredicates(){//for testing purpose
        List<PredicateUnit> predicates = new ArrayList<>();
        predicates.add(new PredicateUnit(new Attribute("S.c"), Predicate.Op.GREATER_THAN, new IntField(0)));
        predicates.add(new PredicateUnit(new Attribute("R.b"), Predicate.Op.EQUALS, new Attribute("S.b")));
        predicates.add(new PredicateUnit(new Attribute("R.a"), Predicate.Op.EQUALS, new Attribute("T.a")));
        return predicates;
    }

    public List<Attribute> ManualSchema(){//for testing purpose
        List<Attribute> schema = new ArrayList<>();
        /*Attribute attribute = new Attribute("R.a");
        attribute.setCardinality(4);
        attribute.setNumOfNullValue(1);*/
        schema.add(new Attribute("R.a"));
        schema.add(new Attribute("R.b"));
        schema.add(new Attribute("S.b"));
        schema.add(new Attribute("S.c"));
        schema.add(new Attribute("T.a"));
        schema.add(new Attribute("T.d"));
        return schema;
    }


}
