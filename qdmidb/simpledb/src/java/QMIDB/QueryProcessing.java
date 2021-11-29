package QMIDB;

import java.util.ArrayList;
import java.util.List;
import simpledb.*;


public class QueryProcessing {
    public QueryProcessing(int queryID) {
        //initialization
        fileHandles fH = new fileHandles();
        fH.loadWiFiImputations();
        List<Attribute> schema = fH.readSchema();
        //List<Attribute> schema = ManualSchema();
        //predicates: must put Filter first
        List<PredicateUnit> predicates = fH.readPredicatesForGivenQuery(queryID);
        Schema.setSchema(schema, predicates);
        Buffer.initBuffer();
        PredicateSet.initPredicateSet(predicates);
        AggregateOptimization.init();
//        System.out.println("Print aggregate predicate!");
//        System.out.println(AggregateOptimization.getAggregatePred());
        //List<PredicateUnit> predicates = ManualPredicates();
        RelationshipGraph.initGraph(schema, predicates);
        //testing
//        System.out.println("nodes in RG");
//        for(int i=0;i<RelationshipGraph.getNodes().size();i++){
//            System.out.println(RelationshipGraph.getNodes().get(i).getAttribute());
//        }
//        System.out.println("all attributes");
//        for(int i=0;i<Statistics.getAttributes().size();i++){
//            System.out.println(Statistics.getAttributes().get(i).getAttribute());
//        }
//        PredicateSet.print();//correct
        //Schema.print();//correct
        //Statistics.print();
        ImputeFactory.setImputationMethod("HOTDECK");
        Buffer.bufferCDCValues(schema);
        //RelationshipGraph.printNonJoinNeighbor();
        //System.out.println(Statistics.getAttribute("R.b").getNumOfNullValue());
        //System.out.println(Statistics.getAttribute("S.b").getNumOfNullValue());
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
