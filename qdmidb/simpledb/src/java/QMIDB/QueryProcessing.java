package QMIDB;

import java.util.ArrayList;
import java.util.List;
import simpledb.*;


public class QueryProcessing {
    public QueryProcessing(int queryID, String dataset) {
        fileHandles fH = new fileHandles();
        List<Attribute> schema = fH.readSchema(dataset);
        List<PredicateUnit> predicates = fH.readPredicates(queryID, dataset);

        Schema.setSchema(schema, predicates);
        Buffer.initBuffer();
        PredicateSet.initPredicateSet(predicates);
        AggregateOptimization.init();
        RelationshipGraph.initGraph(schema, predicates);
        ImputeFactory.setImputationMethod("HOTDECK");

        //load buffered values in CDC dataset to compute stats
        if(dataset.equals("CDC")){
            Buffer.bufferCDCValues(schema);
        }else if(dataset.equals("WiFi")){
            Buffer.bufferWiFiValues(schema);
        } else{

        }

        //        System.out.println("Print right attributes:");
//        for(int i=0;i<RelationshipGraph.getRightAttributes().size();i++){
//            System.out.println(RelationshipGraph.getRightAttributes().get(i));
//        }
        //testing
        System.out.println("nodes in RG");
        for(int i=0;i<RelationshipGraph.getNodes().size();i++){
            System.out.println(RelationshipGraph.getNodes().get(i).getAttribute());
        }
//        System.out.println("all attributes");
//        for(int i=0;i<Statistics.getAttributes().size();i++){
//            System.out.println(Statistics.getAttributes().get(i).getAttribute());
//        }
//        PredicateSet.print();//correct
        //Schema.print();//correct
        //Statistics.print();
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
