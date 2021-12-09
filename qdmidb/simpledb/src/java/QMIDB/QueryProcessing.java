package QMIDB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import simpledb.*;


public class QueryProcessing {
    public void init(String dataset) {
        fileHandles fH = new fileHandles();
        List<Attribute> schema = fH.readSchema(dataset);
        //load buffered values to compute stats
        if(dataset.equals("CDC")){
            Buffer.bufferCDCValues(schema);
        }else if(dataset.equals("WiFi")){
            Buffer.bufferWiFiValues(schema);
        } else if(dataset.equals("ACS")){
            Buffer.bufferACSValues(schema);
        }else{
            System.out.println("Relation name incorrect!");
        }

//                System.out.println("Print right attributes:");
//        for(int i=0;i<RelationshipGraph.getRightAttributes().size();i++){
//            System.out.println(RelationshipGraph.getRightAttributes().get(i));
//        }
        //testing
//        System.out.println("nodes in RG");
//        for(int i=0;i<RelationshipGraph.getNodes().size();i++){
//            System.out.println(RelationshipGraph.getNodes().get(i).getAttribute());
//        }
//        System.out.println("all attributes");
//        for(int i=0;i<Statistics.getAttributes().size();i++){
//            System.out.println(Statistics.getAttributes().get(i).getAttribute());
//        }
        //PredicateSet.print();//correct
        //Schema.print();//correct
        //Statistics.print();
    }

    public void ExperimentRunner()throws IOException,Exception{
        QueryProcessing QP = new QueryProcessing();
        String dataset = "ACS";//WiFi, CDC, ACS
        String algorithm = "Quip";//Quip, ImputeDB
        String imputation = "HOTDECK";//REGRESSION_TREE,HOTDECK
        int queryID = 5;

        System.out.println("Computing stats for " + dataset + "...");
        QP.init(dataset);
        System.out.println("Stats computation is done!");

        run(queryID,algorithm,imputation,dataset);
    }

    public void run(int queryID, String algorithm, String imputation, String dataset)throws IOException,Exception {
        fileHandles fH = new fileHandles();
        List<Attribute> schema = fH.readSchema(dataset);
        List<PredicateUnit> predicates = fH.readPredicates(queryID, dataset);
        PredicateSet.initPredicateSet(predicates);
        RelationshipGraph.initGraph(schema, predicates);
        Schema.setSchema(schema, predicates);

        ImputeFactory.setImputationMethod(imputation);
        Buffer.initBuffer();
        AggregateOptimization.init();

        test t = new test();
        t.runCDC(queryID, dataset, algorithm);
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
