package QMIDB;

import simpledb.Aggregator;
import simpledb.IntField;
import simpledb.Predicate;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

/*
    this class reads metadata from file to initialize the data structures
 */
public class fileHandles {
    private final String WifISchemaFilePath = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/schema.txt";
    private final String CDCSchemaFilePath = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/cdcdataset/schema.txt";
    private final String ACSSchemaFilePath = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/acsdataset/schema.txt";
    private final String WiFiPredicatesFilePath = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/predicates.txt";
    private final String CDCPredicatesFilePath = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/cdcdataset/predicates.txt";
    private final String ACSPredicatesFilePath = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/acsdataset/predicates.txt";

    public List<Attribute> readSchema(String dataset){
        int N,n;
        String schemaPath = "";
        if(dataset.equals("CDC")){
            schemaPath = CDCSchemaFilePath;
        }else if(dataset.equals("WiFi")){
            schemaPath = WifISchemaFilePath;
        }else if(dataset.equals("ACS")){
            schemaPath = ACSSchemaFilePath;
        }
        List<Attribute> attributes = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(schemaPath)))) {

            String line = br.readLine();
            N = Integer.valueOf(line);
            for(int i=0;i<N;i++){
                line = br.readLine();
                n = Integer.valueOf(line);
                for(int j=0;j<n;j++){
                    line = br.readLine();
                    Attribute attribute = new Attribute(line);
                    attribute.setSchemaWidth(n);
                    line = br.readLine();
                    attribute.setCardinality(Integer.valueOf(line.split("\\,")[0]));
                    attribute.setNumOfNullValue(Integer.valueOf(line.split("\\,")[1]));
                    attributes.add(attribute);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return attributes;
    }

    public List<PredicateUnit> readPredicates(int queryID, String dataset){
        //read predicates for query with given queryID
        List<PredicateUnit> predicateUnits = new ArrayList<>();
        int n, queryNum, QID;
        String predicatePath = "";
        if(dataset.equals("CDC")){
            predicatePath = CDCPredicatesFilePath;
        }else if(dataset.equals("WiFi")){
            predicatePath = WiFiPredicatesFilePath;
        }else if(dataset.equals("ACS")){
            predicatePath = ACSPredicatesFilePath;
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(predicatePath)))) {
            String line = br.readLine();
            queryNum = Integer.valueOf(line);
            for(int i=0;i<queryNum;i++){//iterative each query
                List<Attribute> attrs = new ArrayList<>();

                line = br.readLine();
                QID = Integer.valueOf(line);
                line = br.readLine();
                int numOfAttrs = Integer.valueOf(line);
                for(int j=0;j<numOfAttrs;j++){
                    line = br.readLine();
                    attrs.add(new Attribute(line));
                }
                line = br.readLine();
                n = Integer.valueOf(line);
                for(int j=0;j<n;j++){
                    String type = br.readLine();
                    String predicate[] = br.readLine().split(" ");
                    if(QID == queryID){
                        Statistics.initStatistics(attrs);
                        switch (type){
                            case "F":
                                predicateUnits.add(new PredicateUnit(new Attribute(predicate[0]),getOp(predicate[1]),new IntField(Integer.valueOf(predicate[2]))));//ihe only support int for now
                                break;
                            case "J":
                                predicateUnits.add(new PredicateUnit(new Attribute(predicate[0]),getOp(predicate[1]),new Attribute(predicate[2])));
                                break;
                            case "A":
                                predicateUnits.add(new PredicateUnit(new Attribute(predicate[0]),getAop(predicate[1])));
                                break;
                            case "O":
                                predicateUnits.add(new PredicateUnit(new Attribute(predicate[0]),predicate[1]));
                                break;
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return predicateUnits;
    }

    public Predicate.Op getOp(String op){
        switch (op){
            case "=":
                return Predicate.Op.EQUALS;
            case "<>":
                return Predicate.Op.NOT_EQUALS;
            case "LIKE":
                return Predicate.Op.LIKE;
            case ">":
                return Predicate.Op.GREATER_THAN;
            case ">=":
                return Predicate.Op.GREATER_THAN_OR_EQ;
            case "<":
                return Predicate.Op.LESS_THAN;
            case "<=":
                return Predicate.Op.LESS_THAN_OR_EQ;
            default:
                System.out.println("predicate op is invalid");
                return null;
        }
    }

    public Aggregator.Op getAop(String aop){
        switch (aop){//MIN, MAX, SUM, AVG, COUNT,
            case "MIN":
                return Aggregator.Op.MIN;
            case "MAX":
                return Aggregator.Op.MAX;
            case "SUM":
                return Aggregator.Op.SUM;
            case "AVG":
                return Aggregator.Op.AVG;
            case "COUNT":
                return Aggregator.Op.COUNT;
            default:
                System.out.println("predicate aop is invalid");
                return null;
        }
    }
}
