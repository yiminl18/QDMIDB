package QMIDB;

import simpledb.Aggregator;
import simpledb.IntField;
import simpledb.Predicate;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/*
    this class reads metadata from file to initialize the data structures
 */
public class fileHandles {
    private final String schemaFilePath = "simpledb/metadata/schema.txt";
    private final String predicateFilePath = "simpledb/metadata/predicate.txt";

    public List<Attribute> readSchema(){
        int N,n;
        List<Attribute> attributes = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(schemaFilePath)))) {
            String line = br.readLine();
            N = Integer.valueOf(line);
            for(int i=0;i<N;i++){
                line = br.readLine();
                n = Integer.valueOf(line);
                for(int j=0;j<n;j++){
                    line = br.readLine();
                    Attribute attribute = new Attribute(line.split("\\,")[1]);
                    attribute.setSchemaWidth(n);
                    attributes.add(attribute);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return attributes;
    }

    public List<PredicateUnit> readPredicates(){
        List<PredicateUnit> predicateUnits = new ArrayList<>();
        int n;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(predicateFilePath)))) {
            String line = br.readLine();
            n = Integer.valueOf(line);
            for(int i=0;i<n;i++){
                String type = br.readLine();
                String predicate[] = br.readLine().split(" ");
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
