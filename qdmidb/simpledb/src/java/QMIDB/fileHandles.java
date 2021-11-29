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
    private final String schemaFilePath = "simpledb/metadata/schema.txt";
    private final String WifISchemaFilePath = "simpledb/wifidataset/schema.txt";
    private final String CDCSchemaFilePath = "simpledb/cdcdataset/schema.txt";
    private final String predicateFilePath = "simpledb/metadata/predicate.txt";
    private final String WiFiPredicatesFilePath = "simpledb/wifidataset/predicates.txt";
    private final String CDCPredicatesFilePath = "simpledb/cdcdataset/predicates.txt";
    private final String CDCPredicatesMAXMINFilePath = "simpledb/cdcdataset/predicatesMINMAX.txt";

    public List<Attribute> readSchema(){
        int N,n;
        List<Attribute> attributes = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(CDCSchemaFilePath)))) {

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

    public List<PredicateUnit> readPredicatesForGivenQuery(int queryID){
        //read predicates for query with given queryID
        List<PredicateUnit> predicateUnits = new ArrayList<>();
        //Vector<PredicateUnit> js = new ArrayList<>();
        //String lin = "bad pig";
        //lin.equals("always");
        int n, queryNum, QID;

        int room;
        room = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(CDCPredicatesFilePath)))) {
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

    public List<PredicateUnit> readPredicatesOneQuery(){
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

    public void loadWiFiImputations(){
        String fileUserImputed = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/userImputedValues.txt";
        String fileSpaceImputed = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/spaceImputedValues.txt";
        String fileWiFiImputed = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/wifidataset/wifiImputedValues.txt";
        loadImputedValues("users", fileUserImputed);
        loadImputedValues("space",fileSpaceImputed);
        loadImputedValues("wifi", fileWiFiImputed);
    }

    public void loadImputedValues(String relation, String filePath){
        //first key is tid, starting from 0 for each relation
        //second key is field index to its imputed value
        HashMap<Integer, HashMap<Integer, Integer>> imputedValues = new HashMap<>();
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(filePath));
            String row;
            while (true) {
                row = csvReader.readLine();
                if(row == null){
                    break;
                }
                int tid = Integer.valueOf(row);
                row = csvReader.readLine();
                int numOfImputedValues = Integer.valueOf(row);
                HashMap<Integer, Integer> mp = new HashMap<>();
                for(int i=0;i<numOfImputedValues;i++){
                    row = csvReader.readLine();
                    String[] data = row.split(",");
                    int fieldIndex = Integer.valueOf(data[0]);
                    int value = Integer.valueOf(data[1]);
                    mp.put(fieldIndex, value);
                }
                imputedValues.put(tid, mp);
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        ImputeFactory.loadImputations(relation,imputedValues);
    }
}
