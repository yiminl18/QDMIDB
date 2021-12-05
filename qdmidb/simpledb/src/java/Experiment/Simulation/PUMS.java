package Experiment.Simulation;

import org.apache.commons.lang3.jmh_generated.HashSetvBitSetTest_testHashSet_jmhTest;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class PUMS {

    int columnSize = 33;
    int cardinality = 743659;
    private String ACSSchemaPath = "../QDMIDB/QDMIDB/qdmidb/simpledb/acsdataset/schema.txt";
    private String ACSCatalogPath = "../QDMIDB/QDMIDB/qdmidb/simpledb/acsdataset/catalog.txt";
    private String ACSPath = "../QDMIDB/QDMIDB/qdmidb/simpledb/acsdataset/";
    BufferedWriter ACSWriter, ACSSchemaWriter, ACSCatalogWriter;
    private List<List<Integer>> attrValues = new ArrayList<>();
    private HashMap<String, List<Integer>> completeACS = new HashMap<>();


    public PUMS(){
        getCompleteCleanTable();
        openSchemaWriters();
        int relationNum = 5;
        try{
            ACSSchemaWriter.write(String.valueOf(relationNum));
            ACSSchemaWriter.newLine();
        }catch (IOException e) {
            e.printStackTrace();
        }
        //split tables
        for(int i=0;i<relationNum;i++){
            String tableName = "t" + i;
            String RelativePath = ACSPath + tableName + ".txt";
            openACSWriters(RelativePath);
            switch (i){
                case 0:
                    getTable(tableName, splitTable(0,5,0));
                    break;
                case 1:
                    getTable(tableName, splitTable(6,9,0));
                    break;
                case 2:
                    getTable(tableName, splitTable(10,17,9));
                    break;
                case 3:
                    getTable(tableName, splitTable(18,25,15));
                    break;
                case 4:
                    getTable(tableName, splitTable(26,columnSize-1,20));
                    break;
            }
            closeWriters();
        }
        closeSchemaWriters();
    }

    public List<String> splitTable(int L, int R, int foreignKeyID){//boundary both included
        List<String> attrs = new ArrayList<>();
        for(int i=L;i<=R;i++){
            attrs.add("c" + i);
        }
        if(foreignKeyID < L || foreignKeyID > R){
            attrs.add("c" + foreignKeyID);
        }
        return attrs;
    }

    public void openACSWriters(String Path){
        try {
            String ACS = Paths.get(Path).toAbsolutePath().toString();
            FileWriter ACSFile = new FileWriter(ACS);
            ACSWriter = new BufferedWriter(ACSFile);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void openSchemaWriters(){
        try {
            String ACSSchema = Paths.get(ACSSchemaPath).toAbsolutePath().toString();
            FileWriter ACSSchemaFile = new FileWriter(ACSSchema);
            ACSSchemaWriter = new BufferedWriter(ACSSchemaFile);

            String ACSCatalog = Paths.get(ACSCatalogPath).toAbsolutePath().toString();
            FileWriter ACSCatalogFile = new FileWriter(ACSCatalog);
            ACSCatalogWriter = new BufferedWriter(ACSCatalogFile);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeWriters(){
        try{
            ACSWriter.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeSchemaWriters(){
        try{
            ACSSchemaWriter.close();
            ACSCatalogWriter.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getCompleteCleanTable(){
        //get complete clean relation
        Random rand = new Random();
        for(int i=0;i<columnSize;i++){
            int poolSize = ThreadLocalRandom.current().nextInt(5000,20000);
            int missingRate = ThreadLocalRandom.current().nextInt(10, 60);
            String columnName = "c" + i;
            List<Integer> attrs = new ArrayList<>();
            for(int j=0;j<cardinality;j++){
                int n = rand.nextInt(100);
                if(n > missingRate){
                    attrs.add(ThreadLocalRandom.current().nextInt(0,poolSize));
                }
                else{
                    attrs.add(WiFi.MISSING_INTEGER);
                }
            }
            completeACS.put(columnName, attrs);
        }
    }

    public void writeCatalog(String tableName, List<String> columnNames){
        try{
            String catalog = tableName;
            catalog += "(";
            for(int i=0;i<columnNames.size();i++){
                catalog += columnNames.get(i) + " int";
                if(i != columnNames.size()-1){
                    catalog += ", ";
                }else{
                    catalog += ")";
                }
            }
            ACSCatalogWriter.write(catalog);
            ACSCatalogWriter.newLine();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getTable(String tableName, List<String> columnNames){
        //split relations from completeACS
        try{
            //write metadata
            writeCatalog(tableName, columnNames);
            ACSSchemaWriter.write(String.valueOf(columnNames.size()));
            ACSSchemaWriter.newLine();
            for(int i=0;i<columnNames.size();i++){
                String attrName = tableName + "." + columnNames.get(i);
                ACSSchemaWriter.write(attrName);
                ACSSchemaWriter.newLine();
                int missing = 0;
                //System.out.println(tableName + " " + columnNames.get(i));
                for(int j=0;j<completeACS.get(columnNames.get(i)).size();j++){
                    if(completeACS.get(columnNames.get(i)).get(j) == WiFi.MISSING_INTEGER){
                        missing ++;
                    }
                }
                String stat = cardinality + "," + missing;
                ACSSchemaWriter.write(stat);
                ACSSchemaWriter.newLine();
            }
            //write table
            for(int i=0;i<cardinality;i++){
                String tuple = "";
                for(int j=0;j<columnNames.size();j++){
                    String attr = columnNames.get(j);
                    tuple += completeACS.get(attr).get(i);
                    if(j != columnNames.size() - 1){
                        tuple += ",";
                    }
                }
                ACSWriter.write(tuple);
                ACSWriter.newLine();
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
