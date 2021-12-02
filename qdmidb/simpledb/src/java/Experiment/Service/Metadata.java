package Experiment.Service;
/*
    this class provides common services to process different data sets
 */

import java.io.*;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import com.sun.javafx.scene.layout.PaneHelper;
import javafx.util.*;

public class Metadata {
    private static final int MISSING_INTEGER = Integer.MIN_VALUE;
    private static final int numOfRelation = 3;
    private static final String catalogFile = "../QDMIDB/QDMIDB/qdmidb/queryplancodes/wifi/catalog.txt";
    private static final String schemaFile = "../QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/schema.txt";

    public void getSchema(){
        //automatically generate schema and statistics from raw data given catalog file
        String catalogPath = Paths.get(catalogFile).toAbsolutePath().toString();
        String schemaPath = Paths.get(schemaFile).toAbsolutePath().toString();
        try {
            BufferedReader catalog = new BufferedReader(new FileReader(catalogPath));
            FileWriter schemaWriter = new FileWriter(schemaPath);
            BufferedWriter bw = new BufferedWriter(schemaWriter);

            bw.write(String.valueOf(numOfRelation));
            bw.newLine();

            String row;
            while ((row = catalog.readLine()) != null) {
                String tableName = row.split("\\(")[0];
                String strs = row.split("\\(")[1].split("\\)")[0];
                String[] attrs = strs.split(",");
                List<String> attributes = new ArrayList<>();
                for(int i=0;i<attrs.length;i++){
                    String s = attrs[i];
                    if(s.substring(0,1).equals(" ")){
                        s = s.substring(1,s.length());
                    }
                    String attr = tableName + "." + s.split(" ")[0];
                    attributes.add(attr);
                    //System.out.println(attr);
                }
                //write number of attributes
                bw.write(String.valueOf(attributes.size()));
                bw.newLine();

                String rawDataFile = "../QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/" + tableName + ".txt";
                String rawDataPath = Paths.get(rawDataFile).toAbsolutePath().toString();
                //System.out.println(rawDataPath);
                //print statistics
                HashMap<String, Pair<Integer, Integer>> stats = computeStatistics(attributes, rawDataPath);
                for(int i=0;i<attributes.size();i++){
                    bw.write(attributes.get(i));
                    bw.newLine();
                    int cardinality = stats.get(attributes.get(i)).getKey();
                    int missingNum = stats.get(attributes.get(i)).getValue();
                    String out = cardinality + "," + missingNum;
                    bw.write(out);
                    bw.newLine();
                }
            }

            catalog.close();
            bw.flush();
            bw.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, Pair<Integer, Integer>> computeStatistics(List<String> attrs, String filePath){
        HashMap<String, Pair<Integer, Integer>> statistics = new HashMap<>();
        for(int i=0;i<attrs.size();i++){
            statistics.put(attrs.get(i),new Pair<>(0,0));
        }
        try{
            BufferedReader f = new BufferedReader(new FileReader(filePath));
            String row;
            int cardinality = 0, missingNum;
            while ((row = f.readLine()) != null) {
                String[] values = row.split(",");
                if(attrs.size() != values.length){
                    System.out.println("Schema length is different!" + " " + attrs.size() + " " + values.length + " " + attrs.get(0));
                    return null;
                }
                for(int i=0;i<attrs.size();i++){
                    missingNum = statistics.get(attrs.get(i)).getValue();
                    if(Integer.valueOf(values[i]) == MISSING_INTEGER){
                        statistics.put(attrs.get(i),new Pair<>(0,missingNum+1));
                    }
                }
                cardinality ++;
            }
            //set cardinality
            for(int i=0;i<attrs.size();i++){
                missingNum = statistics.get(attrs.get(i)).getValue();
                statistics.put(attrs.get(i),new Pair<>(cardinality,missingNum));
            }
        }catch (IOException e) {
        e.printStackTrace();
        }
        return statistics;
    }

}
