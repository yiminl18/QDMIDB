package Experiment.CDCData;

import Experiment.WiFiData.AP2Room;
import Experiment.WiFiData.Connect;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


public class Data {
    public static final int MISSING_INTEGER = Integer.MIN_VALUE;
    private static String schemaFile = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/cdcdataset/schema.txt";
    private static BufferedWriter schemaHandle;

    public Data(){
        schemaHandle = openSchema();

        write2Schema(schemaHandle,"3");//3 relations

        String demoOutFile = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/cdcdataset/demoDirty.txt";
        String examsOutFile = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/cdcdataset/examsDirty.txt";
        String labsOutFile = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/cdcdataset/labsDirty.txt";
        String demoRaw = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/cdcdataset/demo.csv";
        String examsRaw = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/cdcdataset/exams.csv";
        String labsRaw = "/Users/linyiming/eclipse-workspace/QDMIDB/qdmidb/simpledb/cdcdataset/labs.csv";

        getMedataForOneRelation("demo", demoRaw, demoOutFile);
        getMedataForOneRelation("exams", examsRaw, examsOutFile);
        getMedataForOneRelation("labs", labsRaw, labsOutFile);

        closeSchema(schemaHandle);
    }

    public BufferedWriter openSchema(){
        BufferedWriter bwSchema = null;
        try{
            FileWriter outSchema = new FileWriter(schemaFile);
            bwSchema = new BufferedWriter(outSchema);
        }catch (IOException e) {
            e.printStackTrace();
        }
        return bwSchema;
    }

    public static void write2Schema(BufferedWriter handle, String content){
        try{
            handle.write(content);
            handle.newLine();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeSchema(BufferedWriter handle){
        try{
            handle.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getMedataForOneRelation(String relation, String inPath, String outPath){
        try{
            FileWriter outWriter = new FileWriter(outPath);
            BufferedWriter out = new BufferedWriter(outWriter);
            BufferedReader in = new BufferedReader(new FileReader(inPath));
            int count = 0;
            String row;
            String[] schema = null;
            List<Integer> missingCount = new ArrayList<>();
            while ((row = in.readLine()) != null) {
                count++;
                if (count == 1){
                    //read schema
                    schema = row.split(",");
                    for(int i=0;i<schema.length;i++){
                        missingCount.add(0);
                    }
                    continue;
                }
                String output = "";
                List<String> strs = new ArrayList<>();
                String str = "";
                for(int i=0;i<row.length();i++){
                    if(i == 0 && row.charAt(i) == ','){
                        output += MISSING_INTEGER;
                    }
                    output += row.charAt(i);
                    if(i<row.length()-1 && row.charAt(i) == ',' && row.charAt(i+1) == ','){
                        output += MISSING_INTEGER;
                    }
                    if(i == row.length()-1 && row.charAt(i) == ','){
                        output += MISSING_INTEGER;
                        strs.add(String.valueOf(MISSING_INTEGER));
                    }
                    if(row.charAt(i) == ','){
                        if(i == 0){
                            strs.add(String.valueOf(MISSING_INTEGER));
                        }
                        else{
                            if(str.equals("")){
                                strs.add(String.valueOf(MISSING_INTEGER));
                            }
                            else{
                                strs.add(str);
                                str = "";
                            }
                        }
                    }else{
                        str += row.charAt(i);
                    }
                }

                for(int i=0;i<strs.size();i++){
                    if(strs.get(i).equals(String.valueOf(MISSING_INTEGER))){
                        missingCount.set(i,missingCount.get(i)+1);
                    }
                }

                out.write(output);
                out.newLine();
            }

            int cardinality = count-1;
            //write number of attributes
            write2Schema(schemaHandle, String.valueOf(schema.length));
            for(int i=0;i<schema.length;i++){
                String attributeName = relation + "." + schema[i];
                write2Schema(schemaHandle, attributeName);
                String output = cardinality + ","  + missingCount.get(i);
                write2Schema(schemaHandle, output);
            }

            out.flush();
            out.close();
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
