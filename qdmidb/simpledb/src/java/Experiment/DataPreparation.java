package Experiment;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.*;

/*
    This class transform the format of raw data and prepare metadata for testing
 */
public class DataPreparation {
    List<Table> tables = new ArrayList<>();
    private final List<String> tableNames = Arrays.asList("demo","exams","labs");

    public DataPreparation(){
        for(int i=0;i<tableNames.size();i++){
            tables.add(new Table(tableNames.get(i)));
            generateTable(tableNames.get(i));
        }
    }

    private Table getTable(String tableName){
        for(int i=0;i<tables.size();i++){
            if(tables.get(i).tableName.equals(tableName)){
                return tables.get(i);
            }
        }
        return null;
    }

    private List<String> stringSplit(String s, char a){
        List<String> str = new ArrayList<>();
        int pre = 0;
        boolean flag = false;
        for(int i=0;i<s.length();i++){
            char c = s.charAt(i);
            if(c == a){
                if(i==0){//first char is missing
                    str.add("*");
                }else{
                    if(!flag){
                        flag = true;
                        str.add(s.substring(pre,i));
                        pre = i;
                        continue;
                    }
                    if(i == pre+1){//missing
                        str.add("*");
                    }else{
                        str.add(s.substring(pre+1,i));
                    }
                    if(i == s.length()-1){
                        str.add("*");
                    }
                }
                pre = i;
            }
            else if(i == s.length()-1){
                str.add(s.substring(pre+1,s.length()));
            }
        }
        return str;
    }

    public void generateTable(String tableName){
        String tablePathIn = "simpledb/demo_data/" + tableName + ".csv";
        String tablePathOut = "simpledb/demo_data/" + tableName + "_new.csv";

        if(new File(tablePathOut).exists()){
            System.out.println(tablePathOut + " Exists!");
            return;
        }

        Table table = getTable(tableName);
        int cardinality = 0;
        List<Integer> missingCount = new ArrayList<>();
        int count=0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(tablePathIn)))) {

            FileWriter out = new FileWriter(tablePathOut);
            BufferedWriter bw=new BufferedWriter(out);

            String line = br.readLine();
            List<String> attributes = stringSplit(line, ',');
            //System.out.println(attributes.size());
            for(int i=0;i<attributes.size();i++){
                //System.out.println(attributes.get(i));
                table.addAttribute(tableName + "." + attributes.get(i));
                missingCount.add(0);
            }
            while((line = br.readLine())!=null){
                count++;
                //System.out.println(line);
                List<String> attributeValues = stringSplit(line, ',');
                String outValues = "";
                //System.out.println(attributeValues.size());
                for(int i=0;i<attributeValues.size();i++){
                    //System.out.print(attributeValues.get(i) + " ");
                    if(attributeValues.get(i).equals("*")) {
                        missingCount.set(i, missingCount.get(i)+1);
                        outValues += String.valueOf(Integer.MIN_VALUE);
                    }else{
                        outValues += attributeValues.get(i);
                    }
                    if(i != attributeValues.size()-1){
                        outValues += ",";
                    }
                }
                //System.out.println("");
                bw.write(outValues);
                bw.newLine();
                cardinality ++;
            }

            bw.flush();
            bw.close();

        }catch (IOException e) {
            e.printStackTrace();
        }
        for(int i=0;i<table.attributes.size();i++){
            table.attributes.get(i).setCardinality(cardinality);
            table.attributes.get(i).setNumOfNullValue(missingCount.get(i));
        }

        //table.print();
    }
}
