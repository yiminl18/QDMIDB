package Experiment.TestData;
import QMIDB.*;

import java.util.ArrayList;
import java.util.List;

public class Table{
    String tableName;
    List<Attribute> attributes = new ArrayList<>();

    public Table(String tableName){
        this.tableName = tableName;
    }

    public void addAttribute(String attribute){
        attributes.add(new Attribute(attribute));
    }

    public void print(){
        System.out.println(tableName);
        for(int i=0;i<attributes.size();i++){
            System.out.println(attributes.get(i).getAttribute() + " " + attributes.get(i).getCardinality() + " " + attributes.get(i).getNumOfNullValue());
        }
    }
}
