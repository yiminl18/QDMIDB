package QMIDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
    Store schema of relations
 */
public class Schema {
    private static List<Attribute> schema;
    private static List<String> filterAttributeNames = new ArrayList<>();//store name of attributes in filter predicate
    private static HashMap<String, Integer> schemaWidthMap = new HashMap<>();
    private static HashMap<String, String> firstAttribute = new HashMap<>();//return the first attribute of the schema

    public static void setSchema(List<Attribute> Schema, List<PredicateUnit> predicateSET){
        schema = Schema;
        String preAttribute = "null", currentAttribute;
        String first = null;
        for(int i=0;i<schema.size();i++){
            schemaWidthMap.put(schema.get(i).getAttribute(),schema.get(i).getSchemaWidth());
            currentAttribute = schema.get(i).getAttribute();
            if(currentAttribute.charAt(0) != preAttribute.charAt(0)){
                first = currentAttribute;
            }
            firstAttribute.put(currentAttribute, first);
            preAttribute = currentAttribute;
            String attribute = Schema.get(i).getAttribute();
            for(int j=0;j<predicateSET.size();j++){
                if(predicateSET.get(j).getType().equals("Filter") && predicateSET.get(j).getFilterAttribute().getAttribute().equals(attribute)){
                    filterAttributeNames.add(attribute);
                    break;
                }
            }
        }
    }

    public static List<String> getFilterAttributeNames(){
        return filterAttributeNames;
    }

    public static List<Attribute> getSchema(){
        return schema;
    }

    public static String getFirstAttribute(String attribute){
        return firstAttribute.get(attribute);
    }

    public static int getWidth(String attributeName){
        return schemaWidthMap.get(attributeName);
    }

    public static void print(){
        for(int i=0;i<schema.size();i++){
            System.out.println(schema.get(i).getAttribute() + " " + schema.get(i).getCardinality() + schema.get(i).getNumOfNullValue());
        }
    }
}
