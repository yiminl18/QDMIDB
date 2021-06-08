package QMIDB;

import java.util.HashMap;
import java.util.List;

/*
    Store schema of relations
 */
public class Schema {
    private static List<Attribute> schema;
    private static HashMap<String, Integer> schemaWidthMap = new HashMap<>();

    public static void setSchema(List<Attribute> Schema){
        schema = Schema;
        for(int i=0;i<schema.size();i++){
            schemaWidthMap.put(schema.get(i).getAttribute(),schema.get(i).getSchemaWidth());
        }
    }

    public static int getWidth(String attributeName){
        return schemaWidthMap.get(attributeName);
    }

    public static void print(){
        for(int i=0;i<schema.size();i++){
            System.out.println(schema.get(i).getAttribute());
        }
    }
}
