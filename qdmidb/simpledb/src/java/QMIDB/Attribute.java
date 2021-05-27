package QMIDB;

/*
    * this class implements Attribute for relations
 */
public class Attribute {
    //format should be "tableName.attributeName"
    private String attribute;

    public Attribute(String attribute) {
        this.attribute = attribute;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public int getHashCode(){
        return this.attribute.hashCode();
    }

    public String getRelation(){
        return attribute.split("\\.")[0];
    }
}
