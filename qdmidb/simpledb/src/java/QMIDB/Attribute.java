package QMIDB;

/*
    * this class implements Attribute for relations
 */
public class Attribute {
    private String relation;
    private String attribute;

    public Attribute(String relation, String attribute) {
        this.relation = relation;
        this.attribute = attribute;
    }

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public int getHashCode(){
        return this.relation.hashCode() + this.attribute.hashCode();
    }
}
