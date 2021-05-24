package QMIDB;
import java.util.List;
/*
    *it is used to store all attributed in the predicate set
 */
public class AttributeSet {
    private List<Attribute> attributes;

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public AttributeSet(List<Attribute> attributes) {
        this.attributes = attributes;
    }
}
