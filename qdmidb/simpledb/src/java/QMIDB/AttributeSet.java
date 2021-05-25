package QMIDB;
import java.util.List;
/*
    *it is used to store all attributed in the predicate set
 */
public class AttributeSet {
    private static List<Attribute> attributes;

    public static List<Attribute> getAttributes() {
        return attributes;
    }

    public static void AttributeSet(List<Attribute> Attributes) {
        attributes = Attributes;
    }
}
