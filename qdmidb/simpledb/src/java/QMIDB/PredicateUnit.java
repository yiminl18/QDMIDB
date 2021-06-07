package QMIDB;

import simpledb.*;

/*
    * a single predicate used in PredicateSet class
    * is used to construct relationship graph
 */
public class PredicateUnit {
    private Attribute left;
    private Predicate.Op op;
    private Attribute right;
    private Field operand;
    private Attribute aggregateAttribute;
    private Aggregator.Op Aop;
    private Attribute orderAttribute;
    private String orderType; //desc, asc
    private Attribute filterAttribute;
    private String type;//Filter, Join, Aggregate, Order

    public PredicateUnit(Attribute left, Predicate.Op op, Attribute right) {//read join
        this.left = left;
        this.op = op;
        this.right = right;
        this.type = "Join";
    }

    public PredicateUnit(Attribute filterAttribute, Predicate.Op op, Field operand){//read filter
        this.filterAttribute = filterAttribute;
        this.op = op;
        this.operand = operand;
        this.type = "Filter";
    }

    public PredicateUnit(Attribute aggregateAttribute, Aggregator.Op aop){//read aggregate
        this.aggregateAttribute = aggregateAttribute;
        this.Aop = aop;
        this.type = "Aggregate";
    }

    public PredicateUnit(Attribute orderAttribute, String orderType){//read order
        this.orderAttribute = orderAttribute;
        this.orderType = orderType;
        this.type = "Order";
    }

    public Field getOperand(){
        return operand;
    }

    public simpledb.JoinPredicate transform(){//transform to predicate that is accepted by simpledb
        return new JoinPredicate(left.getAttribute(), op, right.getAttribute());
    }

    public Attribute getLeft() {
        return left;
    }

    public Predicate.Op getOp() {
        return op;
    }

    public void setOp(Predicate.Op op) {
        this.op = op;
    }

    public Attribute getRight() {
        return right;
    }

    public String getType() {return type;}

    public Attribute getFilterAttribute() {return filterAttribute;}

    public JoinPredicate toJoinPredicate(){
        return new JoinPredicate(left.getAttribute(), op, right.getAttribute());
    }

    public Predicate toPredicate(){
        return new Predicate(filterAttribute.getAttribute(), op, operand);
    }

    public boolean getIsJoin(){
        if(type.equals("Join")){
            return true;
        }else{
            return false;
        }
    }

    public void print(){
        switch (type){
            case "Join":
                System.out.println(left.getAttribute() + " " + op + " " + right.getAttribute());
                break;
            case "Filter":
                System.out.println(filterAttribute.getAttribute() + " " + op + " " + operand);
                break;
            default:
                System.out.println("Predicate other than join and filter");
        }
    }

}
