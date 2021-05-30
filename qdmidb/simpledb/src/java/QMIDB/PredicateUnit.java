package QMIDB;

import simpledb.Field;
import simpledb.JoinPredicate;
import simpledb.Predicate;

/*
    * a single predicate used in PredicateSet class
    * is used to construct relationship graph
 */
public class PredicateUnit {
    private Attribute left;
    private Predicate.Op op;
    private Attribute right;
    private Field operand;
    private boolean isJoin;

    public PredicateUnit(Attribute left, Predicate.Op op, Attribute right, Field operand, boolean isJoin) {
        this.left = left;
        this.op = op;
        this.right = right;
        this.operand = operand;
        this.isJoin = isJoin;
    }

    public simpledb.JoinPredicate transform(){//transform to predicate that is accepted by simpledb
        return new JoinPredicate(left.getAttribute(), op, right.getAttribute());
    }

    public Attribute getLeft() {
        return left;
    }

    public void setLeft(Attribute left) {
        this.left = left;
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

    public void setRight(Attribute right) {
        this.right = right;
    }

    public Field getOperand() {
        return operand;
    }

    public void setOperand(Field operand) {
        this.operand = operand;
    }

    public boolean getIsJoin() {
        return isJoin;
    }

    public void setJoin(boolean join) {
        isJoin = join;
    }
}
