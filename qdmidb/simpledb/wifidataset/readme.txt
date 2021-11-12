Structure of schema should be:
M
N
K1
T1,A1
Cardinality1, numOfNullValues1
T2,A2
...
A_K1
K2
...
KN
...
----
M: id of query
N: number of relations
Ki: number of attributes in i-relation
Ti: type of attribute: INT/DOUBLE/STRING
Ai: attribute name
Format: relation_name.attribute_name

Structure of predicate:
M
N
Type_i
pred_i
...
----
M: id of query 
N: number of predicate
Type: F/J/A/O: filter, join, aggregate, order
F -- pred: attribute op operand
J -- pred: attributeLeft op attributeRight
A -- pred: attribute Type -- MAX/MIN/SUM/COUNT/AVG
O -- pred: attribute D/A (desc or asc)

