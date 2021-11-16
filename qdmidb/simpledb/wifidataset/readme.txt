Structure of schema should be:
MM
M
N
K1
A1
Cardinality1, numOfNullValues1
A2
...
A_K1
K2
...
KN
...
----
MM: number of query
M: id of query
N: number of relations
Ki: number of attributes in i-relation
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

Structure of imputedValues
tid
number of imputed field
fieldIndex,imputed values

*Use following command to transform txt file to dat file, which is consumed by codes
java -jar simpledb.jar convert R.txt N
where
