Structure of schema should be:
M
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
M: number of relations
Ki: number of attributes in i-relation
Ai: attribute name
Format: relation_name.attribute_name

Structure of predicate:
MM
M
K
A1,A2,..,Ak
N
Type_i
pred_i
...
----
MM: number of queries 
M: id of query 
K: number of attributes in predicates (including project)
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
where N is number of columns in R.txt

In Quip and ImputeDB, simpledb.jar, missing values should be MISSING.INTEGER instead of a white space. 

Note:
1. Predicate.txt will not include aggregate predicate except MIN/MAX, which will be added in query plan directly. 
