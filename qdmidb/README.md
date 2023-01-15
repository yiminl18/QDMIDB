# QDMIDB

QDMIDB implements a query-driven approach to impute missing values in relational database. 

- Data sets and Query sets
  - See qdmidb/queryplancodes/, this folder contains two real datasets and one synthetic datasets used in the experiments, together with the query and the metadata of tables

- Core ZIP engine
  - See /qdmidb/simpledb/src/java/QDMIDB/, this folder contains the core codes of ZIP engines including the modified relational operator, routing logic, query plans, imputation operators. ZIP is built on top of the SimpleDB, whose core codes are available in /qdmidb/simpledb/src/java/simpledb
  - Tests codes for ZIP are included in /qdmidb/simpledb/src/java in Experiment as well as QMIDB. A more user-friendly version that packs the core engines as a server to run without touching codebase will be ready soon. 
