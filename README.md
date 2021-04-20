# 5300-Echidna 

5300-Echidna's DB Relation Manager project: Sprint Verano for CPSC5300

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop.
- <code>Milestone2</code> has the requirement files for Milestone2.

*Sprint Verano:*

To run milestone1 on cs1:

$ clone git repositiory: git clone https://github.com/klundeen/5300-Echidna

$ cd 5300-Echidna

$ make

$ ./sql5300 ~/cpsc5300/data

$ SQL> *enter SQL command here to see parser output*

To run milestone2 on cs1:

$ make

$ ./sql5300 ~/cpsc5300/data

$ SQL> test

Note: make sure to delete the contents of ~/cpsc5300/data between each run or File Exists DbException will occur.
Use the following commands to empty data folder
$ rm -f ~/cpsc5300/data/*

Milestone2 status:
All functionality is currently working for Milestone2 except for the select() method. Select() method causes seg fault.

