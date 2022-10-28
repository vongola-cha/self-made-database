# self-made-database

a MySQL-like system written via Python

not case sensitive

contain: A parser and support multiple SQL queries

input string -> parser -> engine -> actions -> db -> tables -> columns
<img width="847" alt="image" src="https://user-images.githubusercontent.com/39432014/198690031-9d3faf3d-3d79-4376-af16-61245f2c7fd8.png">

a select query example:
SELECT a, test0.b FROM test0 INNER JOIN test2 ON test0.a=test2.a WHERE test0.a>5


### HOW TO RUN
python start.py

- Try to build your own databases, and then tables.
- Try our test database demo:
 
GroupTwo> use demo

demo> show tables

- use exit to exit the system

GroupTwo> exit

demo> exit
