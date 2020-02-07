#Introduction

This small project is written for ammonite. The extension `.sc` is used to get code completion working smoothly in intellij. The point of the project is to be able to take csv data and easily insert it into a database. Sometimes databases support operation to import csv data, but this data should then reside on the same server as the database, which isn't very convenient if you for example are running your database in docker.

# Usage

You use the tool by streaming your csv file into it. It will read the first line of the file as the columns of your table, then it will generate one insert for every consecutive row. 
You could for example for spin up a mysql locally:
```
podman run -e MYSQL_ROOT_PASSWORD=pass --name economy -p3306:3306 -d
```
then connect to the db and create the db `economy` and the table `economy` and then run.
```
cat testdata/Economy.csv | ./csv2sql.sc --table economy --database economy |  mysql -h127.0.0.1 -uroot -ppass
```
You can also use the program from podman or docker (just replace podman with docker):

`cat testdata/Economy.csv | podman run --rm -i csv2sql --table economy --database economy | mysql -h127.0.0.1 -uroot -ppass` 

then you can open the database and query your data at will.

# Requirements

* Ammonite (https://github.com/lihaoyi/Ammonite)[Ammonite] with its requirements.
