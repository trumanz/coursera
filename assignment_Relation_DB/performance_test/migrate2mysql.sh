#!/bin/sh
sqlite3 reuters.db .dump  > db.sql
sed -i '/PRAGMA foreign_keys=OFF;/d'  ./db.sql
sed -i 's/BEGIN TRANSACTION;/BEGIN;/g'  ./db.sql
sed -i 's/INSERT INTO "Frequency"/INSERT INTO Frequency/g' ./db.sql

USER='root'
PASSWD='root'
DB='test'

echo "you need manualy change the 'BEGIN;' between CREATE TABLE and INSERT, and exec"
echo "mysql -u$USER -p$PASSWD $DB  <  db.sql"
