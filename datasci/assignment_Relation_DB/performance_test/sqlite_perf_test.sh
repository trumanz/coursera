#!/bin/bash 
cp  reuters.db tmp.db
time sqlite3    tmp.db  < 1.sql  >/dev/null
time sqlite3    tmp.db  < 2.sql  >/dev/null
time sqlite3    tmp.db  < 3.sql  >/dev/null

rm -rf tmp.db
cp  reuters.db tmp.db
sqlite3  tmp.db <  create_index.sql 

time sqlite3   tmp.db < 1.sql  >/dev/null
time sqlite3   tmp.db  < 2.sql  >/dev/null
time sqlite3   tmp.db  < 3.sql  >/dev/null
