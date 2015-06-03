#!/bin/bash 
time mysql -uroot -proot  test < 1.sql  >/dev/null
time mysql -uroot -proot  test  < 2.sql  >/dev/null
time mysql -uroot -proot  test < 3.sql  >/dev/null

mysql -uroot -proot  test <  create_index.sql 

time mysql -uroot -proot  test < 1.sql  >/dev/null
time mysql -uroot -proot  test  < 2.sql  >/dev/null
time mysql -uroot -proot  test < 3.sql  >/dev/null
