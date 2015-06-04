
NFS 10MB/s

======schema
CREATE TABLE Frequency (
docid VARCHAR(255),
term VARCHAR(255),
count int,
PRIMARY KEY(docid, term));
sqlite> 
=====

count all
select count(*) from frequency;
161802    0m3.181s


a) σ10398_txt_earn(frequency)
  select count(*) from frequency where docid = "10398_txt_earn";
138   0m0.060s

b) πterm(σdocid=10398_txt_earn and count=1(frequency))
  SELECT f.term FROM  Frequency f WHERE f.docid="10398_txt_earn" and f.count=1;
(110)   0m0.098s 

c) πterm(σdocid=10398_txt_earn and count=1(frequency)) U πterm(σdocid=925_txt_trade and count=1(frequency))
SELECT count(*) from (SELECT f.term FROM Frequency f WHERE f.docid="10398_txt_earn" and f.count=1 UNION SELECT f.term FROM Frequency f WHERE f.docid="925_txt_trade" and f.count=1)X;

(225)  0m0.209s


d) count: Write a SQL statement to count the number of documents containing the word "parliament"
SELECT count(*) FROM Frequency f WHERE f.term='parliament';
Note: 1. count(f.count) ,  2,  like '%parliament %'  ???

e) big documents
  sqlite> select  * FROM (select f.docid,count(f.docid) AS count  from Frequency f GROUP BY f.docid)X  where X.count>300;
 or  select  count(*) FROM (select f.docid,count(f.docid) AS cc  from Frequency f GROUP BY f.docid  HAVING cc > 300)X;


f) two words
select count(*) from (select * from Frequency f where term="transactions")X JOIN (select * from Frequency where term="world")Y ON X.docid = Y.docid;
or  select * from  Frequency t, Frequency w where t.term = "transactions" and w.term = "world" and t.docid = w.docid;

g) multiply
SELECT A.row_num, B.col_num, SUM(A.value* B.value) from a as A, b as B  WHERE A.row_num=2 and B.col_num=3 and A.col_num = B.row_num GROUP BY A.row_num,B.col_num;

select val from  (SELECT A.row_num, B.col_num, SUM(A.value* B.value)as val from a as A, b as B  WHERE A.col_num = B.row_num GROUP BY A.row_num,B.col_num)x where row_num=2 and col_num=3;




h) similarity matrix
1. select x.docid,y.docid,sum(x.count*y.count)as count from Frequency x, Frequency y where x.term = y.term and x.docid < y.docid group by x.docid, y.docid  ORDER BY  count  ASC;

1m22.042s

select * from (select x.docid as a ,y.docid as b,sum(x.count*y.count)as count from Frequency x, Frequency y where x.term = y.term and x.docid < y.docid group by x.docid, y.docid  ORDER BY  count  ASC)x where x.a="10080_txt_crude" and x.b="17035_txt_earn";

1m10.919s

select * from (select x.docid as a ,y.docid as b,sum(x.count*y.count)as count from Frequency x, Frequency y where x.term = y.term and x.docid < y.docid  and x.docid="10080_txt_crude" and y.docid="17035_txt_earn" group by x.docid, y.docid  ORDER BY  count  ASC)x;

0m0.005s


i)keyword search

create view fk AS select * from frequency UNION SELECT 'q' as docid, 'washington' as term, 1 as count  UNION SELECT 'q' as docid, 'taxes' as term, 1 as count  UNION  SELECT 'q' as docid, 'treasury' as term, 1 as count;

 select * from (select x.docid as a ,y.docid as b,sum(x.count*y.count)as count from fk x, fk y where x.term = y.term and x.docid < y.docid and y.docid="q" group by x.docid, y.docid  ORDER BY  count  DESC )x LIMIT 1;





