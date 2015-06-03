select x.docid,y.docid,sum(x.count*y.count)as count from Frequency x, Frequency y where x.term = y.term and x.docid < y.docid group by x.docid, y.docid  ORDER BY  count  ASC;
