mysql> SHOW PROFILE FOR QUERY 1;
+--------------------------------+----------+
| Status                         | Duration |
+--------------------------------+----------+
| starting                       | 0.000018 |
| Waiting for query cache lock   | 0.000004 |
| checking query cache for query | 0.000029 |
| checking permissions           | 0.000004 |
| Opening tables                 | 0.000007 |
| init                           | 0.000010 |
| optimizing                     | 0.000004 |
| executing                      | 0.000011 |
| end                            | 0.000003 |
| query end                      | 0.000003 |
| closing tables                 | 0.000003 |
| freeing items                  | 0.000014 |
| logging slow query             | 0.000002 |
| cleaning up                    | 0.000002 |
+--------------------------------+----------+
14 rows in set (0.00 sec)

