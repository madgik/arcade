A light-weight terminal that get's a subset of SQL and translates it to post request for the mserver.

Dependency: `moz_sql_parser`, `pip3 install moz_sql_parser`
https://github.com/mozilla/moz-sql-parser


Execution: `python3 mfederate.py http://host:port`
host:port is where mserver is running.
<br>example query:<br>
`select pearson(c1,c2) from data where (c1>1 and c3>4) or c2>1;`
<br>

Table `data` and attributes `c1,c2,c3` should exist in all the local nodes
The filters should follow disjunctive normal form like in the example above
