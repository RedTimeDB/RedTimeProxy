#!/bin/bash

echo "query test:"

curl -X POST 'http://127.0.0.1:7076/query' --data-urlencode 'q=CREATE DATABASE db1'
curl -X POST 'http://127.0.0.1:7076/query' --data-urlencode 'q=CREATE DATABASE db2'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show databases'

curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=select * from cpu1;'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=select * from cpu2'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=select * from cpu3;'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=select * from cpu4'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=select * from "measurement with spaces, commas and \"quotes\""'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=select * from "\"measurement with spaces, commas and \"quotes\"\""'

curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show tag keys from cpu1'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show FIELD keys on db1 from cpu2'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=show TAG keys from cpu3'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show field KEYS on db2 from cpu4'

curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show MEASUREMENTS'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show series'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show series from cpu1'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show field KEYS'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show field KEYS from cpu1'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show TAG keys'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show TAG keys from cpu2'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show tag VALUES WITH key = "region"'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show tag VALUES from cpu2 WITH key = "region"'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=SHOW retention policies'
# curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show stats;'

curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show MEASUREMENTS on db2'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show series on db2'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show series on db2 from cpu4'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show field KEYS on db2'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show field KEYS on db2 from cpu4'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show TAG keys on db2'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show TAG keys on db2 from cpu3'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show tag VALUES on db2 WITH key = "region"'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show tag VALUES on db2 from cpu3 WITH key = "region"'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=SHOW retention policies on db2'
# curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=show stats'


echo ""
echo "error test:"

curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q='
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=select * from'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=select * measurement'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show TAG from cpu1'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show TAG values from '
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show field KEYS fr'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=show series from'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=show measurement'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=show stat'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=drop'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=delete from '
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=drop series'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=drop series from'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=drop measurement'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=CREATE DATABASE'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=drop database '
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=SHOW retention policies on newdb'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show TAG keys test from mem'


echo ""
echo "gzip test:"

queries=(
    'q=select * from "cpu1"'
    'q=show series from cpu2'
    'q=show TAG keys from cpu1'
    'q=show field KEYS from cpu2'
    'q=show MEASUREMENTS'
    'q=show series'
    'q=show field KEYS'
    'q=show TAG keys'
    'q=show tag VALUES WITH key = "region"'
    'q=SHOW retention policies'
    # 'q=show stats'
    # 'q='
    # 'q=select * from'
    # 'q=select * measurement'
    # 'q=show TAG from cpu1'
    # 'q=show TAG values from '
    # 'q=show field KEYS fr'
    # 'q=show series from'
    # 'q=show measurement'
    # 'q=show stat'
    # 'q=drop'
    # 'q=delete from '
    # 'q=drop series'
    # 'q=drop series from'
    # 'q=drop measurement'
    # 'q=CREATE DATABASE'
    # 'q=drop database '
    # 'q=SHOW retention policies on '
    # 'q=show TAG keys test from mem'
)

len=${#queries[*]}
i=0
while (($i<$len)); do
    query=${queries[$i]}
    curl -G -s 'http://127.0.0.1:7076/query?db=db1&epoch=s' -H "Accept-Encoding: gzip" --data-urlencode "$query" | gzip -d
    i=$(($i+1))
done


echo ""
echo "drop test:"

curl -X POST 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=delete from cpu1'
curl -X POST 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=drop series from cpu2'
curl -X POST 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=drop measurement cpu3'
curl -X POST 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=drop series from cpu4'
curl -X POST 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=delete from "measurement with spaces, commas and \"quotes\""'
curl -X POST 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=drop measurement "\"measurement with spaces, commas and \"quotes\"\""'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=select * from cpu1;'
curl -G 'http://127.0.0.1:7076/query?db=db1' --data-urlencode 'q=select * from cpu2'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=select * from cpu3;'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=select * from cpu4'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=select * from "measurement with spaces, commas and \"quotes\""'
curl -G 'http://127.0.0.1:7076/query?db=db2' --data-urlencode 'q=select * from "\"measurement with spaces, commas and \"quotes\"\""'
curl -X POST 'http://127.0.0.1:7076/query' --data-urlencode 'q=drop database db1'
curl -X POST 'http://127.0.0.1:7076/query' --data-urlencode 'q=drop database db2'
curl -G 'http://127.0.0.1:7076/query' --data-urlencode 'q=show databases'
