### How to build
```
mvn clean package 

```


### 1. Create test data
```
for i in {00001..99999}
do
    echo key$i,fm1:col1,value$i  >> data.txt
done
```

```
key1,fm1:col1,value1
key2,fm1:col1,value2
key3,fm1:col1,value3
key4,fm1:col1,value4
key5,fm1:col1,value5
key6,fm1:col1,value6
key7,fm1:col1,value7
...
```

### 2. Run Bulkload 
```
hadoop fs -put data.txt /tmp

//Need Running on hdfs cluster 
hadoop jar iwantfind-1.0-SNAPSHOT.jar 

```


