"""to make it case insensitive we are converting out data in lower case"""

from pyspark import SparkContext

sc = SparkContext("local[*]", "WordCount")
content = sc.textFile("E:\\dataset\\file1.txt")
contentFlat = content.flatMap(lambda x: x.split(" "))
contentMap = contentFlat.map(lambda x: (x.lower(),1))
contentReduce = contentMap.reduceByKey(lambda x,y: x+y)

result = contentReduce.collect()

for record in result:
    print(record)
