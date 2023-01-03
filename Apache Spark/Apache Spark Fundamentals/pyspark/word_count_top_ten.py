"""here we are calculating top 10 occured words"""

from pyspark import SparkContext

sc = SparkContext("local[*]", "WordCount")
content = sc.textFile("E:\\dataset\\file1.txt")
contentFlat = content.flatMap(lambda x: x.split(" "))
contentMap = contentFlat.map(lambda x: (x.lower(),1))
contentReduce = contentMap.reduceByKey(lambda x,y: x+y)
contentSort = contentReduce.sortBy(lambda x: x[1], ascending=False)

result = contentSort.collect()

count = 0
for record in result:
    if count < 10:
        print(record)
        count += 1
    else:
        break
